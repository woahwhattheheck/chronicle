package chronicle

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"sync"
	"time"
)

// NotebookCellType represents the type of a notebook cell.
type NotebookCellType string

const (
	CellMarkdown NotebookCellType = "markdown"
	CellQuery    NotebookCellType = "query"
	CellChart    NotebookCellType = "chart"
)

// NotebookConfig configures the notebook engine.
type NotebookConfig struct {
	Enabled          bool          `json:"enabled"`
	MaxCells         int           `json:"max_cells"`
	MaxOutputRows    int           `json:"max_output_rows"`
	ExecutionTimeout time.Duration `json:"execution_timeout"`
	AutoSaveInterval time.Duration `json:"auto_save_interval"`
	MaxNotebooks     int           `json:"max_notebooks"`
}

// DefaultNotebookConfig returns sensible defaults.
func DefaultNotebookConfig() NotebookConfig {
	return NotebookConfig{
		Enabled:          false,
		MaxCells:         200,
		MaxOutputRows:    10000,
		ExecutionTimeout: 30 * time.Second,
		AutoSaveInterval: 60 * time.Second,
		MaxNotebooks:     100,
	}
}

// NotebookCell represents a single cell in a notebook.
type NotebookCell struct {
	ID       string            `json:"id"`
	Type     NotebookCellType  `json:"type"`
	Source   string            `json:"source"`
	Output   *CellOutput       `json:"output,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// CellOutput holds the execution result of a cell.
type CellOutput struct {
	Data      any       `json:"data,omitempty"`
	Error     string    `json:"error,omitempty"`
	Duration  string    `json:"duration,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Notebook represents an interactive .chronicle notebook document.
type Notebook struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Cells       []NotebookCell    `json:"cells"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Author      string            `json:"author,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Parameters  map[string]string `json:"parameters,omitempty"`
}

// NotebookEngine manages notebook lifecycle and execution.
type NotebookEngine struct {
	config    NotebookConfig
	db        *DB
	notebooks map[string]*Notebook
	mu        sync.RWMutex
}

// NewNotebookEngine creates a new notebook engine.
func NewNotebookEngine(db *DB, config NotebookConfig) *NotebookEngine {
	return &NotebookEngine{
		config:    config,
		db:        db,
		notebooks: make(map[string]*Notebook),
	}
}

// CreateNotebook creates a new notebook.
func (ne *NotebookEngine) CreateNotebook(nb Notebook) (*Notebook, error) {
	ne.mu.Lock()
	defer ne.mu.Unlock()

	if len(ne.notebooks) >= ne.config.MaxNotebooks {
		return nil, fmt.Errorf("max notebooks reached (%d)", ne.config.MaxNotebooks)
	}
	if nb.ID == "" {
		// A coarse clock can return the same timestamp for consecutive creates.
		// Probe under the existing lock instead of overwriting a live notebook.
		for candidate := time.Now().UnixNano(); ; candidate++ {
			nb.ID = fmt.Sprintf("nb-%d", candidate)
			if _, exists := ne.notebooks[nb.ID]; !exists {
				break
			}
		}
	}
	if nb.Title == "" {
		nb.Title = "Untitled Notebook"
	}
	nb.CreatedAt = time.Now()
	nb.UpdatedAt = time.Now()
	if nb.Cells == nil {
		nb.Cells = []NotebookCell{}
	}

	ne.notebooks[nb.ID] = &nb
	return &nb, nil
}

// GetNotebook returns a notebook by ID.
func (ne *NotebookEngine) GetNotebook(id string) (*Notebook, bool) {
	ne.mu.RLock()
	defer ne.mu.RUnlock()
	nb, ok := ne.notebooks[id]
	return nb, ok
}

// ListNotebooks returns all notebooks.
func (ne *NotebookEngine) ListNotebooks() []*Notebook {
	ne.mu.RLock()
	defer ne.mu.RUnlock()

	result := make([]*Notebook, 0, len(ne.notebooks))
	for _, nb := range ne.notebooks {
		result = append(result, nb)
	}
	return result
}

// DeleteNotebook removes a notebook.
func (ne *NotebookEngine) DeleteNotebook(id string) error {
	ne.mu.Lock()
	defer ne.mu.Unlock()

	if _, exists := ne.notebooks[id]; !exists {
		return fmt.Errorf("notebook %q not found", id)
	}
	delete(ne.notebooks, id)
	return nil
}

// AddCell adds a cell to a notebook.
func (ne *NotebookEngine) AddCell(notebookID string, cell NotebookCell) error {
	ne.mu.Lock()
	defer ne.mu.Unlock()

	nb, exists := ne.notebooks[notebookID]
	if !exists {
		return fmt.Errorf("notebook %q not found", notebookID)
	}
	if len(nb.Cells) >= ne.config.MaxCells {
		return fmt.Errorf("max cells reached (%d)", ne.config.MaxCells)
	}
	if cell.ID == "" {
		// Imported cells may already occupy an automatically generated ID.
		existingIDs := make(map[string]struct{}, len(nb.Cells))
		for _, existing := range nb.Cells {
			existingIDs[existing.ID] = struct{}{}
		}
		for candidate := time.Now().UnixNano(); ; candidate++ {
			cell.ID = fmt.Sprintf("cell-%d", candidate)
			if _, exists := existingIDs[cell.ID]; !exists {
				break
			}
		}
	}
	nb.Cells = append(nb.Cells, cell)
	nb.UpdatedAt = time.Now()
	return nil
}

// ExecuteCell runs a query cell and stores the output.
func (ne *NotebookEngine) ExecuteCell(notebookID, cellID string) (*CellOutput, error) {
	ne.mu.Lock()
	nb, exists := ne.notebooks[notebookID]
	if !exists {
		ne.mu.Unlock()
		return nil, fmt.Errorf("notebook %q not found", notebookID)
	}

	var cell *NotebookCell
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			cell = &nb.Cells[i]
			break
		}
	}
	if cell == nil {
		ne.mu.Unlock()
		return nil, fmt.Errorf("cell %q not found", cellID)
	}
	source := cell.Source
	cellType := cell.Type
	ne.mu.Unlock()

	if cellType != CellQuery {
		return &CellOutput{Data: source, Timestamp: time.Now()}, nil
	}

	start := time.Now()
	output := &CellOutput{Timestamp: time.Now()}

	// Apply parameter substitution
	ne.mu.RLock()
	params := nb.Parameters
	ne.mu.RUnlock()

	query := source
	for k, v := range params {
		query = strings.ReplaceAll(query, "{{"+k+"}}", v)
	}

	// Execute via db
	if ne.db != nil {
		q := &Query{Metric: strings.TrimSpace(query)}
		result, err := ne.db.Execute(q)
		if err != nil {
			output.Error = err.Error()
		} else if result != nil {
			if len(result.Points) > ne.config.MaxOutputRows {
				result.Points = result.Points[:ne.config.MaxOutputRows]
			}
			output.Data = result
		}
	} else {
		output.Error = "no database connection"
	}

	output.Duration = time.Since(start).String()

	ne.mu.Lock()
	for i := range nb.Cells {
		if nb.Cells[i].ID == cellID {
			nb.Cells[i].Output = output
			break
		}
	}
	nb.UpdatedAt = time.Now()
	ne.mu.Unlock()

	return output, nil
}

// ExecuteAll runs all query cells in order.
func (ne *NotebookEngine) ExecuteAll(notebookID string) ([]*CellOutput, error) {
	ne.mu.RLock()
	nb, exists := ne.notebooks[notebookID]
	if !exists {
		ne.mu.RUnlock()
		return nil, fmt.Errorf("notebook %q not found", notebookID)
	}
	cellIDs := make([]string, 0)
	for _, c := range nb.Cells {
		if c.Type == CellQuery {
			cellIDs = append(cellIDs, c.ID)
		}
	}
	ne.mu.RUnlock()

	outputs := make([]*CellOutput, 0, len(cellIDs))
	for _, cellID := range cellIDs {
		out, err := ne.ExecuteCell(notebookID, cellID)
		if err != nil {
			return outputs, err
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

// ExportHTML exports a notebook as static HTML.
func (ne *NotebookEngine) ExportHTML(notebookID string) (string, error) {
	ne.mu.RLock()
	nb, exists := ne.notebooks[notebookID]
	if !exists {
		ne.mu.RUnlock()
		return "", fmt.Errorf("notebook %q not found", notebookID)
	}
	ne.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("<!DOCTYPE html><html><head><meta charset='UTF-8'>")
	sb.WriteString(fmt.Sprintf("<title>%s</title>", html.EscapeString(nb.Title)))
	sb.WriteString("<style>body{font-family:sans-serif;max-width:900px;margin:0 auto;padding:20px}")
	sb.WriteString(".cell{margin:10px 0;padding:10px;border:1px solid #ddd;border-radius:4px}")
	sb.WriteString(".query{background:#f0f8ff}.output{background:#f5f5f5;margin-top:5px;padding:8px}")
	sb.WriteString("pre{overflow-x:auto}</style></head><body>")
	sb.WriteString(fmt.Sprintf("<h1>%s</h1>", html.EscapeString(nb.Title)))
	if nb.Description != "" {
		sb.WriteString(fmt.Sprintf("<p>%s</p>", html.EscapeString(nb.Description)))
	}

	for _, cell := range nb.Cells {
		sb.WriteString("<div class='cell'>")
		switch cell.Type {
		case CellMarkdown:
			sb.WriteString(fmt.Sprintf("<div class='markdown'>%s</div>", html.EscapeString(cell.Source)))
		case CellQuery:
			sb.WriteString(fmt.Sprintf("<div class='query'><pre>%s</pre></div>", html.EscapeString(cell.Source)))
			if cell.Output != nil {
				sb.WriteString("<div class='output'>")
				if cell.Output.Error != "" {
					sb.WriteString(fmt.Sprintf("<pre style='color:red'>%s</pre>", html.EscapeString(cell.Output.Error)))
				} else {
					data, _ := json.MarshalIndent(cell.Output.Data, "", "  ")
					sb.WriteString(fmt.Sprintf("<pre>%s</pre>", html.EscapeString(string(data))))
				}
				if cell.Output.Duration != "" {
					sb.WriteString(fmt.Sprintf("<small>Duration: %s</small>", html.EscapeString(cell.Output.Duration)))
				}
				sb.WriteString("</div>")
			}
		case CellChart:
			sb.WriteString(fmt.Sprintf("<div class='chart'>[Chart: %s]</div>", html.EscapeString(cell.Source)))
		}
		sb.WriteString("</div>")
	}

	sb.WriteString("</body></html>")
	return sb.String(), nil
}

// ParseMarkdown parses a .chronicle markdown file into a Notebook.
func (ne *NotebookEngine) ParseMarkdown(content string) (*Notebook, error) {
	nb := &Notebook{
		Cells: make([]NotebookCell, 0),
	}

	lines := strings.Split(content, "\n")
	var currentCell *NotebookCell
	inCodeBlock := false
	var codeLang string
	var codeLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Title from first H1
		if strings.HasPrefix(trimmed, "# ") && nb.Title == "" {
			nb.Title = strings.TrimPrefix(trimmed, "# ")
			continue
		}

		if strings.HasPrefix(trimmed, "```") {
			if !inCodeBlock {
				// Start code block
				inCodeBlock = true
				codeLang = strings.TrimPrefix(trimmed, "```")
				codeLines = []string{}
				continue
			}
			// End code block
			inCodeBlock = false
			source := strings.Join(codeLines, "\n")
			cellType := CellQuery
			if codeLang == "chart" || codeLang == "vega" {
				cellType = CellChart
			}
			nb.Cells = append(nb.Cells, NotebookCell{
				ID:       fmt.Sprintf("cell-%d", len(nb.Cells)),
				Type:     cellType,
				Source:   source,
				Metadata: map[string]string{"language": codeLang},
			})
			continue
		}

		if inCodeBlock {
			codeLines = append(codeLines, line)
			continue
		}

		// Accumulate markdown text
		if currentCell == nil || currentCell.Type != CellMarkdown {
			if trimmed != "" {
				currentCell = &NotebookCell{
					ID:   fmt.Sprintf("cell-%d", len(nb.Cells)),
					Type: CellMarkdown,
				}
				nb.Cells = append(nb.Cells, *currentCell)
			}
		}
		if len(nb.Cells) > 0 && nb.Cells[len(nb.Cells)-1].Type == CellMarkdown {
			if nb.Cells[len(nb.Cells)-1].Source != "" {
				nb.Cells[len(nb.Cells)-1].Source += "\n"
			}
			nb.Cells[len(nb.Cells)-1].Source += line
		}
	}

	return nb, nil
}
