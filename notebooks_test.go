package chronicle

import (
	"strings"
	"sync"
	"testing"
)

func TestNotebookEngineCreate(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNotebookEngine(db, DefaultNotebookConfig())

	nb, err := engine.CreateNotebook(Notebook{Title: "Test Notebook"})
	if err != nil {
		t.Fatalf("CreateNotebook failed: %v", err)
	}
	if nb.ID == "" {
		t.Error("expected non-empty ID")
	}
	if nb.Title != "Test Notebook" {
		t.Errorf("expected title 'Test Notebook', got %s", nb.Title)
	}
}

func TestNotebookEngineGetAndList(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())

	nb, _ := engine.CreateNotebook(Notebook{Title: "NB1"})
	engine.CreateNotebook(Notebook{Title: "NB2"})

	got, found := engine.GetNotebook(nb.ID)
	if !found {
		t.Fatal("expected to find notebook")
	}
	if got.Title != "NB1" {
		t.Errorf("expected NB1, got %s", got.Title)
	}

	list := engine.ListNotebooks()
	if len(list) != 2 {
		t.Errorf("expected 2 notebooks, got %d", len(list))
	}
}

func TestNotebookEngineDelete(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	nb, _ := engine.CreateNotebook(Notebook{Title: "Delete Me"})

	if err := engine.DeleteNotebook(nb.ID); err != nil {
		t.Fatalf("DeleteNotebook failed: %v", err)
	}
	_, found := engine.GetNotebook(nb.ID)
	if found {
		t.Error("expected notebook to be deleted")
	}
}

func TestNotebookEngineAddCell(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	nb, _ := engine.CreateNotebook(Notebook{Title: "With Cells"})

	err := engine.AddCell(nb.ID, NotebookCell{Type: CellQuery, Source: "temperature"})
	if err != nil {
		t.Fatalf("AddCell failed: %v", err)
	}

	got, _ := engine.GetNotebook(nb.ID)
	if len(got.Cells) != 1 {
		t.Errorf("expected 1 cell, got %d", len(got.Cells))
	}
}

func TestNotebookEngineExecuteCell(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNotebookEngine(db, DefaultNotebookConfig())
	nb, _ := engine.CreateNotebook(Notebook{Title: "Exec"})

	cellID := "test-cell"
	engine.AddCell(nb.ID, NotebookCell{ID: cellID, Type: CellQuery, Source: "cpu"})

	out, err := engine.ExecuteCell(nb.ID, cellID)
	if err != nil {
		t.Fatalf("ExecuteCell failed: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestNotebookEngineExecuteAll(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNotebookEngine(db, DefaultNotebookConfig())
	nb, _ := engine.CreateNotebook(Notebook{Title: "All"})
	engine.AddCell(nb.ID, NotebookCell{ID: "c1", Type: CellQuery, Source: "metric1"})
	engine.AddCell(nb.ID, NotebookCell{ID: "c2", Type: CellMarkdown, Source: "# Header"})
	engine.AddCell(nb.ID, NotebookCell{ID: "c3", Type: CellQuery, Source: "metric2"})

	outputs, err := engine.ExecuteAll(nb.ID)
	if err != nil {
		t.Fatalf("ExecuteAll failed: %v", err)
	}
	if len(outputs) != 2 { // only query cells
		t.Errorf("expected 2 outputs, got %d", len(outputs))
	}
}

func TestNotebookEngineExportHTML(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())
	nb, _ := engine.CreateNotebook(Notebook{Title: "Export Test"})
	engine.AddCell(nb.ID, NotebookCell{ID: "md1", Type: CellMarkdown, Source: "Hello world"})
	engine.AddCell(nb.ID, NotebookCell{ID: "q1", Type: CellQuery, Source: "SELECT * FROM cpu"})

	html, err := engine.ExportHTML(nb.ID)
	if err != nil {
		t.Fatalf("ExportHTML failed: %v", err)
	}
	if html == "" {
		t.Error("expected non-empty HTML")
	}
}

func TestNotebookEngineParseMarkdown(t *testing.T) {
	engine := NewNotebookEngine(nil, DefaultNotebookConfig())

	content := `# My Notebook

Some description text

` + "```sql" + `
SELECT * FROM temperature WHERE time > now() - 1h
` + "```" + `

More text here

` + "```chart" + `
{"type": "line"}
` + "```" + `
`

	nb, err := engine.ParseMarkdown(content)
	if err != nil {
		t.Fatalf("ParseMarkdown failed: %v", err)
	}
	if nb.Title != "My Notebook" {
		t.Errorf("expected title 'My Notebook', got %s", nb.Title)
	}
	if len(nb.Cells) == 0 {
		t.Error("expected cells")
	}

	if len(nb.Cells) != 4 {
		t.Fatalf("expected four cells, including prose after the query; got %d", len(nb.Cells))
	}
	if nb.Cells[2].Type != CellMarkdown || !strings.Contains(nb.Cells[2].Source, "More text here") {
		t.Fatalf("lost markdown after fenced query: %#v", nb.Cells[2])
	}

	// Should have markdown, query, markdown, and chart cells
	hasQuery := false
	hasChart := false
	for _, c := range nb.Cells {
		if c.Type == CellQuery {
			hasQuery = true
		}
		if c.Type == CellChart {
			hasChart = true
		}
	}
	if !hasQuery {
		t.Error("expected at least one query cell")
	}
	if !hasChart {
		t.Error("expected at least one chart cell")
	}
}

func TestNotebookEngineMaxNotebooks(t *testing.T) {
	config := DefaultNotebookConfig()
	config.MaxNotebooks = 2
	engine := NewNotebookEngine(nil, config)

	engine.CreateNotebook(Notebook{ID: "nb-1", Title: "1"})
	engine.CreateNotebook(Notebook{ID: "nb-2", Title: "2"})
	_, err := engine.CreateNotebook(Notebook{ID: "nb-3", Title: "3"})
	if err == nil {
		t.Error("expected max notebooks error")
	}
}

func TestNotebookEngineMaxCells(t *testing.T) {
	config := DefaultNotebookConfig()
	config.MaxCells = 2
	engine := NewNotebookEngine(nil, config)
	nb, _ := engine.CreateNotebook(Notebook{Title: "Full"})

	engine.AddCell(nb.ID, NotebookCell{ID: "c1", Type: CellMarkdown, Source: "a"})
	engine.AddCell(nb.ID, NotebookCell{ID: "c2", Type: CellMarkdown, Source: "b"})
	err := engine.AddCell(nb.ID, NotebookCell{ID: "c3", Type: CellMarkdown, Source: "c"})
	if err == nil {
		t.Error("expected max cells error")
	}
}

// Multiple engines and callers may allocate within one Windows clock tick.
func TestNotebookGeneratedIDs(t *testing.T) {
	const workers, each = 8, 16
	ids := make(chan string, workers*each*3)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine := NewNotebookEngine(nil, DefaultNotebookConfig())
			for n := 0; n < each; n++ {
				nb, err := engine.CreateNotebook(Notebook{Title: "retained"})
				if err != nil {
					t.Error(err)
					return
				}
				ids <- nb.ID
				for c := 0; c < 2; c++ {
					if err := engine.AddCell(nb.ID, NotebookCell{Type: CellMarkdown, Source: "text"}); err != nil {
						t.Error(err)
						return
					}
				}
				for _, cell := range nb.Cells {
					ids <- cell.ID
				}
			}
			if got := len(engine.ListNotebooks()); got != each {
				t.Errorf("lost notebooks: got %d, want %d", got, each)
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[string]bool)
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate generated ID: %s", id)
		}
		seen[id] = true
	}
	if got := len(seen); got != workers*each*3 {
		t.Errorf("distinct notebook/cell IDs: got %d, want %d", got, workers*each*3)
	}
}
