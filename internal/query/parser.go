package query

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// AggFunc enumerates aggregation functions.
type AggFunc int

const (
	AggNone AggFunc = iota
	AggCount
	AggSum
	AggMean
	AggMin
	AggMax
	AggStddev
	AggPercentile
	AggRate
	AggFirst
	AggLast
)

// TagOp enumerates tag operators.
type TagOp int

const (
	TagOpEq TagOp = iota
	TagOpNotEq
	TagOpIn
	TagOpNotIn
)

// TagFilter represents a tag predicate.
type TagFilter struct {
	Key    string
	Op     TagOp
	Values []string
}

// Aggregation defines an aggregation operation.
type Aggregation struct {
	Function AggFunc
	Window   time.Duration
}

// Query represents a parsed query.
type Query struct {
	Metric      string
	Tags        map[string]string
	TagFilters  []TagFilter
	Start       int64
	End         int64
	Aggregation *Aggregation
	GroupBy     []string
	Limit       int
}

// Parser parses a limited SQL-like query syntax.
type Parser struct{}

// Parse parses a query string.
// Example: SELECT mean(value) FROM temperature WHERE room = 'living' AND time > now() - 1h GROUP BY time(5m), room LIMIT 100
func (p *Parser) Parse(queryStr string) (*Query, error) {
	tokens := tokenize(queryStr)
	if len(tokens) == 0 {
		return nil, errors.New("empty query")
	}

	cursor := 0
	expect := func(value string) bool {
		if cursor >= len(tokens) {
			return false
		}
		if strings.EqualFold(tokens[cursor], value) {
			cursor++
			return true
		}
		return false
	}

	if !expect("SELECT") {
		return nil, errors.New("expected SELECT")
	}

	aggFunc := AggNone
	if cursor < len(tokens) && strings.Contains(tokens[cursor], "(") {
		aggToken := tokens[cursor]
		cursor++
		fn := strings.ToLower(strings.SplitN(aggToken, "(", 2)[0])
		aggFunc = ParseAggFunc(fn)
	}

	if !expect("FROM") {
		return nil, errors.New("expected FROM")
	}

	if cursor >= len(tokens) {
		return nil, errors.New("expected metric")
	}
	metric := tokens[cursor]
	cursor++

	q := &Query{
		Metric: metric,
		Tags:   make(map[string]string),
	}
	if aggFunc != AggNone {
		q.Aggregation = &Aggregation{Function: aggFunc, Window: time.Second}
	}

	if cursor < len(tokens) && strings.EqualFold(tokens[cursor], "WHERE") {
		cursor++
		for cursor < len(tokens) {
			if strings.EqualFold(tokens[cursor], "GROUP") || strings.EqualFold(tokens[cursor], "LIMIT") {
				break
			}
			if strings.EqualFold(tokens[cursor], "AND") {
				cursor++
				continue
			}

			if strings.EqualFold(tokens[cursor], "time") {
				cursor++
				if cursor >= len(tokens) {
					return nil, errors.New("expected time operator")
				}
				op := strings.ToUpper(tokens[cursor])
				cursor++
				if op == "BETWEEN" {
					if cursor >= len(tokens) {
						return nil, errors.New("expected time value")
					}
					startTokens := []string{}
					for cursor < len(tokens) {
						if strings.EqualFold(tokens[cursor], "AND") {
							cursor++
							break
						}
						startTokens = append(startTokens, tokens[cursor])
						cursor++
					}
					endTokens := []string{}
					for cursor < len(tokens) {
						if strings.EqualFold(tokens[cursor], "GROUP") || strings.EqualFold(tokens[cursor], "LIMIT") {
							break
						}
						if strings.EqualFold(tokens[cursor], "AND") {
							break
						}
						endTokens = append(endTokens, tokens[cursor])
						cursor++
					}
					startTs, err := ParseTimeExpression(">=", strings.Join(startTokens, " "))
					if err != nil {
						return nil, err
					}
					endTs, err := ParseTimeExpression("<=", strings.Join(endTokens, " "))
					if err != nil {
						return nil, err
					}
					q.Start = startTs
					q.End = endTs
					continue
				}

				if cursor >= len(tokens) {
					return nil, errors.New("expected time value")
				}

				valTokens := []string{}
				for cursor < len(tokens) {
					if strings.EqualFold(tokens[cursor], "AND") || strings.EqualFold(tokens[cursor], "GROUP") || strings.EqualFold(tokens[cursor], "LIMIT") {
						break
					}
					valTokens = append(valTokens, tokens[cursor])
					cursor++
				}
				val := strings.Join(valTokens, " ")

				ts, err := ParseTimeExpression(op, val)
				if err != nil {
					return nil, err
				}
				if op == ">" || op == ">=" {
					q.Start = ts
				} else if op == "<" || op == "<=" {
					q.End = ts
				}
				continue
			}

			if cursor+1 >= len(tokens) {
				return nil, errors.New("invalid tag filter")
			}
			key := tokens[cursor]
			cursor++
			rawOp := tokens[cursor]
			op := strings.ToUpper(rawOp)
			cursor++

			switch op {
			case "!":
				if cursor < len(tokens) && tokens[cursor] == "=" {
					cursor++
				}
				op = "!="
			case "NOT":
				if cursor < len(tokens) && strings.EqualFold(tokens[cursor], "IN") {
					cursor++
					op = "NOT IN"
				} else if cursor < len(tokens) && strings.HasPrefix(strings.ToUpper(tokens[cursor]), "IN(") {
					rawOp = tokens[cursor]
					cursor++
					op = "NOT IN("
				} else if cursor < len(tokens) && tokens[cursor] == "=" {
					cursor++
					op = "!="
				} else {
					op = "!="
				}
			case "<", ">":
				if cursor < len(tokens) && tokens[cursor] == "=" {
					op += "="
					cursor++
				}
			}

			if strings.HasPrefix(op, "NOT IN(") {
				values := []string{}
				first := strings.Trim(rawOp[3:], "'\"")
				if first != "" {
					values = append(values, first)
				}
				for cursor < len(tokens) {
					tok := tokens[cursor]
					cursor++
					if tok == ")" {
						break
					}
					if tok == "," {
						continue
					}
					values = append(values, strings.Trim(tok, "'\""))
				}
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpNotIn, Values: values})
				continue
			}

			if op == "NOT IN" {
				values := []string{}
				if cursor < len(tokens) && tokens[cursor] == "(" {
					cursor++
				}
				for cursor < len(tokens) {
					tok := tokens[cursor]
					cursor++
					if tok == ")" {
						break
					}
					if tok == "," {
						continue
					}
					values = append(values, strings.Trim(tok, "'\""))
				}
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpNotIn, Values: values})
				continue
			}

			if strings.HasPrefix(op, "IN(") {
				values := []string{}
				first := strings.Trim(rawOp[3:], "'\"")
				if first != "" {
					values = append(values, first)
				}
				for cursor < len(tokens) {
					tok := tokens[cursor]
					cursor++
					if tok == ")" {
						break
					}
					if tok == "," {
						continue
					}
					values = append(values, strings.Trim(tok, "'\""))
				}
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpIn, Values: values})
				continue
			}

			if op == "IN" {
				values := []string{}
				if cursor < len(tokens) && tokens[cursor] == "(" {
					cursor++
				}
				for cursor < len(tokens) {
					tok := tokens[cursor]
					cursor++
					if tok == ")" {
						break
					}
					if tok == "," {
						continue
					}
					values = append(values, strings.Trim(tok, "'\""))
				}
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpIn, Values: values})
				continue
			}

			if cursor >= len(tokens) {
				return nil, errors.New("invalid tag filter")
			}
			val := strings.Trim(tokens[cursor], "'\"")
			cursor++

			switch op {
			case "=":
				q.Tags[key] = val
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpEq, Values: []string{val}})
			case "!=", "<>":
				q.TagFilters = append(q.TagFilters, TagFilter{Key: key, Op: TagOpNotEq, Values: []string{val}})
			default:
				return nil, errors.New("unsupported tag operator")
			}
		}
	}

	if cursor < len(tokens) && strings.EqualFold(tokens[cursor], "GROUP") {
		cursor++
		if !expect("BY") {
			return nil, errors.New("expected BY")
		}
		var groupBy []string
		for cursor < len(tokens) {
			token := tokens[cursor]
			cursor++
			if strings.EqualFold(token, "LIMIT") {
				cursor--
				break
			}
			if strings.HasPrefix(strings.ToLower(token), "time(") {
				dur := strings.TrimSuffix(strings.TrimPrefix(token, "time("), ")")
				parsed, err := time.ParseDuration(dur)
				if err != nil {
					return nil, err
				}
				if q.Aggregation == nil {
					q.Aggregation = &Aggregation{Function: AggMean, Window: parsed}
				}
				q.Aggregation.Window = parsed
				continue
			}
			if token == "," {
				continue
			}
			groupBy = append(groupBy, token)
		}
		q.GroupBy = groupBy
	}

	if cursor < len(tokens) && strings.EqualFold(tokens[cursor], "LIMIT") {
		cursor++
		if cursor >= len(tokens) {
			return nil, errors.New("expected limit value")
		}
		limit, err := strconv.Atoi(tokens[cursor])
		if err != nil {
			return nil, err
		}
		q.Limit = limit
	}

	return q, nil
}

// ParseAggFunc parses an aggregation function name.
func ParseAggFunc(token string) AggFunc {
	switch strings.ToLower(token) {
	case "count":
		return AggCount
	case "sum":
		return AggSum
	case "mean":
		return AggMean
	case "min":
		return AggMin
	case "max":
		return AggMax
	case "stddev":
		return AggStddev
	case "percentile":
		return AggPercentile
	case "rate":
		return AggRate
	case "first":
		return AggFirst
	case "last":
		return AggLast
	default:
		return AggNone
	}
}

// ParseTimeExpression parses a time expression like "now() - 1h".
func ParseTimeExpression(op, value string) (int64, error) {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, " ", "")
	if strings.HasPrefix(value, "now()") {
		if strings.Contains(value, "-") {
			parts := strings.SplitN(value, "-", 2)
			if len(parts) == 2 {
				dur, err := time.ParseDuration(strings.TrimSpace(parts[1]))
				if err != nil {
					return 0, err
				}
				return time.Now().Add(-dur).UnixNano(), nil
			}
		}
		if strings.Contains(value, "+") {
			parts := strings.SplitN(value, "+", 2)
			if len(parts) == 2 {
				dur, err := time.ParseDuration(strings.TrimSpace(parts[1]))
				if err != nil {
					return 0, err
				}
				return time.Now().Add(dur).UnixNano(), nil
			}
		}
		return time.Now().UnixNano(), nil
	}

	if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
		return ts, nil
	}

	if dur, err := time.ParseDuration(value); err == nil {
		if op == ">" || op == ">=" {
			return time.Now().Add(-dur).UnixNano(), nil
		}
		return time.Now().Add(dur).UnixNano(), nil
	}

	return 0, errors.New("invalid time expression")
}

func tokenize(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}

	for _, r := range input {
		switch {
		case r == '\'' || r == '"':
			inQuote = !inQuote
			current.WriteRune(r)
		case inQuote:
			current.WriteRune(r)
		case r == ' ' || r == '\n' || r == '\t':
			flush()
		case r == ',' || r == '(' || r == ')':
			flush()
			tokens = append(tokens, string(r))
		default:
			current.WriteRune(r)
		}
	}
	flush()

	// Merge function tokens like time(5m), but keep membership lists separate.
	// Merging IN('a') would hide its closing parenthesis from the list parser.
	var merged []string
	i := 0
	for i < len(tokens) {
		if i+2 < len(tokens) && !strings.EqualFold(tokens[i], "IN") && tokens[i+1] == "(" && tokens[i+2] != ")" {
			merged = append(merged, tokens[i]+tokens[i+1]+tokens[i+2])
			i += 3
			if i < len(tokens) && tokens[i] == ")" {
				merged[len(merged)-1] += ")"
				i++
			}
			continue
		}
		merged = append(merged, tokens[i])
		i++
	}

	return merged
}
