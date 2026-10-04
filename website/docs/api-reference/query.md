---
sidebar_position: 4
---

# Query API

Complete reference for Chronicle query types and execution.

## Query Struct

```go
type Query struct {
    Metric      string             // Required: metric name
    Tags        map[string]string  // Simple tag equality filters
    TagFilters  []TagFilter        // Advanced tag filters
    Start       int64              // Start time (unix nanos)
    End         int64              // End time (unix nanos)
    Aggregation *Aggregation       // Optional aggregation
    GroupBy     []string           // Group by tag keys
    Limit       int                // Max results (0 = unlimited)
}
```

## Creating Queries

### Basic Query

```go
query := &chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
}
```

### With Tag Filters

```go
// Simple equality
query := &chronicle.Query{
    Metric: "cpu_usage",
    Tags:   map[string]string{"host": "server-01"},
}

// Advanced filters
query := &chronicle.Query{
    Metric: "cpu_usage",
    TagFilters: []chronicle.TagFilter{
        {Key: "host", Op: chronicle.TagOpEq, Values: []string{"server-01"}},
        {Key: "env", Op: chronicle.TagOpNotEq, Values: []string{"dev"}},
    },
}
```

### With Aggregation

```go
query := &chronicle.Query{
    Metric: "cpu_usage",
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   5 * time.Minute,
    },
}
```

### With Grouping

```go
query := &chronicle.Query{
    Metric:  "cpu_usage",
    GroupBy: []string{"host", "region"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
}
```

## TagFilter

```go
type TagFilter struct {
    Key    string   // Tag key
    Op     TagOp    // Operator
    Values []string // Values to match
}
```

### Tag Operators

```go
const (
    TagOpEq       TagOp = iota // Equals (0)
    TagOpNotEq                 // Not equals (1)
    TagOpIn                    // In list (2)
    TagOpRegex                 // Regex match (3)
    TagOpNotRegex              // Negated regex match (4)
    TagOpNotIn                 // Not in list (5)
)
```

**Examples:**

```go
// host = "server-01"
chronicle.TagFilter{Key: "host", Op: chronicle.TagOpEq, Values: []string{"server-01"}}

// env != "dev"
chronicle.TagFilter{Key: "env", Op: chronicle.TagOpNotEq, Values: []string{"dev"}}

// region IN ("us-west", "us-east")
chronicle.TagFilter{Key: "region", Op: chronicle.TagOpIn, Values: []string{"us-west", "us-east"}}

// host NOT IN ("web-1", "web-2")
chronicle.TagFilter{Key: "host", Op: chronicle.TagOpNotIn, Values: []string{"web-1", "web-2"}}
```

`TagOpNotIn` is appended after the regex operators so their existing numeric values remain unchanged. Use the named constants rather than assuming the public operators share the internal SQL parser's enum values.

An exclusion rejects a series when its tag value equals any value in the list. A series without that tag still matches `TagOpNotIn`. All entries in `TagFilters` are combined with AND, so use a separate positive filter when the query must also restrict the allowed values.

## Aggregation

```go
type Aggregation struct {
    Function AggFunc       // Aggregation function
    Window   time.Duration // Time window
}
```

### Aggregation Functions

```go
const (
    AggNone       AggFunc = iota // No aggregation
    AggCount                     // Count of points
    AggSum                       // Sum of values
    AggMean                      // Average
    AggMin                       // Minimum
    AggMax                       // Maximum
    AggStddev                    // Standard deviation
    AggPercentile                // Percentile
    AggRate                      // Per-second rate
    AggFirst                     // First value
    AggLast                      // Last value
)
```

**Usage:**

```go
// Hourly averages
&chronicle.Aggregation{Function: chronicle.AggMean, Window: time.Hour}

// 5-minute rate (for counters)
&chronicle.Aggregation{Function: chronicle.AggRate, Window: 5 * time.Minute}

// Daily max
&chronicle.Aggregation{Function: chronicle.AggMax, Window: 24 * time.Hour}
```

## Result

```go
type Result struct {
    Points []Point
}
```

**Processing results:**

```go
result, err := db.Execute(query)
if err != nil {
    return err
}

for _, p := range result.Points {
    fmt.Printf("Metric: %s\n", p.Metric)
    fmt.Printf("Tags: %v\n", p.Tags)
    fmt.Printf("Value: %f\n", p.Value)
    fmt.Printf("Time: %v\n", time.Unix(0, p.Timestamp))
}
```

## QueryParser

Parse SQL-like queries:

```go
parser := &chronicle.QueryParser{}
query, err := parser.Parse(queryString)
if err != nil {
    return err
}
result, err := db.Execute(query)
```

### SQL Syntax

```sql
SELECT <function>(<field>) | *
FROM <metric>
[WHERE <conditions>]
[GROUP BY time(<interval>), <tags>]
[LIMIT <n>]
```

**Examples:**

```go
// Select all
parser.Parse("SELECT * FROM cpu_usage")

// With time range
parser.Parse("SELECT * FROM cpu_usage WHERE time > now() - 1h")

// With aggregation
parser.Parse("SELECT mean(value) FROM cpu_usage GROUP BY time(5m)")

// With filters and grouping
parser.Parse(`
    SELECT max(value) 
    FROM cpu_usage 
    WHERE host = 'server-01' AND env != 'dev'
    GROUP BY time(1h), region
    LIMIT 100
`)
```

### Supported Functions

| Function | SQL Syntax |
|----------|------------|
| Mean | `mean(value)` |
| Sum | `sum(value)` |
| Min | `min(value)` |
| Max | `max(value)` |
| Count | `count(value)` |
| Rate | `rate(value)` |
| Stddev | `stddev(value)` |
| First | `first(value)` |
| Last | `last(value)` |

### Time Expressions

| Expression | Meaning |
|------------|---------|
| `now()` | Current time |
| `now() - 1h` | 1 hour ago |
| `now() - 7d` | 7 days ago |
| `now() - 30m` | 30 minutes ago |

## PromQL Executor

For Prometheus-compatible queries:

```go
executor := chronicle.NewPromQLExecutor(db)
```

### Instant Query

```go
result, err := executor.Query(
    `http_requests_total{method="GET"}`,
    time.Now(),
)
```

### Range Query

```go
result, err := executor.QueryRange(
    `rate(http_requests_total[5m])`,
    time.Now().Add(-time.Hour), // Start
    time.Now(),                  // End
    time.Minute,                 // Step
)
```

### Supported PromQL

**Selectors:**
- `metric_name`
- `{label="value"}`
- `{label!="value"}`
- `{label=~"regex"}`
- `{label!~"regex"}`

**Functions:**
- `rate()`, `irate()`
- `sum()`, `avg()`, `min()`, `max()`, `count()`
- `increase()`
- `histogram_quantile()`

**Aggregation:**
- `by (label)`
- `without (label)`

## Query Examples

### Last Hour CPU Usage

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "cpu_usage",
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
})
```

### Hourly Average by Host

```go
result, _ := db.Execute(&chronicle.Query{
    Metric:  "cpu_usage",
    Start:   time.Now().Add(-24 * time.Hour).UnixNano(),
    End:     time.Now().UnixNano(),
    GroupBy: []string{"host"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMean,
        Window:   time.Hour,
    },
})
```

### Request Rate

```go
result, _ := db.Execute(&chronicle.Query{
    Metric: "http_requests_total",
    Tags:   map[string]string{"method": "GET"},
    Start:  time.Now().Add(-time.Hour).UnixNano(),
    End:    time.Now().UnixNano(),
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggRate,
        Window:   time.Minute,
    },
})
```

### Top 10 Slowest Endpoints

```go
result, _ := db.Execute(&chronicle.Query{
    Metric:  "request_latency",
    Start:   time.Now().Add(-time.Hour).UnixNano(),
    End:     time.Now().UnixNano(),
    GroupBy: []string{"endpoint"},
    Aggregation: &chronicle.Aggregation{
        Function: chronicle.AggMax,
        Window:   time.Hour,
    },
    Limit: 10,
})
```
