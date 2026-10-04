package chronicle

import (
	"regexp"
	"sort"
	"strings"
)

// seriesKeyFromMetricTags generates a unique key for a series from metric and tags.
// The key format is: metric|tag1=val1,tag2=val2 (tags sorted alphabetically).
func seriesKeyFromMetricTags(metric string, tags map[string]string) string {
	if len(tags) == 0 {
		return metric
	}

	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)

	return metric + "|" + strings.Join(parts, ",")
}

// matchSeriesTags checks if a series' tags match a filter.
// Returns true if all filter tags are present with matching values.
func matchSeriesTags(seriesTags, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}

// matchSeriesTagFilters checks if a series' tags match all tag filters.
func matchSeriesTagFilters(seriesTags map[string]string, filters []TagFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		value, ok := seriesTags[filter.Key]
		switch filter.Op {
		case TagOpEq:
			if !ok || len(filter.Values) == 0 || value != filter.Values[0] {
				return false
			}
		case TagOpNotEq:
			if ok && len(filter.Values) > 0 && value == filter.Values[0] {
				return false
			}
		case TagOpIn:
			if !ok {
				return false
			}
			matched := false
			for _, v := range filter.Values {
				if value == v {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case TagOpNotIn:
			// Missing tags are treated as not in the exclusion set (match).
			if ok && tagFilterExcludes(filter, value) {
				return false
			}
		case TagOpRegex:
			if len(filter.Values) == 0 {
				return false
			}
			re := filter.compiledRe
			if re == nil {
				// Fallback: compile if not precompiled (shouldn't happen in normal path)
				var err error
				re, err = regexp.Compile(filter.Values[0])
				if err != nil {
					return false
				}
			}
			if !re.MatchString(value) {
				return false
			}
		case TagOpNotRegex:
			if len(filter.Values) > 0 {
				re := filter.compiledRe
				if re == nil {
					var err error
					re, err = regexp.Compile(filter.Values[0])
					if err != nil {
						continue // can't match — skip
					}
				}
				if re.MatchString(value) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// tagFilterExcludes uses a query-prepared lookup when available. Direct
// matcher callers and small lists retain the same allocation-free scan.
func tagFilterExcludes(filter TagFilter, value string) bool {
	if len(filter.Values) == 0 {
		return false
	}
	if len(filter.Values) == 1 {
		return value == filter.Values[0]
	}
	if filter.excludedValues != nil {
		_, excluded := filter.excludedValues[value]
		return excluded
	}
	for _, excluded := range filter.Values {
		if value == excluded {
			return true
		}
	}
	return false
}

// copyTags creates a deep copy of a tag map.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

// formatTagsString formats tags as a comma-separated string.
func formatTagsString(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
