package query

import (
	"reflect"
	"testing"
	"time"
)

func TestParserMembershipListBoundaries(t *testing.T) {
	for _, op := range []string{"IN", "NOT IN"} {
		for _, list := range []string{
			"", "'a' AND region = 'west' LIMIT 1", "('a'",
			"('a' AND region = 'west' LIMIT 1", "('a' 'b')",
			"(,'a')", "('a',)", "('a',,'b')",
		} {
			sql := "SELECT sum(value) FROM cpu WHERE host " + op + " " + list
			t.Run(sql, func(t *testing.T) {
				got, err := (&Parser{}).Parse(sql)
				if err == nil || got != nil {
					t.Fatalf("malformed membership list returned query=%#v error=%v", got, err)
				}
			})
		}
		for _, tc := range []struct {
			list   string
			values []string
		}{
			{"('a')", []string{"a"}},
			{"( 'a', 'b' )", []string{"a", "b"}},
			{"()", []string{}},
			{"('')", []string{""}},
			{"('a,b', 'LIMIT', ')')", []string{"a,b", "LIMIT", ")"}},
			{"(one, two)", []string{"one", "two"}},
		} {
			for _, space := range []string{"", " "} {
				sql := "SELECT sum(value) FROM cpu WHERE host " + op + space + tc.list + " AND region = 'west' GROUP BY time(5m), region LIMIT 7"
				t.Run(sql, func(t *testing.T) {
					got, err := (&Parser{}).Parse(sql)
					if err != nil {
						t.Fatal(err)
					}
					wantOp := TagOpIn
					if op == "NOT IN" {
						wantOp = TagOpNotIn
					}
					if len(got.TagFilters) != 2 || got.TagFilters[0].Op != wantOp || !reflect.DeepEqual(got.TagFilters[0].Values, tc.values) {
						t.Fatalf("membership changed: %#v", got.TagFilters)
					}
					if got.Limit != 7 || got.Tags["region"] != "west" || !reflect.DeepEqual(got.GroupBy, []string{"region"}) || got.Aggregation == nil || got.Aggregation.Window != 5*time.Minute {
						t.Fatalf("following clauses changed: %#v", got)
					}
				})
			}
		}
	}
}
