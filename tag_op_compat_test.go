package chronicle

import "testing"

func TestTagOpNumericCompatibilityAndParserMapping(t *testing.T) {
	cases := []struct {
		name string
		op   TagOp
		want int
	}{
		{"eq", TagOpEq, 0},
		{"not-eq", TagOpNotEq, 1},
		{"in", TagOpIn, 2},
		{"regex", TagOpRegex, 3},
		{"not-regex", TagOpNotRegex, 4},
		{"not-in", TagOpNotIn, 5},
	}
	for _, tc := range cases {
		if got := int(tc.op); got != tc.want {
			t.Fatalf("%s numeric value changed: got %d, want %d", tc.name, got, tc.want)
		}
	}

	q, err := (&QueryParser{}).Parse("SELECT sum(value) FROM cpu WHERE host NOT IN ('web-1', 'web-2')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for _, filter := range q.TagFilters {
		if filter.Key != "host" {
			continue
		}
		if filter.Op != TagOpNotIn {
			t.Fatalf("internal NOT IN mapped to public op %d, want TagOpNotIn (%d)", filter.Op, TagOpNotIn)
		}
		return
	}
	t.Fatal("parsed NOT IN host filter not found")
}
