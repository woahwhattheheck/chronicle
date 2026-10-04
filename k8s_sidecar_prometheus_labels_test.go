package chronicle

import (
	"reflect"
	"strings"
	"testing"
)

func TestK8sSidecarPrometheusQuotedLabels(t *testing.T) {
	cases := []struct {
		name, line string
		want       map[string]string
	}{
		{"plain", `requests_total{method="GET",status="200"} 7`, map[string]string{"method": "GET", "status": "200"}},
		{"quoted_brace", `requests_total{route="/v1/{id}"} 7`, map[string]string{"route": "/v1/{id}"}},
		{"escaped_quote", `requests_total{message="say \"hi\""} 7`, map[string]string{"message": `say "hi"`}},
		{"escaped_backslash", `requests_total{path="C:\\DIR\\FILE.TXT"} 7`, map[string]string{"path": `C:\DIR\FILE.TXT`}},
		{"escaped_newline", `requests_total{message="first\nsecond"} 7`, map[string]string{"message": "first\nsecond"}},
		{"literal_backslash_n", `requests_total{message="\\n"} 7`, map[string]string{"message": `\n`}},
		{"quote_then_brace", `requests_total{route="a\"}b",method="GET"} 7`, map[string]string{"route": `a"}b`, "method": "GET"}},
		{"delimiters_in_value", `requests_total{message="a, b=c {d}",status="200"} 7`, map[string]string{"message": "a, b=c {d}", "status": "200"}},
		{"trailing_backslash", `requests_total{path="C:\\"} 7`, map[string]string{"path": `C:\`}},
		{"unicode", `requests_total{route="/日本/{id}"} 7`, map[string]string{"route": "/日本/{id}"}},
		{"tab_with_labels", "requests_total{method=\"GET\"}\t7", map[string]string{"method": "GET"}},
		{"tab_without_labels", "requests_total\t7", map[string]string{}},
		{"empty_label", `requests_total{message=""} 7`, map[string]string{"message": ""}},
	}
	var sidecar K8sSidecar
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			points := sidecar.parsePrometheusMetrics(tc.line+"\n", nil)
			if len(points) != 1 {
				t.Fatalf("valid sample was dropped: got %d points for %q", len(points), tc.line)
			}
			p := points[0]
			if p.Metric != "requests_total" || p.Value != 7 || p.Timestamp <= 0 || !reflect.DeepEqual(p.Tags, tc.want) {
				t.Fatalf("wrong sample: metric=%q value=%v tags=%#v; want tags=%#v", p.Metric, p.Value, p.Tags, tc.want)
			}
		})
	}
}

func TestK8sSidecarPrometheusLabelRecovery(t *testing.T) {
	var sidecar K8sSidecar
	input := "# comment\n\n" + `broken{message="unterminated} 7` + "\n" +
		`requests_total{route="/v1/{id}",source="exporter"} 7` + "\n"
	points := sidecar.parsePrometheusMetrics(input, map[string]string{"source": "target"})
	if len(points) != 1 || points[0].Metric != "requests_total" || points[0].Tags["route"] != "/v1/{id}" || points[0].Tags["source"] != "target" {
		t.Fatalf("malformed-line recovery or existing target-label precedence changed: %#v", points)
	}
}

func BenchmarkK8sSidecarPrometheusPlainLabels(b *testing.B) {
	data := strings.Repeat("requests_total{method=\"GET\",status=\"200\"} 7\n", 100)
	var sidecar K8sSidecar
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		if len(sidecar.parsePrometheusMetrics(data, nil)) != 100 {
			b.Fatal("plain samples lost")
		}
	}
}
