package chronicle

import "testing"

func TestK8sSidecarPrometheusSampleTimestamp(t *testing.T) {
	cases := []struct {
		name, sample string
		want         int64
	}{
		{"default", "7", 123},
		{"milliseconds", "7 1395066363000", 1395066363000000000},
		{"epoch", "7 0", 0},
		{"before_epoch", "7 -1", -1000000},
		{"largest_nanosecond_timestamp", "7 9223372036854", 9223372036854000000},
		{"smallest_nanosecond_timestamp", "7 -9223372036854", -9223372036854000000},
	}
	var sidecar K8sSidecar
	for _, prefix := range []string{"sample ", `sample{route="/{id}",note="two words"} `} {
		for _, tc := range cases {
			t.Run(prefix+tc.name, func(t *testing.T) {
				point, err := sidecar.parseMetricLine(prefix+tc.sample, 123)
				if err != nil || point.Metric != "sample" || point.Value != 7 || point.Timestamp != tc.want {
					t.Fatalf("got %#v, err=%v; want timestamp %d", point, err, tc.want)
				}
			})
		}
	}
}

func TestK8sSidecarPrometheusMalformedSample(t *testing.T) {
	var sidecar K8sSidecar
	for _, line := range []string{
		`sample{note="two words"}`,
		`sample{note="two words"} `,
		"sample ",
		"sample 7 invalid",
		`sample{note="two words"} 7 1.5`,
		"sample 7 9223372036855",
		`sample{route="/{id}"} 7 -9223372036855`,
		"sample 7 9223372036854775808",
		"sample 7 1000 extra",
	} {
		t.Run(line, func(t *testing.T) {
			if point, err := sidecar.parseMetricLine(line, 123); err == nil {
				t.Fatalf("accepted malformed sample %q as %#v", line, point)
			}
		})
	}
}

func TestK8sSidecarPrometheusSampleRecovery(t *testing.T) {
	var sidecar K8sSidecar
	data := "# comment\n" + `broken{note="two words"}` + "\n" +
		"bad_timestamp 7 invalid\n" +
		`sample{route="/{id}",source="exporter"} 7 1395066363000` + "\n"
	points := sidecar.parsePrometheusMetrics(data, map[string]string{"source": "target"})
	if len(points) != 1 {
		t.Fatalf("got %d points; want only the valid sample: %#v", len(points), points)
	}
	point := points[0]
	if point.Metric != "sample" || point.Value != 7 || point.Timestamp != 1395066363000000000 || point.Tags["route"] != "/{id}" || point.Tags["source"] != "target" {
		t.Fatalf("sample time, label repair, or target precedence changed: %#v", point)
	}
}
