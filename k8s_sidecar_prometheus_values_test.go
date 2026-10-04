package chronicle

import (
	"math"
	"testing"
)

func TestK8sSidecarPrometheusNumericValues(t *testing.T) {
	values := []struct {
		input string
		want  float64
	}{
		{"0", 0},
		{"-0", math.Copysign(0, -1)},
		{"7", 7},
		{"-0.25", -0.25},
		{"1.25e-3", 0.00125},
		{"6.02e23", 6.02e23},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
		{"NaN", math.NaN()},
	}
	for _, prefix := range []string{"sample ", `sample{route="/{id}"} `} {
		for _, tc := range values {
			t.Run(prefix+tc.input, func(t *testing.T) {
				var sidecar K8sSidecar
				point, err := sidecar.parseMetricLine(prefix+tc.input, 123)
				equal := point.Value == tc.want || (math.IsNaN(point.Value) && math.IsNaN(tc.want))
				if err != nil || !equal || point.Metric != "sample" || point.Timestamp != 123 {
					t.Fatalf("value %q: got %#v, err=%v; want %v", tc.input, point, err, tc.want)
				}
				if tc.input == "-0" && !math.Signbit(point.Value) {
					t.Fatal("negative zero lost its sign")
				}
			})
		}
	}
}

func TestK8sSidecarPrometheusRejectNumericSuffix(t *testing.T) {
	for _, prefix := range []string{"sample ", `sample{method="GET"} `} {
		for _, value := range []string{"7garbage", "1.2.3", "1e999", "invalid"} {
			t.Run(prefix+value, func(t *testing.T) {
				var sidecar K8sSidecar
				if point, err := sidecar.parseMetricLine(prefix+value, 123); err == nil {
					t.Fatalf("accepted invalid value %q as %v", value, point.Value)
				}
			})
		}
	}
}
