package chronicle

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func sidecarRegistryTargets(n int) []ScrapeTarget {
	targets := make([]ScrapeTarget, n)
	for i := range targets {
		targets[i] = ScrapeTarget{Address: fmt.Sprintf("pod-%05d", i), Port: 9090, Path: "/metrics", Scheme: "http"}
	}
	return targets
}

func TestK8sSidecarScrapeTargetURL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		target  ScrapeTarget
		wantURL string
	}{
		{"dns", ScrapeTarget{Scheme: "http", Address: "metrics", Port: 9090, Path: "/metrics"}, "http://metrics:9090/metrics"},
		{"ipv4", ScrapeTarget{Scheme: "http", Address: "10.0.0.8", Port: 9100, Path: "/stats"}, "http://10.0.0.8:9100/stats"},
		{"ipv6", ScrapeTarget{Scheme: "http", Address: "fd00::10", Port: 9090, Path: "/metrics"}, "http://[fd00::10]:9090/metrics"},
		{"bracketed_ipv6", ScrapeTarget{Scheme: "https", Address: "[2001:db8::5]", Port: 9443, Path: "/metrics"}, "https://[2001:db8::5]:9443/metrics"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sidecarScrapeTargetURL(tc.target); got != tc.wantURL {
				t.Fatalf("scrape URL = %q, want %q", got, tc.wantURL)
			}
		})
	}
}

func TestK8sSidecarTargetRegistryFirstWinsAndOrder(t *testing.T) {
	for _, size := range []int{1, 16, 32, 33, 64, 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s := &K8sSidecar{}
			want := sidecarRegistryTargets(size)
			for _, target := range want {
				s.AddTarget(target)
			}
			for _, target := range want {
				target.Path = "/replacement"
				target.HonorLabels = true
				s.AddTarget(target)
			}
			// Identity remains the exact address/port pair, not path or case-folded host.
			for _, target := range []ScrapeTarget{
				{Address: want[0].Address, Port: 9091},
				{Address: "POD-00000", Port: 9090},
			} {
				s.AddTarget(target)
				want = append(want, target)
			}
			if got := s.GetTargets(); !reflect.DeepEqual(got, want) {
				t.Fatalf("targets changed order or first registration: got %v want %v", got, want)
			}
			copy := s.GetTargets()
			copy[0].Address = "modified-copy"
			if s.GetTargets()[0].Address != want[0].Address {
				t.Fatal("GetTargets no longer returns a separate slice")
			}
		})
	}
}

func TestK8sSidecarTargetRegistrySeedRemoveReadd(t *testing.T) {
	s, err := NewK8sSidecar(nil, DefaultK8sSidecarConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.cancel()
	want := s.GetTargets()
	for _, target := range sidecarRegistryTargets(64) {
		s.AddTarget(target)
		want = append(want, target)
	}
	defaultTarget := want[0]
	defaultTarget.Path = "/do-not-replace"
	s.AddTarget(defaultTarget)
	s.RemoveTarget("missing", 9090)
	for _, target := range []ScrapeTarget{want[0], want[32], want[64]} {
		s.RemoveTarget(target.Address, target.Port)
		s.RemoveTarget(target.Address, target.Port)
		for i, item := range want {
			if item.Address == target.Address && item.Port == target.Port {
				want = append(want[:i], want[i+1:]...)
				break
			}
		}
		target.Path = "/re-added"
		s.AddTarget(target)
		want = append(want, target)
		if got := s.GetTargets(); !reflect.DeepEqual(got, want) {
			t.Fatalf("remove/re-add changed target state: got %v want %v", got, want)
		}
	}
	for _, target := range want {
		s.RemoveTarget(target.Address, target.Port)
	}
	if len(s.GetTargets()) != 0 {
		t.Fatal("removing all targets retained entries")
	}
	s.AddTarget(defaultTarget)
	if got := s.GetTargets(); !reflect.DeepEqual(got, []ScrapeTarget{defaultTarget}) {
		t.Fatalf("empty registry could not register again: %v", got)
	}
}

func TestK8sSidecarTargetRegistrySeededDuplicates(t *testing.T) {
	want := sidecarRegistryTargets(40)
	s := &K8sSidecar{targets: append(append([]ScrapeTarget{}, want...), want[0])}
	s.AddTarget(want[0])
	s.RemoveTarget(want[0].Address, want[0].Port)
	if got := s.GetTargets(); !reflect.DeepEqual(got, want[1:]) {
		t.Fatalf("removal must remove all seeded duplicates: %v", got)
	}
	s.AddTarget(want[0])
	if got := s.GetTargets(); !reflect.DeepEqual(got, append(want[1:], want[0])) {
		t.Fatalf("removed seeded identity was not reusable: %v", got)
	}
}

func TestK8sSidecarTargetRegistryConcurrent(t *testing.T) {
	s := &K8sSidecar{}
	targets := sidecarRegistryTargets(128)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range targets {
				target := targets[(i+worker)%len(targets)]
				s.AddTarget(target)
				_ = s.GetTargets()
			}
		}(worker)
	}
	wg.Wait()
	got := s.GetTargets()
	if len(got) != len(targets) {
		t.Fatalf("concurrent duplicate registration: got %d targets want %d", len(got), len(targets))
	}
	seen := make(map[string]bool, len(got))
	for _, target := range got {
		if seen[target.Address] {
			t.Fatalf("duplicate address: %s", target.Address)
		}
		seen[target.Address] = true
	}
}

func BenchmarkK8sSidecarTargetRegistry(b *testing.B) {
	for _, size := range []int{1, 16, 1024, 8192} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			targets := sidecarRegistryTargets(size)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				s := &K8sSidecar{}
				for pass := 0; pass < 2; pass++ {
					for _, target := range targets {
						s.AddTarget(target)
					}
				}
				if len(s.targets) != size {
					b.Fatal("unexpected target count")
				}
			}
		})
	}
}
