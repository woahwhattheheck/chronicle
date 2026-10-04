package oteldistro

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPushMetricsDuringStop(t *testing.T) {
	var panics int64
	panicMessages := make(chan any, 800)
	for round := 0; round < 100; round++ {
		distro := NewChronicleOTelDistro(nil, DefaultOTelDistroConfig())
		pipeline := &Pipeline{dataChan: make(chan *Metrics, 1), running: true}
		distro.pipelines["metrics"] = pipeline
		done := make(chan struct{})
		var workers sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() {
					if failure := recover(); failure != nil {
						panicMessages <- failure
						atomic.AddInt64(&panics, 1)
					}
				}()
				for {
					select {
					case <-done:
						return
					default:
						distro.PushMetrics(&Metrics{})
					}
				}
			}()
		}
		for atomic.LoadInt64(&distro.metrics.MetricsReceived) < 100 {
			runtime.Gosched()
		}
		distro.stopPipeline(pipeline)
		close(done)
		workers.Wait()

		received := atomic.LoadInt64(&distro.metrics.MetricsReceived)
		dropped := atomic.LoadInt64(&distro.metrics.MetricsDropped)
		distro.PushMetrics(&Metrics{})
		if distro.metrics.MetricsReceived != received || distro.metrics.MetricsDropped != dropped {
			t.Fatal("stopped pipeline must not admit or count another report")
		}
		// Shutdown closes the existing bounded queue; queued data is retained.
		queued := 0
		for range pipeline.dataChan {
			queued++
		}
		if queued != 1 {
			t.Fatalf("expected one queued report, got %d", queued)
		}
		distro.cancel()
	}
	if panics != 0 {
		t.Fatalf("concurrent PushMetrics/stopPipeline produced %d panics; first: %v", panics, <-panicMessages)
	}
}
