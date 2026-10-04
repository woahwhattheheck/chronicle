package chronicle

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

type targetResponseBlockingWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *targetResponseBlockingWriter) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return w.ResponseRecorder.Write(body)
}

func TestK8sSidecarTargetResponseLock(t *testing.T) {
	original := ScrapeTarget{Address: "original", Port: 9090, Path: "/metrics", Scheme: "http", Labels: map[string]string{"pod": "kept"}}
	replacement := ScrapeTarget{Address: "replacement", Port: 9091, Path: "/other", Scheme: "http"}
	sidecar := &K8sSidecar{targets: []ScrapeTarget{original}}
	writer := &targetResponseBlockingWriter{
		ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writer.release) }) }
	defer release()
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		sidecar.handleTargets(writer, httptest.NewRequest("GET", "/targets", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never entered response Write")
	}

	type update struct {
		operation string
		duration  time.Duration
	}
	updates := make(chan update, 2)
	go func() {
		start := time.Now()
		sidecar.AddTarget(replacement)
		updates <- update{"add", time.Since(start)}
	}()
	go func() {
		start := time.Now()
		sidecar.RemoveTarget(original.Address, original.Port)
		updates <- update{"remove", time.Since(start)}
	}()
	completed := make(map[string]time.Duration)
	heldProgress := make(map[string]bool)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(completed) < 2 {
		select {
		case result := <-updates:
			completed[result.operation] = result.duration
			heldProgress[result.operation] = true
		case <-deadline.C:
			goto observed
		}
	}
observed:
	release()
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish after releasing response Write")
	}
	for len(completed) < 2 {
		select {
		case result := <-updates:
			completed[result.operation] = result.duration
		case <-time.After(2 * time.Second):
			t.Fatal("target update did not finish after releasing response Write")
		}
	}
	t.Logf("response_write_held: add_completed=%t remove_completed=%t add_ms=%.6f remove_ms=%.6f",
		heldProgress["add"], heldProgress["remove"],
		float64(completed["add"])/float64(time.Millisecond), float64(completed["remove"])/float64(time.Millisecond))
	if !heldProgress["add"] || !heldProgress["remove"] {
		t.Error("target updates waited for /targets response Write")
	}
	expected, err := json.Marshal([]ScrapeTarget{original})
	if err != nil {
		t.Fatal(err)
	}
	if writer.Body.String() != string(expected)+"\n" || writer.Header().Get("Content-Type") != "application/json" {
		t.Errorf("response snapshot changed during target updates: %s", writer.Body.String())
	}
	if targets := sidecar.GetTargets(); !reflect.DeepEqual(targets, []ScrapeTarget{replacement}) {
		t.Errorf("target updates were lost: %+v", targets)
	}
	for _, targets := range [][]ScrapeTarget{nil, {}} {
		control := &K8sSidecar{targets: targets}
		response := httptest.NewRecorder()
		control.handleTargets(response, httptest.NewRequest("GET", "/targets", nil))
		expected, err := json.Marshal(targets)
		if err != nil {
			t.Fatal(err)
		}
		if response.Body.String() != string(expected)+"\n" {
			t.Errorf("nil/empty target encoding changed: got %q, want %q", response.Body.String(), string(expected)+"\n")
		}
	}
}
