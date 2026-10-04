package chronicle

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultTSDiffMergeConfig(t *testing.T) {
	cfg := DefaultTSDiffMergeConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.MaxDiffSize != 100000 {
		t.Errorf("MaxDiffSize = %d, want 100000", cfg.MaxDiffSize)
	}
	if cfg.ConflictStrategy != "last-write-wins" {
		t.Errorf("ConflictStrategy = %s, want last-write-wins", cfg.ConflictStrategy)
	}
	if cfg.MergeTimeout != 30*time.Second {
		t.Errorf("MergeTimeout = %v, want 30s", cfg.MergeTimeout)
	}
	if cfg.BranchTTL != 7*24*time.Hour {
		t.Errorf("BranchTTL = %v, want 168h", cfg.BranchTTL)
	}
}

func newTestDiffMergeEngine(t *testing.T) *TSDiffMergeEngine {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewTSDiffMergeEngine(db, DefaultTSDiffMergeConfig())
}

func TestTSDiffMerge_CreateAndListBranches(t *testing.T) {
	e := newTestDiffMergeEngine(t)

	if err := e.CreateBranch("main", ""); err != nil {
		t.Fatalf("CreateBranch(main): %v", err)
	}
	if err := e.CreateBranch("feature", "main"); err != nil {
		t.Fatalf("CreateBranch(feature): %v", err)
	}

	branches := e.ListBranches()
	if len(branches) != 2 {
		t.Fatalf("ListBranches() len = %d, want 2", len(branches))
	}
	if branches[0] != "feature" || branches[1] != "main" {
		t.Errorf("branches = %v, want [feature main]", branches)
	}

	if err := e.CreateBranch("main", ""); err == nil {
		t.Error("expected error creating duplicate branch")
	}
	if err := e.CreateBranch("", "main"); err == nil {
		t.Error("expected error for empty branch name")
	}
}

func TestTSDiffMerge_DeleteBranch(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	e.CreateBranch("tmp", "main")

	if err := e.DeleteBranch("tmp"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}
	if len(e.ListBranches()) != 1 {
		t.Error("expected 1 branch after delete")
	}
	if err := e.DeleteBranch("nonexistent"); err == nil {
		t.Error("expected error deleting nonexistent branch")
	}
}

func TestTSDiffMerge_WriteAndRead(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")

	pts := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 100},
		{Metric: "cpu", Value: 2.0, Timestamp: 200},
		{Metric: "cpu", Value: 3.0, Timestamp: 300},
		{Metric: "mem", Value: 50.0, Timestamp: 100},
	}
	if err := e.WriteToBranch("main", pts); err != nil {
		t.Fatalf("WriteToBranch: %v", err)
	}

	result, err := e.ReadFromBranch("main", "cpu", 100, 200)
	if err != nil {
		t.Fatalf("ReadFromBranch: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("got %d points, want 2", len(result))
	}

	if _, err := e.ReadFromBranch("nope", "cpu", 0, 999); err == nil {
		t.Error("expected error reading from nonexistent branch")
	}

	result, err = e.ReadFromBranch("main", "disk", 0, 999)
	if err != nil {
		t.Fatalf("ReadFromBranch disk: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 points for missing metric, got %d", len(result))
	}

	if err := e.WriteToBranch("nope", pts); err == nil {
		t.Error("expected error writing to nonexistent branch")
	}
}

func TestTSDiffMerge_DiffBranches(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")

	basePts := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 100},
		{Metric: "cpu", Value: 2.0, Timestamp: 200},
	}
	e.WriteToBranch("main", basePts)
	e.CreateBranch("feature", "main")

	featurePts := []Point{
		{Metric: "cpu", Value: 99.0, Timestamp: 200},
		{Metric: "cpu", Value: 5.0, Timestamp: 300},
	}
	e.WriteToBranch("feature", featurePts)

	diff, err := e.DiffBranches("main", "feature")
	if err != nil {
		t.Fatalf("DiffBranches: %v", err)
	}

	if diff.SourceBranch != "main" || diff.TargetBranch != "feature" {
		t.Errorf("branch names mismatch: %s -> %s", diff.SourceBranch, diff.TargetBranch)
	}
	if len(diff.Metrics) == 0 {
		t.Error("expected at least one metric in diff")
	}
	// Very fast diffs may measure zero on platforms with coarse timer resolution.
	// ComputedAt verifies that timing metadata was populated without fabricating duration.
	if diff.ComputedAt.IsZero() {
		t.Error("expected diff computation timestamp")
	}

	if _, err := e.DiffBranches("nope", "feature"); err == nil {
		t.Error("expected error for nonexistent source")
	}
	if _, err := e.DiffBranches("main", "nope"); err == nil {
		t.Error("expected error for nonexistent target")
	}
}

func TestTSDiffMerge_DiffMetric(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("a", "")
	e.CreateBranch("b", "")

	e.WriteToBranch("a", []Point{
		{Metric: "m", Value: 10, Timestamp: 1},
		{Metric: "m", Value: 20, Timestamp: 2},
		{Metric: "m", Value: 30, Timestamp: 3},
	})
	e.WriteToBranch("b", []Point{
		{Metric: "m", Value: 10, Timestamp: 1},
		{Metric: "m", Value: 25, Timestamp: 2},
		{Metric: "m", Value: 40, Timestamp: 4},
	})

	sd, err := e.DiffMetric("a", "b", "m")
	if err != nil {
		t.Fatalf("DiffMetric: %v", err)
	}

	if sd.Summary.AddedCount != 1 {
		t.Errorf("AddedCount = %d, want 1", sd.Summary.AddedCount)
	}
	if sd.Summary.RemovedCount != 1 {
		t.Errorf("RemovedCount = %d, want 1", sd.Summary.RemovedCount)
	}
	if sd.Summary.ModifiedCount != 1 {
		t.Errorf("ModifiedCount = %d, want 1", sd.Summary.ModifiedCount)
	}
	if sd.Summary.TotalSource != 3 {
		t.Errorf("TotalSource = %d, want 3", sd.Summary.TotalSource)
	}
	if sd.Summary.TotalTarget != 3 {
		t.Errorf("TotalTarget = %d, want 3", sd.Summary.TotalTarget)
	}
}

func TestTSDiffMerge_MergeNoConflicts(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	e.WriteToBranch("main", []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 100},
	})
	e.CreateBranch("feature", "main")
	e.WriteToBranch("feature", []Point{
		{Metric: "cpu", Value: 5.0, Timestamp: 500},
	})

	result, err := e.Merge(DMMergeRequest{
		SourceBranch: "feature",
		TargetBranch: "main",
		Strategy:     "last-write-wins",
		Message:      "merge feature",
		Author:       "tester",
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if result.State != "completed" {
		t.Errorf("State = %s, want completed", result.State)
	}
	if result.Applied == 0 {
		t.Error("expected some applied changes")
	}
}

func TestTSDiffMerge_MergeWithConflicts(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	e.CreateBranch("feature", "")

	e.WriteToBranch("main", []Point{{Metric: "cpu", Value: 10, Timestamp: 1}})
	e.WriteToBranch("feature", []Point{{Metric: "cpu", Value: 20, Timestamp: 1}})

	// Manual strategy leaves conflicts unresolved.
	result, err := e.Merge(DMMergeRequest{
		SourceBranch: "main",
		TargetBranch: "feature",
		Strategy:     "manual",
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if result.State != "conflicts" {
		t.Errorf("State = %s, want conflicts", result.State)
	}
	if len(result.Conflicts) == 0 {
		t.Error("expected at least one conflict")
	}

	// Source-wins resolves automatically.
	result2, err := e.Merge(DMMergeRequest{
		SourceBranch: "main",
		TargetBranch: "feature",
		Strategy:     "source-wins",
	})
	if err != nil {
		t.Fatalf("Merge source-wins: %v", err)
	}
	if result2.State != "completed" {
		t.Errorf("State = %s, want completed", result2.State)
	}

	// Target-wins.
	result3, err := e.Merge(DMMergeRequest{
		SourceBranch: "main",
		TargetBranch: "feature",
		Strategy:     "target-wins",
	})
	if err != nil {
		t.Fatalf("Merge target-wins: %v", err)
	}
	if result3.State != "completed" {
		t.Errorf("State = %s, want completed", result3.State)
	}
}

func TestTSDiffMerge_MergeDryRun(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	e.CreateBranch("feature", "")
	e.WriteToBranch("feature", []Point{{Metric: "cpu", Value: 5, Timestamp: 1}})

	result, err := e.Merge(DMMergeRequest{
		SourceBranch: "feature",
		TargetBranch: "main",
		Strategy:     "last-write-wins",
		DryRun:       true,
	})
	if err != nil {
		t.Fatalf("Merge dry run: %v", err)
	}
	if result.State != "completed" {
		t.Errorf("State = %s, want completed", result.State)
	}

	pts, _ := e.ReadFromBranch("main", "cpu", 0, 999)
	if len(pts) != 0 {
		t.Errorf("expected 0 points in main after dry run, got %d", len(pts))
	}
}

func TestTSDiffMerge_CherryPick(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	e.CreateBranch("feature", "")

	e.WriteToBranch("feature", []Point{
		{Metric: "cpu", Value: 1, Timestamp: 100},
		{Metric: "cpu", Value: 2, Timestamp: 200},
		{Metric: "mem", Value: 50, Timestamp: 100},
	})

	result, err := e.CherryPick(DMCherryPickRequest{
		SourceBranch: "feature",
		TargetBranch: "main",
		Metrics:      []string{"cpu"},
		TimeRange:    [2]int64{100, 150},
		Message:      "cherry pick cpu",
	})
	if err != nil {
		t.Fatalf("CherryPick: %v", err)
	}
	if result.State != "completed" {
		t.Errorf("State = %s, want completed", result.State)
	}
	if result.Applied != 1 {
		t.Errorf("Applied = %d, want 1", result.Applied)
	}

	cpuPts, _ := e.ReadFromBranch("main", "cpu", 0, 999)
	if len(cpuPts) != 1 {
		t.Errorf("main cpu points = %d, want 1", len(cpuPts))
	}
	memPts, _ := e.ReadFromBranch("main", "mem", 0, 999)
	if len(memPts) != 0 {
		t.Errorf("main mem points = %d, want 0", len(memPts))
	}
}

func TestTSDiffMerge_CherryPickErrors(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")

	if _, err := e.CherryPick(DMCherryPickRequest{SourceBranch: "nope", TargetBranch: "main"}); err == nil {
		t.Error("expected error for nonexistent source")
	}
	if _, err := e.CherryPick(DMCherryPickRequest{SourceBranch: "main", TargetBranch: "nope"}); err == nil {
		t.Error("expected error for nonexistent target")
	}
}

func TestTSDiffMerge_ABTestCreateAndAnalyze(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("control", "")
	e.CreateBranch("variant", "")

	now := time.Now()
	for i := 0; i < 50; i++ {
		ts := now.Add(time.Duration(i) * time.Second).UnixNano()
		e.WriteToBranch("control", []Point{{Metric: "latency", Value: 100 + float64(i%5), Timestamp: ts}})
		e.WriteToBranch("variant", []Point{{Metric: "latency", Value: 80 + float64(i%5), Timestamp: ts}})
	}

	test, err := e.CreateABTest(DMABTest{
		Name:          "latency-test",
		ControlBranch: "control",
		VariantBranch: "variant",
		Metrics:       []string{"latency"},
		StartTime:     now.Add(-time.Hour),
		EndTime:       now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateABTest: %v", err)
	}
	if test.State != "created" {
		t.Errorf("State = %s, want created", test.State)
	}

	results, err := e.AnalyzeABTest(test.ID)
	if err != nil {
		t.Fatalf("AnalyzeABTest: %v", err)
	}
	if len(results.MetricResults) != 1 {
		t.Fatalf("MetricResults len = %d, want 1", len(results.MetricResults))
	}
	if results.Winner == "" {
		t.Error("expected a winner to be determined")
	}

	stored := e.GetABTest(test.ID)
	if stored.State != "completed" {
		t.Errorf("stored State = %s, want completed", stored.State)
	}
}

func TestTSDiffMerge_ABTestErrors(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")

	if _, err := e.CreateABTest(DMABTest{ControlBranch: "main", VariantBranch: "nope"}); err == nil {
		t.Error("expected error for nonexistent variant branch")
	}
	if _, err := e.CreateABTest(DMABTest{ControlBranch: "nope", VariantBranch: "main"}); err == nil {
		t.Error("expected error for nonexistent control branch")
	}
	if _, err := e.AnalyzeABTest("nonexistent"); err == nil {
		t.Error("expected error for nonexistent test")
	}
}

func TestTSDiffMerge_StatsTracking(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("a", "")
	e.CreateBranch("b", "")

	e.WriteToBranch("a", []Point{{Metric: "m", Value: 1, Timestamp: 1}})
	e.WriteToBranch("b", []Point{{Metric: "m", Value: 2, Timestamp: 1}})

	e.DiffBranches("a", "b")
	e.Merge(DMMergeRequest{SourceBranch: "a", TargetBranch: "b", Strategy: "source-wins"})
	e.CherryPick(DMCherryPickRequest{SourceBranch: "a", TargetBranch: "b"})

	stats := e.Stats()
	if stats.DiffsComputed < 1 {
		t.Errorf("DiffsComputed = %d, want >= 1", stats.DiffsComputed)
	}
	if stats.MergesCompleted < 1 {
		t.Errorf("MergesCompleted = %d, want >= 1", stats.MergesCompleted)
	}
	if stats.CherryPicks < 1 {
		t.Errorf("CherryPicks = %d, want >= 1", stats.CherryPicks)
	}
}

func TestTSDiffMerge_GetAndListMerges(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("a", "")
	e.CreateBranch("b", "")
	e.WriteToBranch("a", []Point{{Metric: "m", Value: 1, Timestamp: 1}})

	result, _ := e.Merge(DMMergeRequest{SourceBranch: "a", TargetBranch: "b"})
	if got := e.GetMerge(result.ID); got == nil {
		t.Error("GetMerge returned nil")
	}
	if got := e.ListMerges(); len(got) == 0 {
		t.Error("ListMerges returned empty")
	}
}

func TestTSDiffMerge_ListABTests(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("c", "")
	e.CreateBranch("v", "")

	e.CreateABTest(DMABTest{ControlBranch: "c", VariantBranch: "v", Metrics: []string{"m"}})
	if got := e.ListABTests(); len(got) != 1 {
		t.Errorf("ListABTests len = %d, want 1", len(got))
	}
}

func TestTSDiffMerge_HTTPStats(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	mux := http.NewServeMux()
	e.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/branches/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestTSDiffMerge_HTTPCreateBranch(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	mux := http.NewServeMux()
	e.RegisterHTTPHandlers(mux)

	body := strings.NewReader(`{"name":"main","base_branch":""}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/branches", body)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}
}

func TestTSDiffMerge_HTTPListBranches(t *testing.T) {
	e := newTestDiffMergeEngine(t)
	e.CreateBranch("main", "")
	mux := http.NewServeMux()
	e.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/branches", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
