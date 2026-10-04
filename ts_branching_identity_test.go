package chronicle

import (
	"sync"
	"testing"
)

func TestTSBranchIdentityUniqueness(t *testing.T) {
	for name, generate := range map[string]func() string{
		"branch": generateBranchID,
		"commit": generateCommitID,
	} {
		t.Run(name, func(t *testing.T) {
			const workers, perWorker = 8, 1024
			ids := make(chan string, workers*perWorker)
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < perWorker; j++ {
						ids <- generate()
					}
				}()
			}
			wg.Wait()
			close(ids)
			seen := make(map[string]struct{}, workers*perWorker)
			for id := range ids {
				if _, exists := seen[id]; exists {
					t.Fatalf("%s allocator repeated ID %q", name, id)
				}
				seen[id] = struct{}{}
			}
		})
	}
}
func TestTSBranchHistoryIdentities(t *testing.T) {
	config := DefaultTSBranchConfig()
	config.AutoCleanup = false
	manager, err := NewTSBranchManager(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	main, err := manager.GetBranch("main")
	if err != nil {
		t.Fatal(err)
	}
	feature, err := manager.CreateBranch("feature", "main", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if feature.ID == main.ID {
		t.Fatal("new branch shares its parent's identity")
	}
	const count = 32
	want := make([]string, 0, count)
	for i := 0; i < count; i++ {
		err := manager.Write("feature", &Point{Metric: "counter", Timestamp: int64(i + 1), Value: float64(i)}, "", "")
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, feature.HeadCommit)
	}
	history, err := manager.GetCommitHistory("feature", count+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != count {
		t.Fatalf("history has %d commits, want %d", len(history), count)
	}
	seen := make(map[string]struct{}, count)
	for i, commit := range history {
		if commit.ID != want[count-1-i] {
			t.Fatalf("history item %d has unexpected identity", i)
		}
		if _, exists := seen[commit.ID]; exists {
			t.Fatalf("history repeats commit %q", commit.ID)
		}
		seen[commit.ID] = struct{}{}
		if commit.BranchID != feature.ID {
			t.Fatal("history changed branch identity")
		}
		if i+1 < len(history) && commit.ParentID != history[i+1].ID {
			t.Fatal("broken history parent link")
		}
	}
	if history[len(history)-1].ParentID != "" {
		t.Fatal("first write has unexpected parent")
	}
}
