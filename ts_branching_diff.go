package chronicle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Diff, merge, and rebase operations for time-series branches.

// Diff computes the difference between two branches
func (bm *TSBranchManager) Diff(sourceBranch, targetBranch string) (*TSBranchDiff, error) {
	source, err := bm.GetBranch(sourceBranch)
	if err != nil {
		return nil, fmt.Errorf("source branch: %w", err)
	}

	target, err := bm.GetBranch(targetBranch)
	if err != nil {
		return nil, fmt.Errorf("target branch: %w", err)
	}

	diff := &TSBranchDiff{
		SourceBranch:  sourceBranch,
		TargetBranch:  targetBranch,
		SourceCommit:  source.HeadCommit,
		TargetCommit:  target.HeadCommit,
		Additions:     make([]Point, 0),
		Deletions:     make([]Point, 0),
		Modifications: make([]PointModification, 0),
		Conflicts:     make([]TSMergeConflict, 0),
	}

	// Find common ancestor
	diff.BaseCommit = bm.findCommonAncestor(source, target)

	// Get points from both branches
	sourcePoints := bm.getBranchPoints(source)
	targetPoints := bm.getBranchPoints(target)

	// Build maps for comparison
	sourceMap := make(map[string]Point)
	for _, p := range sourcePoints {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		sourceMap[key] = p
	}

	targetMap := make(map[string]Point)
	for _, p := range targetPoints {
		key := fmt.Sprintf("%s:%d", p.Metric, p.Timestamp)
		targetMap[key] = p
	}

	// Find additions (in source but not in target)
	seriesSet := make(map[string]bool)
	for key, p := range sourceMap {
		seriesSet[p.Metric] = true
		if _, exists := targetMap[key]; !exists {
			diff.Additions = append(diff.Additions, p)
		} else if targetMap[key].Value != p.Value {
			diff.Modifications = append(diff.Modifications, PointModification{
				Series:      p.Metric,
				Timestamp:   p.Timestamp,
				SourceValue: p.Value,
				TargetValue: targetMap[key].Value,
			})
		}
	}

	// Find deletions (in target but not in source)
	for key, p := range targetMap {
		seriesSet[p.Metric] = true
		if _, exists := sourceMap[key]; !exists {
			diff.Deletions = append(diff.Deletions, p)
		}
	}

	// Calculate stats
	diff.Stats = DiffStats{
		TotalChanges:   len(diff.Additions) + len(diff.Deletions) + len(diff.Modifications),
		Additions:      len(diff.Additions),
		Deletions:      len(diff.Deletions),
		Modifications:  len(diff.Modifications),
		Conflicts:      len(diff.Conflicts),
		SeriesAffected: len(seriesSet),
	}

	return diff, nil
}

func (bm *TSBranchManager) findCommonAncestor(source, target *TSBranch) string {
	// Simple implementation - find first shared parent
	sourceAncestors := make(map[string]bool)

	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	// Build source ancestor set
	current := source
	for current != nil {
		sourceAncestors[current.ID] = true
		if current.Parent == "" {
			break
		}
		for _, b := range bm.branches {
			if b.ID == current.Parent {
				current = b
				break
			}
		}
	}

	// Find first target ancestor in source set
	current = target
	for current != nil {
		if sourceAncestors[current.ID] {
			return current.HeadCommit
		}
		if current.Parent == "" {
			break
		}
		for _, b := range bm.branches {
			if b.ID == current.Parent {
				current = b
				break
			}
		}
	}

	return ""
}

func (bm *TSBranchManager) getBranchPoints(branch *TSBranch) []Point {
	points := make([]Point, 0)

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	if deltas, exists := bm.deltas[branch.ID]; exists {
		for _, seriesPoints := range deltas {
			points = append(points, seriesPoints...)
		}
	}

	return points
}

// Merge merges source branch into target branch
func (bm *TSBranchManager) Merge(sourceBranch, targetBranch, author, message string, strategy TSMergeStrategy) (*TSMergeResult, error) {
	if strategy == "" {
		strategy = bm.config.DefaultMergeStrategy
	}

	// Get diff
	diff, err := bm.Diff(sourceBranch, targetBranch)
	if err != nil {
		return nil, err
	}

	result := &TSMergeResult{
		Strategy:  strategy,
		Stats:     diff.Stats,
		Conflicts: make([]TSMergeConflict, 0),
	}

	// Check for conflicts in modifications
	for _, mod := range diff.Modifications {
		conflict := TSMergeConflict{
			Series:      mod.Series,
			Timestamp:   mod.Timestamp,
			SourceValue: mod.SourceValue,
			TargetValue: mod.TargetValue,
		}

		// Resolve conflict based on strategy
		var resolved float64
		switch strategy {
		case TSMergeStrategyLastWrite:
			resolved = mod.SourceValue // Assume source is newer
		case TSMergeStrategyFirstWrite:
			resolved = mod.TargetValue
		case TSMergeStrategyMax:
			resolved = tsMax(mod.SourceValue, mod.TargetValue)
		case TSMergeStrategyMin:
			resolved = tsMin(mod.SourceValue, mod.TargetValue)
		case TSMergeStrategyAverage:
			resolved = (mod.SourceValue + mod.TargetValue) / 2
		case TSMergeStrategySum:
			resolved = mod.SourceValue + mod.TargetValue
		case TSMergeStrategySource:
			resolved = mod.SourceValue
		case TSMergeStrategyTarget:
			resolved = mod.TargetValue
		case TSMergeStrategyManual:
			result.Conflicts = append(result.Conflicts, conflict)
			atomic.AddInt64(&bm.mergeConflicts, 1)
			continue
		}

		conflict.Resolution = &resolved
		conflict.Strategy = strategy
		result.Conflicts = append(result.Conflicts, conflict)
	}

	// If manual strategy and has conflicts, return for resolution
	if strategy == TSMergeStrategyManual && len(diff.Modifications) > 0 {
		result.Success = false
		result.Error = "merge has conflicts requiring manual resolution"
		return result, nil
	}

	// Apply additions to target branch
	target, _ := bm.GetBranch(targetBranch)

	pointsToAdd := make([]Point, 0)
	for _, p := range diff.Additions {
		pointsToAdd = append(pointsToAdd, p)
	}

	// Apply resolved conflicts
	for _, conflict := range result.Conflicts {
		if conflict.Resolution != nil {
			pointsToAdd = append(pointsToAdd, Point{
				Metric:    conflict.Series,
				Timestamp: conflict.Timestamp,
				Value:     *conflict.Resolution,
			})
		}
	}

	if len(pointsToAdd) > 0 {
		if err := bm.WriteBatch(targetBranch, pointsToAdd, author, message); err != nil {
			result.Success = false
			result.Error = err.Error()
			return result, nil
		}
	}

	result.Success = true
	result.MergeCommit = target.HeadCommit
	result.Stats.Conflicts = len(result.Conflicts)

	atomic.AddInt64(&bm.totalMerges, 1)

	return result, nil
}

// Rebase rebases source branch onto target branch
func (bm *TSBranchManager) Rebase(sourceBranch, targetBranch string) error {
	source, err := bm.GetBranch(sourceBranch)
	if err != nil {
		return err
	}

	target, err := bm.GetBranch(targetBranch)
	if err != nil {
		return err
	}

	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	// Update source branch's parent to target
	source.Parent = target.ID
	source.BaseCommit = target.HeadCommit
	source.UpdatedAt = time.Now()

	return nil
}

// GetCommit returns a commit by ID
func (bm *TSBranchManager) GetCommit(commitID string) (*BranchCommit, error) {
	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	commit, exists := bm.commits[commitID]
	if !exists {
		return nil, fmt.Errorf("commit not found: %s", commitID)
	}
	return commit, nil
}

// GetCommitHistory returns commit history for a branch
func (bm *TSBranchManager) GetCommitHistory(branchName string, limit int) ([]*BranchCommit, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return nil, err
	}

	bm.branchMu.RLock()
	defer bm.branchMu.RUnlock()

	history := make([]*BranchCommit, 0)
	currentID := branch.HeadCommit

	for currentID != "" && len(history) < limit {
		commit, exists := bm.commits[currentID]
		if !exists {
			break
		}
		history = append(history, commit)
		currentID = commit.ParentID
	}

	return history, nil
}

// Checkout switches the working branch context
func (bm *TSBranchManager) Checkout(branchName string) (*TSBranch, error) {
	return bm.GetBranch(branchName)
}

// Reset resets a branch to a specific commit
func (bm *TSBranchManager) Reset(branchName, commitID string) error {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	branch, exists := bm.branches[branchName]
	if !exists || branch.Deleted {
		return fmt.Errorf("branch not found: %s", branchName)
	}

	// Verify commit exists
	if _, exists := bm.commits[commitID]; !exists {
		return fmt.Errorf("commit not found: %s", commitID)
	}

	branch.HeadCommit = commitID
	branch.UpdatedAt = time.Now()

	return nil
}

// Tag creates a named reference to a commit
func (bm *TSBranchManager) Tag(branchName, tagName, commitID string) error {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return err
	}

	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	if commitID == "" {
		commitID = branch.HeadCommit
	}

	if branch.Metadata == nil {
		branch.Metadata = make(map[string]string)
	}
	branch.Metadata["tag:"+tagName] = commitID

	return nil
}

// GetTag returns the commit ID for a tag
func (bm *TSBranchManager) GetTag(branchName, tagName string) (string, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return "", err
	}

	commitID, exists := branch.Metadata["tag:"+tagName]
	if !exists {
		return "", fmt.Errorf("tag not found: %s", tagName)
	}
	return commitID, nil
}

// Stats returns branch manager statistics
func (bm *TSBranchManager) Stats() BranchManagerStats {
	bm.branchMu.RLock()
	activeBranches := 0
	for _, b := range bm.branches {
		if !b.Deleted {
			activeBranches++
		}
	}
	bm.branchMu.RUnlock()

	return BranchManagerStats{
		TotalBranches:  atomic.LoadInt64(&bm.totalBranches),
		ActiveBranches: int64(activeBranches),
		TotalCommits:   atomic.LoadInt64(&bm.totalCommits),
		TotalMerges:    atomic.LoadInt64(&bm.totalMerges),
		MergeConflicts: atomic.LoadInt64(&bm.mergeConflicts),
	}
}

// BranchManagerStats contains branch manager statistics
type BranchManagerStats struct {
	TotalBranches  int64 `json:"total_branches"`
	ActiveBranches int64 `json:"active_branches"`
	TotalCommits   int64 `json:"total_commits"`
	TotalMerges    int64 `json:"total_merges"`
	MergeConflicts int64 `json:"merge_conflicts"`
}

// Close shuts down the branch manager
func (bm *TSBranchManager) Close() error {
	bm.cancel()
	bm.wg.Wait()
	return nil
}

func (bm *TSBranchManager) cleanupWorker() {
	defer bm.wg.Done()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-bm.ctx.Done():
			return
		case <-ticker.C:
			bm.cleanupDeletedBranches()
		}
	}
}

func (bm *TSBranchManager) cleanupDeletedBranches() {
	bm.branchMu.Lock()
	defer bm.branchMu.Unlock()

	cutoff := time.Now().Add(-bm.config.DeletedRetention)
	for name, branch := range bm.branches {
		if branch.Deleted && branch.DeletedAt != nil && branch.DeletedAt.Before(cutoff) {
			// Clean up delta storage
			bm.deltaMu.Lock()
			delete(bm.deltas, branch.ID)
			bm.deltaMu.Unlock()

			delete(bm.branches, name)
		}
	}
}

// Helper functions

// Branch and commit registries are process-local. A sequence keeps their keys
// distinct when concurrent operations observe the same or a rolled-back clock.
var tsBranchIdentitySequence uint64

func generateBranchID() string {
	return fmt.Sprintf("br_%d_%d", time.Now().UnixNano(), atomic.AddUint64(&tsBranchIdentitySequence, 1))
}

func generateCommitID() string {
	return fmt.Sprintf("cm_%d_%d", time.Now().UnixNano(), atomic.AddUint64(&tsBranchIdentitySequence, 1))
}

func computeCommitHash(commit *BranchCommit) string {
	data, _ := json.Marshal(commit.Changes)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func tsMax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func tsMin(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// JSON helpers
func (b *TSBranch) MarshalJSON() ([]byte, error) {
	type Alias TSBranch
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(b),
	})
}

// String returns a string representation of the branch
func (b *TSBranch) String() string {
	return fmt.Sprintf("TSBranch{Name: %s, ID: %s, Parent: %s}", b.Name, b.ID, b.Parent)
}

// IsDirty returns true if the branch has uncommitted changes
func (bm *TSBranchManager) IsDirty(branchName string) bool {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return false
	}

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	if deltas, exists := bm.deltas[branch.ID]; exists {
		for _, points := range deltas {
			if len(points) > 0 {
				return true
			}
		}
	}
	return false
}

// GetBranchSeries returns all series names in a branch
func (bm *TSBranchManager) GetBranchSeries(branchName string) ([]string, error) {
	branch, err := bm.GetBranch(branchName)
	if err != nil {
		return nil, err
	}

	bm.deltaMu.RLock()
	defer bm.deltaMu.RUnlock()

	series := make([]string, 0)
	if deltas, exists := bm.deltas[branch.ID]; exists {
		for s := range deltas {
			series = append(series, s)
		}
	}

	sort.Strings(series)
	return series, nil
}

// CompareBranches returns a summary comparison between two branches
func (bm *TSBranchManager) CompareBranches(branch1, branch2 string) (string, error) {
	diff, err := bm.Diff(branch1, branch2)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Comparing %s -> %s\n", branch1, branch2))
	sb.WriteString(fmt.Sprintf("  Additions: %d\n", diff.Stats.Additions))
	sb.WriteString(fmt.Sprintf("  Deletions: %d\n", diff.Stats.Deletions))
	sb.WriteString(fmt.Sprintf("  Modifications: %d\n", diff.Stats.Modifications))
	sb.WriteString(fmt.Sprintf("  Series affected: %d\n", diff.Stats.SeriesAffected))

	return sb.String(), nil
}
