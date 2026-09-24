//go:build unix

package chronicle

import (
	"path/filepath"
	"syscall"
)

// diskUsage returns free and total bytes for the filesystem containing path.
func diskUsage(path string) (free uint64, total uint64, err error) {
	dir := filepath.Dir(path)
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, 0, err
	}
	total = stat.Blocks * uint64(stat.Bsize)
	free = stat.Bavail * uint64(stat.Bsize)
	return free, total, nil
}
