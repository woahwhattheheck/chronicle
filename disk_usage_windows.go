//go:build windows

package chronicle

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// diskUsage returns free and total bytes for the filesystem containing path.
func diskUsage(path string) (free uint64, total uint64, err error) {
	// Check the existing parent directory so relative paths and UNC shares work.
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return 0, 0, err
	}

	ptr, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, 0, err
	}

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	err = windows.GetDiskFreeSpaceEx(ptr, &freeBytesAvailable, &totalBytes, &totalFreeBytes)
	if err != nil {
		return 0, 0, err
	}
	return freeBytesAvailable, totalBytes, nil
}
