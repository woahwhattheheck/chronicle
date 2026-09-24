//go:build windows

package chronicle

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// diskUsage returns free and total bytes for the filesystem containing path.
func diskUsage(path string) (free uint64, total uint64, err error) {
	dir := filepath.VolumeName(path)
	if dir == "" {
		abs, absErr := filepath.Abs(path)
		if absErr == nil {
			dir = filepath.VolumeName(abs)
			if dir == "" {
				dir = filepath.Dir(abs)
			}
		} else {
			dir = filepath.Dir(path)
		}
	}
	if dir == "" {
		dir = "."
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
