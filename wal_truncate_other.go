//go:build !windows

package chronicle

import "os"

func truncateWALFile(file *os.File) error {
	return file.Truncate(0)
}
