//go:build windows

package chronicle

import (
	"errors"
	"os"
)

// Go opens O_APPEND handles without FILE_WRITE_DATA on Windows. Keep the
// append handle for writes, but obtain a separate writable handle for reset.
// The caller holds the WAL mutex throughout truncation and writer reset.
func truncateWALFile(file *os.File) error {
	resetFile, err := os.OpenFile(file.Name(), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer resetFile.Close()

	original, err := file.Stat()
	if err != nil {
		return err
	}
	target, err := resetFile.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(original, target) {
		return errors.New("WAL reset: path no longer refers to the open log")
	}
	return resetFile.Truncate(0)
}
