//go:build windows

// Package fsyncutil provides a utility function to fsync a directory so that a rename is durable on disk.
package fsyncutil

import (
	"fmt"
	"os"
)

// SyncDir validates that path is a directory.
//
// Windows does not support syncing directory handles like Unix does, and
// attempting it returns "Access is denied". Callers already fsync the file data
// itself before rename, so on Windows this degrades to directory validation.
func SyncDir(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}
