//go:build !windows

// Package fsyncutil provides a utility function to fsync a directory so that a rename is durable on disk.
package fsyncutil

import (
	"fmt"
	"os"
)

// SyncDir fsyncs a directory so a rename is durable on disk.
func SyncDir(path string) error {
	d, err := os.Open(path) // #nosec G304 -- it's verified this is a directory
	if err != nil {
		return err
	}
	defer func() {
		_ = d.Close()
	}()
	fi, err := d.Stat()
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return d.Sync()
}
