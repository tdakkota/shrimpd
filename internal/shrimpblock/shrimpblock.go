// Package shrimpblock provides a simple implementation of a block storage system for the shrimpd project.
package shrimpblock

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/oteldb/shrimpd/internal/fsyncutil"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// ReadBlock reads a [Block] from a local part file (V2 binary or legacy JSON).
func ReadBlock(p string) (shrimptypes.Block, error) {
	f, err := os.Open(p) // #nosec G304 -- trusted internal part path
	if err != nil {
		return shrimptypes.Block{}, err
	}
	r, _, err := OpenBlockReader(f)
	if err != nil {
		_ = f.Close()
		return shrimptypes.Block{}, err
	}
	var b shrimptypes.Block
	decodeErr := json.NewDecoder(r).Decode(&b)
	rCloseErr := r.Close()
	fCloseErr := f.Close()
	if decodeErr != nil {
		return shrimptypes.Block{}, decodeErr
	}
	if rCloseErr != nil {
		return shrimptypes.Block{}, rCloseErr
	}
	return b, fCloseErr
}

// WriteBlock writes b to path atomically via a temp-file + rename.
func WriteBlock(path string, b shrimptypes.Block, algo string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	cw, err := NewCompressingWriter(tmp, algo)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	encErr := json.NewEncoder(cw).Encode(b)
	closeErr := cw.Close()
	if encErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return encErr
	}
	if closeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return closeErr
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}
