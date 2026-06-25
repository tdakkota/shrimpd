package shrimpblock

import (
	"cmp"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// BuildSparse builds a sparse index from a list of entries, sampling every `every` entries.
func BuildSparse(entries []shrimptypes.Entry, every int) []shrimptypes.SparseEntry {
	var out []shrimptypes.SparseEntry
	for i := 0; i < len(entries); i += every {
		out = append(out, shrimptypes.SparseEntry{Timestamp: entries[i].Timestamp, Idx: i})
	}
	return out
}

// BuildSparseFromPart walks a V2 part and builds a sparse index by sampling every Nth row
// across the entire part (global row index). Only reads TS arrays; never touches Blob.
func BuildSparseFromPart(pf *PartFileV2, every int) []shrimptypes.SparseEntry {
	if every <= 0 {
		every = 32
	}
	var out []shrimptypes.SparseEntry
	global := 0
	for bi := range pf.Headers {
		bb, err := ReadBinBlock(pf, bi)
		if err != nil {
			continue
		}
		for i := range bb.TS {
			if global%every == 0 {
				out = append(out, shrimptypes.SparseEntry{Timestamp: bb.TS[i], Idx: global})
			}
			global++
		}
	}
	return out
}

// SparseRange returns the range of indices in a sparse index that overlap with the given timestamp range.
func SparseRange(sparse []shrimptypes.SparseEntry, from, to int64) (lo, hi int) {
	const hiNotFound = 1<<31 - 1
	if len(sparse) == 0 {
		return 0, hiNotFound
	}

	// find first index where Ts >= from
	loIdx, _ := slices.BinarySearchFunc(sparse, from, func(e shrimptypes.SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if loIdx > 0 {
		loIdx-- // include previous sample
	}
	lo = sparse[loIdx].Idx

	// find first index where Ts > to
	// Search for to+1 with a standard three-way comparator so BinarySearchFunc
	// converges correctly (handles equality and produces exact insertion point).
	hiIdx, _ := slices.BinarySearchFunc(sparse, to+1, func(e shrimptypes.SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if hiIdx < len(sparse) {
		hi = sparse[hiIdx].Idx
	} else {
		hi = hiNotFound // will be clamped later
	}
	return lo, hi
}

// WriteSidecar writes a sparse index to a sidecar JSON file.
func WriteSidecar(path string, idx []shrimptypes.SparseEntry) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-sparse-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(idx); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}

// ReadSidecar reads a sparse index from a sidecar JSON file.
func ReadSidecar(path string) ([]shrimptypes.SparseEntry, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal sidecar path
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	var idx []shrimptypes.SparseEntry
	if err := json.NewDecoder(f).Decode(&idx); err != nil {
		return nil, err
	}
	return idx, nil
}
