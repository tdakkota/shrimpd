package shrimpblock

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/blevesearch/vellum"

	"github.com/oteldb/shrimpd/internal/fsyncutil"
	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// LabelTokenPrefix is the prefix used for label index tokens to separate them
// from plain text tokens in the FST composite key space.
const LabelTokenPrefix = "lbl:"

// BuildIndexEntries returns sorted, deduplicated [shrimptypes.IndexEntry]
// values for key/value pairs extracted by shrimpfilter.ExtractLabels.
func BuildIndexEntries(dataID string, entries []shrimptypes.Entry) []shrimptypes.IndexEntry {
	seen := make(map[string]struct{})
	var out []shrimptypes.IndexEntry

	add := func(token string) {
		if _, ok := seen[token]; !ok {
			seen[token] = struct{}{}
			out = append(out, shrimptypes.IndexEntry{Token: token, DataID: dataID})
		}
	}

	for _, e := range entries {
		labels := shrimpfilter.ExtractLabels(e.Data)
		for k, v := range labels {
			add(LabelTokenPrefix + k + "=" + v)
		}
	}

	slices.SortFunc(out, func(a, b shrimptypes.IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	return out
}

// BuildIndexEntriesFromPart walks a V2 part file and returns key/value index entries
// without materializing a full []Entry slice.
func BuildIndexEntriesFromPart(dataID string, pf *PartFileV2) []shrimptypes.IndexEntry {
	seen := make(map[string]struct{})
	var out []shrimptypes.IndexEntry

	add := func(token string) {
		if _, ok := seen[token]; !ok {
			seen[token] = struct{}{}
			out = append(out, shrimptypes.IndexEntry{Token: token, DataID: dataID})
		}
	}

	for bi := range pf.Headers {
		bb, err := ReadBinBlock(pf, bi)
		if err != nil {
			continue
		}
		for i := range bb.TS {
			s := string(bb.DataBytes(i))
			labels := shrimpfilter.ExtractLabels(s)
			for k, v := range labels {
				add(LabelTokenPrefix + k + "=" + v)
			}
		}
	}

	slices.SortFunc(out, func(a, b shrimptypes.IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	return out
}

// compositeKey builds the FST composite key: token + "\x00" + 2-byte big-endian ordinal.
func compositeKey(token string, ordinal uint16) []byte {
	key := make([]byte, len(token)+1+2)
	copy(key, token)
	key[len(token)] = '\x00'
	binary.BigEndian.PutUint16(key[len(token)+1:], ordinal)
	return key
}

// BuildIndexFST writes a vellum FST from sorted (token, dataID) pairs to path,
// using a temp-file + atomic rename. entries must be sorted by (Token, DataID).
// It interns DataIDs to uint16 ordinals and returns the DataIDs table (ordinal -> dataID).
func BuildIndexFST(path string, entries []shrimptypes.IndexEntry) ([]string, error) {
	// DataID ordinals must sort the same way as DataIDs because entries are
	// inserted in (Token, DataID) order and vellum requires sorted keys.
	seenDataIDs := make(map[string]struct{})
	for _, e := range entries {
		seenDataIDs[e.DataID] = struct{}{}
	}
	dataIDs := make([]string, 0, len(seenDataIDs))
	for dataID := range seenDataIDs {
		dataIDs = append(dataIDs, dataID)
	}
	slices.Sort(dataIDs)
	if len(dataIDs) > 1<<16 {
		return nil, fmt.Errorf("too many data IDs for index FST: %d", len(dataIDs))
	}
	ord := make(map[string]uint16, len(dataIDs))
	for i, dataID := range dataIDs {
		ord[dataID] = uint16(i)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-fst-")
	if err != nil {
		return nil, fmt.Errorf("create fst temp: %w", err)
	}
	tmpName := tmp.Name()

	builder, err := vellum.New(tmp, nil)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("new fst builder: %w", err)
	}

	var prevKey []byte
	for _, e := range entries {
		o := ord[e.DataID]
		key := compositeKey(e.Token, o)
		if bytes.Equal(key, prevKey) {
			continue // skip exact duplicates
		}
		if err := builder.Insert(key, 0); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return nil, fmt.Errorf("fst insert %q: %w", key, err)
		}
		prevKey = key
	}

	if err := builder.Close(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("fst builder close: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return nil, err
	}
	if err := fsyncutil.SyncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return dataIDs, nil
}

// ReadIndexMeta reads the index metadata from the specified path.
func ReadIndexMeta(path string) (shrimptypes.IndexPartMeta, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		return shrimptypes.IndexPartMeta{}, err
	}
	defer func() { _ = f.Close() }()
	var meta shrimptypes.IndexPartMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return shrimptypes.IndexPartMeta{}, err
	}
	return meta, nil
}

// WriteIndexMeta writes the index metadata to the specified path atomically.
func WriteIndexMeta(path string, meta shrimptypes.IndexPartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-meta-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(meta); err != nil {
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
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}
