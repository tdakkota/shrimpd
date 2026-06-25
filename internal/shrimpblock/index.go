package shrimpblock

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/blevesearch/vellum"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// LabelTokenPrefix is the prefix used for label index tokens to separate them
// from plain text tokens in the FST composite key space.
const LabelTokenPrefix = "lbl:"

// BuildIndexEntries tokenizes entries and returns sorted, deduplicated [shrimptypes.IndexEntry]
// values. Each entry produces both text tokens and label tokens (lbl:key=value).
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
		for tok := range Tokenize(e.Data) {
			add(tok)
		}
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

// BuildIndexEntriesFromPart walks a V2 part file and returns index entries without
// materializing a full []Entry slice. One transient string per row for Tokenize/ExtractLabels.
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
			for tok := range Tokenize(s) {
				add(tok)
			}
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

// compositeKey builds the FST composite key: token + "\x00" + dataID.
func compositeKey(token, dataID string) []byte {
	key := make([]byte, len(token)+1+len(dataID))
	copy(key, token)
	key[len(token)] = '\x00'
	copy(key[len(token)+1:], dataID)
	return key
}

// BuildIndexFST writes a vellum FST from sorted (token, dataID) pairs to path,
// using a temp-file + atomic rename. entries must be sorted by (Token, DataID).
func BuildIndexFST(path string, entries []shrimptypes.IndexEntry) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-fst-")
	if err != nil {
		return fmt.Errorf("create fst temp: %w", err)
	}
	tmpName := tmp.Name()

	builder, err := vellum.New(tmp, nil)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("new fst builder: %w", err)
	}

	var prevKey []byte
	for _, e := range entries {
		key := compositeKey(e.Token, e.DataID)
		if bytes.Equal(key, prevKey) {
			continue // skip exact duplicates
		}
		if err := builder.Insert(key, 0); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return fmt.Errorf("fst insert %q: %w", key, err)
		}
		prevKey = key
	}

	if err := builder.Close(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("fst builder close: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
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
