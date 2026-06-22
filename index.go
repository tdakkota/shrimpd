package shrimpd

import (
	"cmp"
	"encoding/json"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
)

func tokenize(s string) iter.Seq[string] {
	return func(yield func(tok string) bool) {
		token := func(tok string) bool {
			tok = strings.ToLower(tok)
			return yield(tok)
		}
		seq := strings.FieldsFuncSeq(s, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		})
		for tok := range seq {
			if !token(tok) {
				return
			}
		}
	}
}

func buildTokenSet(entries []Entry) []string {
	var (
		seen = make(map[string]struct{})
		out  []string
	)
	for _, e := range entries {
		for tok := range tokenize(e.Data) {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				out = append(out, tok)
			}
		}
	}
	slices.Sort(out) // deterministic
	return out
}

func buildSparse(entries []Entry, every int) []SparseEntry {
	var out []SparseEntry
	for i := 0; i < len(entries); i += every {
		out = append(out, SparseEntry{Timestamp: entries[i].Timestamp, Idx: i})
	}
	return out
}

func writeSidecar(path string, idx []SparseEntry) error {
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
	return os.Rename(name, path)
}

func readSidecar(path string) ([]SparseEntry, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal sidecar path
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	var idx []SparseEntry
	if err := json.NewDecoder(f).Decode(&idx); err != nil {
		return nil, err
	}
	return idx, nil
}

func hasToken(tokens []string, term string) bool {
	if term == "" || len(tokens) == 0 {
		return true // empty term or no Tokens (legacy part or remote not reindexed locally): cannot prune
	}
	// tokens are sorted; require every sub-token from term to be present (case-insensitive)
	for tok := range tokenize(term) {
		if _, found := slices.BinarySearch(tokens, tok); !found {
			return false
		}
	}
	return true
}

func sparseRange(sparse []SparseEntry, from, to int64) (lo, hi int) {
	const hiNotFound = 1<<31 - 1
	if len(sparse) == 0 {
		return 0, hiNotFound
	}

	// find first index where Ts >= from
	loIdx, _ := slices.BinarySearchFunc(sparse, from, func(e SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if loIdx > 0 {
		loIdx-- // include previous sample
	}
	lo = sparse[loIdx].Idx

	// find first index where Ts > to
	// Search for to+1 with a standard three-way comparator so BinarySearchFunc
	// converges correctly (handles equality and produces exact insertion point).
	hiIdx, _ := slices.BinarySearchFunc(sparse, to+1, func(e SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if hiIdx < len(sparse) {
		hi = sparse[hiIdx].Idx
	} else {
		hi = hiNotFound // will be clamped later
	}
	return lo, hi
}
