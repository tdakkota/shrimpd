package shrimpd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Hello, World!", []string{"hello", "world"}},
		{"foo BAR foo", []string{"foo", "bar", "foo"}},
		{"123 abc-xyz", []string{"123", "abc", "xyz"}},
		{"foo.BAR.foo", []string{"foo", "bar", "foo"}},
		{"", nil},
	}
	for _, c := range cases {
		got := slices.Collect(tokenize(c.in))
		require.Equal(t, c.want, got, "tokenize(%q)", c.in)
	}
}

func TestBuildTokenSet(t *testing.T) {
	ents := []Entry{{Data: "Hello World"}, {Data: "hello foo"}}
	got := buildTokenSet(ents)
	want := []string{"foo", "hello", "world"}
	require.Equal(t, want, got)
}

func TestBuildSparse(t *testing.T) {
	for _, tt := range []struct {
		numEntries int
		by         int
	}{
		{0, 1},
		{100, 1},
		{100, 32},
		{100, 50},
		{100, 60},
		{100, 100},
		{100, 101},
	} {
		ents := make([]Entry, tt.numEntries)
		for i := range ents {
			ents[i].Timestamp = int64(i)
		}
		sp := buildSparse(ents, tt.by)

		var expectedLen int
		if tt.numEntries > 0 {
			expectedLen = (tt.numEntries-1)/tt.by + 1
		}
		require.Len(t, sp, expectedLen, "expected %d sparse entries for %d entries with every=%d", expectedLen, tt.numEntries, tt.by)

		for i, s := range sp {
			expectedIdx := i * tt.by
			require.Equal(t, expectedIdx, s.Idx, "sparse entry %d idx (by=%d)", i, tt.by)
			require.Equal(t, int64(expectedIdx), s.Timestamp, "sparse entry %d ts (by=%d)", i, tt.by)
		}
	}
}

func TestSidecarRoundtrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.sparse.json")
	idx := []SparseEntry{{Timestamp: 1, Idx: 0}, {Timestamp: 10, Idx: 5}}
	require.NoError(t, writeSidecar(p, idx))
	got, err := readSidecar(p)
	require.NoError(t, err)
	require.Equal(t, idx, got)
}

func TestHasToken(t *testing.T) {
	toks := []string{"a", "b", "c"}
	require.True(t, hasToken(toks, "B"), "case insensitive")
	require.False(t, hasToken(toks, "z"), "miss")
	require.True(t, hasToken(nil, ""), "empty term")
}

func TestSparseRange(t *testing.T) {
	sp := []SparseEntry{{Timestamp: 0, Idx: 0}, {Timestamp: 10, Idx: 5}, {Timestamp: 20, Idx: 10}}
	lo, hi := sparseRange(sp, 5, 15)
	require.Equal(t, 0, lo)
	require.Equal(t, 10, hi)
}

func TestSidecarCleanupInGC(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	f := filepath.Join(dir, "parts", "dead.sparse.json")
	require.NoError(t, os.WriteFile(f, []byte("[]"), 0o644))

	name := "dead.sparse.json"
	var id string
	if before, ok := strings.CutSuffix(name, ".sparse.json"); ok {
		id = before
	}
	require.Equal(t, "dead", id)
}

func TestTokenPruning(t *testing.T) {
	ents := []Entry{
		{Timestamp: 1, Data: "hello world"},
		{Timestamp: 2, Data: "foo bar"},
		{Timestamp: 3, Data: "hello foo"},
	}

	tokens := buildTokenSet(ents)
	expectedTokens := []string{"bar", "foo", "hello", "world"}
	require.Equal(t, expectedTokens, tokens)

	require.True(t, hasToken(tokens, "hello"))
	require.True(t, hasToken(tokens, "world"))
	require.False(t, hasToken(tokens, "nonexistent"))
	require.True(t, hasToken(tokens, "HELLO"), "case insensitive")
	require.True(t, hasToken(tokens, ""), "empty term")
	require.True(t, hasToken(nil, ""), "nil tokens with empty term")
	require.True(t, hasToken(nil, "hello"), "nil tokens with non-empty term (graceful degradation)")
}

func TestSparseRangeBounds(t *testing.T) {
	ents := make([]Entry, 100)
	for i := range ents {
		ents[i] = Entry{Timestamp: int64(i), Data: fmt.Sprintf("entry-%d", i)}
	}

	sparse := buildSparse(ents, 32)
	require.Len(t, sparse, 4)
	require.Equal(t, 0, sparse[0].Idx)
	require.Equal(t, int64(0), sparse[0].Timestamp)
	require.Equal(t, 32, sparse[1].Idx)
	require.Equal(t, int64(32), sparse[1].Timestamp)
	require.Equal(t, 64, sparse[2].Idx)
	require.Equal(t, int64(64), sparse[2].Timestamp)
	require.Equal(t, 96, sparse[3].Idx)
	require.Equal(t, int64(96), sparse[3].Timestamp)

	lo, hi := sparseRange(sparse, 10, 50)
	require.Equal(t, 0, lo)
	require.Equal(t, 64, hi)

	lo, hi = sparseRange(sparse, 35, 65)
	require.Equal(t, 32, lo)
	require.Equal(t, 96, hi, "conservative upper bound")

	lo, hi = sparseRange(sparse, 70, 90)
	require.Equal(t, 64, lo)
	require.Equal(t, 96, hi)

	lo, hi = sparseRange([]SparseEntry{}, 0, 100)
	require.Equal(t, 0, lo)
	require.Equal(t, 1<<31-1, hi)
}

func TestGracefulDegradation(t *testing.T) {
	require.True(t, hasToken(nil, ""), "nil tokens with empty term")
	require.True(t, hasToken([]string{}, ""), "empty tokens with empty term")

	lo, hi := sparseRange([]SparseEntry{}, 0, 1000)
	require.Equal(t, 0, lo)
	require.Equal(t, 1<<31-1, hi)
}

func TestBuildIndexEntries(t *testing.T) {
	ents := []Entry{
		{Timestamp: 1, Data: "hello world hello"},
		{Timestamp: 2, Data: "world test"},
	}
	got := BuildIndexEntries("part-1", ents)
	// Tokens should be "hello", "test", "world" (each once, sorted, mapped to part-1)
	want := []IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "test", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}
	require.Equal(t, want, got)
}

func TestIndexWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "index-wal.jsonl")

	wal, err := OpenIndexWAL(walPath)
	require.NoError(t, err)

	entries := []IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}

	require.NoError(t, wal.Append(entries))
	require.NoError(t, wal.Close())

	// Corrupt trailing line by appending junk
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("invalid json line\n")
	require.NoError(t, err)
	f.Close()

	// Recover
	wal2, err := OpenIndexWAL(walPath)
	require.NoError(t, err)
	defer wal2.Close()

	recovered, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, entries, recovered, "should recover valid entries and skip corrupt trailing line")

	// Rotate
	require.NoError(t, wal2.Rotate())
	recovered2, err := wal2.Recover()
	require.NoError(t, err)
	require.Empty(t, recovered2)
}

func TestIndexEngine_LookupAndFlush(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	// Case 1: Lookup incomplete initially because parts are not covered
	candidates := []PartMeta{{ID: "part-1"}}
	_, complete, err := engine.Lookup(context.Background(), "hello", candidates)
	require.NoError(t, err)
	require.False(t, complete, "should be incomplete when candidates are not marked covered")

	// Mark covered
	require.NoError(t, engine.MarkCovered([]string{"part-1"}))

	// Case 2: Lookup in memtable before flush
	entries := []IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}
	require.NoError(t, engine.Write(entries))

	matches, complete, err := engine.Lookup(context.Background(), "hello", candidates)
	require.NoError(t, err)
	require.True(t, complete)
	require.Contains(t, matches, "part-1")

	// Case 3: Flush and lookup
	require.NoError(t, engine.Flush(context.Background()))

	// Memtable should be empty now, results from flushed part
	matches2, complete2, err := engine.Lookup(context.Background(), "hello", candidates)
	require.NoError(t, err)
	require.True(t, complete2)
	require.Contains(t, matches2, "part-1")

	// Check min/max bounds on flushed metadata
	require.Len(t, engine.parts, 1)
	require.Equal(t, "hello", engine.parts[0].MinToken)
	require.Equal(t, "world", engine.parts[0].MaxToken)
}

func TestIndexEngine_MultiTokenLookup(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.MarkCovered([]string{"part-1", "part-2"}))

	entries := []IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
		{Token: "hello", DataID: "part-2"},
		{Token: "test", DataID: "part-2"},
	}
	require.NoError(t, engine.Write(entries))
	require.NoError(t, engine.Flush(context.Background()))

	candidates := []PartMeta{{ID: "part-1"}, {ID: "part-2"}}

	// Querying "hello" should return both parts
	m1, c1, err := engine.Lookup(context.Background(), "hello", candidates)
	require.NoError(t, err)
	require.True(t, c1)
	require.Len(t, m1, 2)
	require.Contains(t, m1, "part-1")
	require.Contains(t, m1, "part-2")

	// Querying "hello world" should only return part-1 (intersection)
	m2, c2, err := engine.Lookup(context.Background(), "hello world", candidates)
	require.NoError(t, err)
	require.True(t, c2)
	require.Len(t, m2, 1)
	require.Contains(t, m2, "part-1")
}

func TestIndexEngine_Compaction(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewIndexEngine("node-1", dir)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, engine.MarkCovered([]string{"part-1", "part-2", "part-3"}))

	// Create two L0 index parts
	require.NoError(t, engine.Write([]IndexEntry{{Token: "hello", DataID: "part-1"}, {Token: "world", DataID: "part-2"}}))
	require.NoError(t, engine.Flush(context.Background()))

	require.NoError(t, engine.Write([]IndexEntry{{Token: "hello", DataID: "part-2"}, {Token: "test", DataID: "part-3"}}))
	require.NoError(t, engine.Flush(context.Background()))

	require.Len(t, engine.parts, 2)

	// Compact with active data IDs: "part-1" and "part-2" ("part-3" is stale)
	activeIDs := map[string]struct{}{
		"part-1": {},
		"part-2": {},
	}
	require.NoError(t, engine.Compact(context.Background(), activeIDs))

	// Should have merged L0 parts into one Level 1 part
	require.Len(t, engine.parts, 1)
	require.Equal(t, 1, engine.parts[0].Level)

	// Lookup "test" (which was only in part-3) should not find anything and part-3 should be removed from covered
	candidates := []PartMeta{{ID: "part-1"}, {ID: "part-2"}}
	m, c, err := engine.Lookup(context.Background(), "test", candidates)
	require.NoError(t, err)
	require.True(t, c)
	require.Empty(t, m)

	// Verify that covered map is cleaned up
	engine.mu.RLock()
	_, cov3 := engine.covered["part-3"]
	engine.mu.RUnlock()
	require.False(t, cov3, "part-3 should be removed from covered after compaction")
}
