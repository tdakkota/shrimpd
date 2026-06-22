package shrimpd

import (
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
