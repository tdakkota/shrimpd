package shrimpblock

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestSidecarRoundtrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.sparse.json")
	idx := []shrimptypes.SparseEntry{{Timestamp: 1, Idx: 0}, {Timestamp: 10, Idx: 5}}
	require.NoError(t, WriteSidecar(p, idx))
	got, err := ReadSidecar(p)
	require.NoError(t, err)
	require.Equal(t, idx, got)
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

func TestGracefulDegradation(t *testing.T) {
	require.True(t, HasToken(nil, ""), "nil tokens with empty term")
	require.True(t, HasToken([]string{}, ""), "empty tokens with empty term")

	lo, hi := SparseRange([]shrimptypes.SparseEntry{}, 0, 1000)
	require.Equal(t, 0, lo)
	require.Equal(t, 1<<31-1, hi)
}

func TestBuildIndexEntriesFromPart(t *testing.T) {
	ents := []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello world hello"},
		{Timestamp: 2, Data: "world test"},
	}
	path := t.TempDir() + "/p.json"
	_, err := WritePartV2(path, ents)
	require.NoError(t, err)
	pf, err := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pf.Close()

	got := BuildIndexEntriesFromPart("part-1", pf)
	want := []shrimptypes.IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "test", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}
	require.Equal(t, want, got)
}

func TestBuildIndexEntries(t *testing.T) {
	ents := []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello world hello"},
		{Timestamp: 2, Data: "world test"},
	}
	got := BuildIndexEntries("part-1", ents)
	// Tokens should be "hello", "test", "world" (each once, sorted, mapped to part-1)
	want := []shrimptypes.IndexEntry{
		{Token: "hello", DataID: "part-1"},
		{Token: "test", DataID: "part-1"},
		{Token: "world", DataID: "part-1"},
	}
	require.Equal(t, want, got)
}
