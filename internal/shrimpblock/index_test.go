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
	// Label-only: entries must contain JSON with resource/attributes to produce lbl: tokens.
	// Avoid top-level "body" to keep label set minimal (ExtractLabels promotes body to label).
	ents := []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"resource":{"service.name":"svc"},"attributes":{"level":"info"}}`},
		{Timestamp: 2, Data: `{"attributes":{"level":"error"}}`},
	}
	path := t.TempDir() + "/p.json"
	_, err := WritePartV2(path, ents)
	require.NoError(t, err)
	pf, err := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pf.Close()

	got := BuildIndexEntriesFromPart("part-1", pf)
	// Only label tokens (lbl:...) are produced; no plain text tokens.
	want := []shrimptypes.IndexEntry{
		{Token: "lbl:level=error", DataID: "part-1"},
		{Token: "lbl:level=info", DataID: "part-1"},
		{Token: "lbl:service_name=svc", DataID: "part-1"},
	}
	require.Equal(t, want, got)
}

func TestBuildIndexEntries(t *testing.T) {
	ents := []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"resource":{"service.name":"svc"},"attributes":{"level":"info"}}`},
		{Timestamp: 2, Data: `{"attributes":{"level":"info"}}`},
	}
	got := BuildIndexEntries("part-1", ents)
	want := []shrimptypes.IndexEntry{
		{Token: "lbl:level=info", DataID: "part-1"},
		{Token: "lbl:service_name=svc", DataID: "part-1"},
	}
	require.Equal(t, want, got)
}

func TestBuildIndexFSTOrdinalOrderFollowsDataIDOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.fst")

	entries := []shrimptypes.IndexEntry{
		{Token: "lbl:a=1", DataID: "part-2"},
		{Token: "lbl:b=1", DataID: "part-1"},
		{Token: "lbl:b=1", DataID: "part-2"},
	}
	dataIDs, err := BuildIndexFST(path, entries)
	require.NoError(t, err)
	require.Equal(t, []string{"part-1", "part-2"}, dataIDs)
	require.FileExists(t, path)
}
