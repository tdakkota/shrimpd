package shrimplication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/internal/shrimpblock"
	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
	"github.com/oteldb/shrimpd/internal/shrimpwal"
)

func TestLSM_QueryMatcher_Termless(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	// Write OTLP-shaped entries (flattened labels). Stay in memtable to avoid needing etcd for flush.
	err = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"severity_text":"INFO","body":"hello","resource":{"service.name":"svc-a"}}`},
		{Timestamp: 2, Data: `{"severity_text":"ERROR","body":"boom","resource":{"service.name":"svc-b"}}`},
		{Timestamp: 3, Data: `{"severity_text":"DEBUG","body":"trace","resource":{"service.name":"svc-a"}}`},
	})
	require.NoError(t, err)

	// Label eq: level=ERROR (memtable path)
	m, err := shrimpfilter.CompileMatcher(nil, []shrimpfilter.LabelFilter{
		{Label: "level", Op: shrimpfilter.OpLabelEq, Value: "ERROR"},
	})
	require.NoError(t, err)
	var got []shrimptypes.Entry
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
	require.Equal(t, int64(2), got[0].Timestamp)

	// Label re + line eq
	m2, err := shrimpfilter.CompileMatcher(
		[]shrimpfilter.LineFilter{{Op: shrimpfilter.OpLineEq, Value: "trace"}},
		[]shrimpfilter.LabelFilter{{Label: "service_name", Op: shrimpfilter.OpLabelRe, Value: "svc-.*"}},
	)
	require.NoError(t, err)
	got = nil
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m2, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
	require.Equal(t, int64(3), got[0].Timestamp)

	// Empty matcher = full scan of memtable
	got = nil
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, shrimpfilter.Matcher{}, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 3)
}

func TestLSM_QueryMatcher_Memtable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	_ = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 10, Data: `{"severity_text":"WARN","body":"m1","resource":{"service.name":"s1"}}`},
	})

	// Query directly from memtable (no flush -> no etcd needed)
	m, _ := shrimpfilter.CompileMatcher(nil, []shrimpfilter.LabelFilter{{Label: "service_name", Op: shrimpfilter.OpLabelEq, Value: "s1"}})
	var got []shrimptypes.Entry
	require.NoError(t, lsm.QueryMatcher(context.Background(), 0, 1<<62, m, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 1)
}

func TestLSM_QueryWithStats_Memtable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	err = lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello"},
		{Timestamp: 2, Data: "world"},
		{Timestamp: 3, Data: "hello shrimp"},
	})
	require.NoError(t, err)

	got, stats, err := lsm.QueryWithStats(context.Background(), 0, 1<<62, "hello")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, stats)
	require.Equal(t, 0, stats.PartsTotal)
	require.Equal(t, 0, stats.PartsScanned)
	require.Equal(t, 3, stats.EntriesScanned)
	require.Equal(t, 2, stats.EntriesMatched)
	require.GreaterOrEqual(t, stats.DurationMs, int64(0))
}

func TestLSM_QueryWithStats_EmptyPartTokensFallbackScansPart(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(dir + "/wal.jsonl")
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	entries := []shrimptypes.Entry{
		{Timestamp: 10, Data: "hello replicated part"},
		{Timestamp: 20, Data: "other line"},
	}
	partID := "remote-part"
	headers, err := shrimpblock.WritePartV2(filepath.Join(dir, "parts", partID+".json"), entries)
	require.NoError(t, err)

	lsm.parts = []shrimptypes.PartMeta{{
		ID:            partID,
		NodeID:        "n2",
		MinTimestamp:  10,
		MaxTimestamp:  20,
		Count:         len(entries),
		Tokens:        []string{},
		FormatVersion: 1,
		BlockCount:    len(headers),
	}}

	got, stats, err := lsm.QueryWithStats(context.Background(), 0, 100, "hello")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(10), got[0].Timestamp)
	require.Equal(t, 1, stats.PartsScanned)
	require.Zero(t, stats.PartsPrunedByIndex)
}

func TestQueryMatcherLabelBloomPruning(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))

	// Create entries: first 1024 with svc-a, next 1024 with svc-b
	totalEntries := 2048
	entries := make([]shrimptypes.Entry, totalEntries)
	for i := range totalEntries {
		svc := "svc-a"
		if i >= totalEntries/2 {
			svc = "svc-b"
		}
		entries[i] = shrimptypes.Entry{
			Timestamp: int64(i),
			Data:      fmt.Sprintf(`{"severity_text":"INFO","body":"msg %d","resource":{"service.name":%q}}`, i, svc),
		}
	}

	path := filepath.Join(dir, "parts", "test.json")
	headers, err := shrimpblock.WritePartV2(path, entries)
	require.NoError(t, err)
	require.Len(t, headers, 4) // 2048/512 = 4 blocks

	// Open the part and check bloom filter contents per block
	pf, err := shrimpblock.OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pf.Close()

	// Blocks 0-1 have only svc-a: bloom must NOT contain svc-b label
	for i := range 2 {
		require.False(t, shrimpblock.BloomMightContainLabel(&pf.Headers[i].Bloom, "service_name", "svc-b"),
			"block %d has svc-a only, bloom should not contain svc-b label", i)
		require.True(t, shrimpblock.BloomMightContainLabel(&pf.Headers[i].Bloom, "service_name", "svc-a"),
			"block %d has svc-a, bloom must contain svc-a label", i)
	}

	// Blocks 2-3 have only svc-b: bloom must NOT contain svc-a label
	for _, i := range []int{2, 3} {
		require.False(t, shrimpblock.BloomMightContainLabel(&pf.Headers[i].Bloom, "service_name", "svc-a"),
			"block %d has svc-b only, bloom should not contain svc-a label", i)
		require.True(t, shrimpblock.BloomMightContainLabel(&pf.Headers[i].Bloom, "service_name", "svc-b"),
			"block %d has svc-b, bloom must contain svc-b label", i)
	}
}
