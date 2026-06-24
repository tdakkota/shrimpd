package shrimpwal

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "wal.jsonl")
	wal, err := OpenWAL(path)
	require.NoError(b, err)
	b.Cleanup(func() { _ = wal.Close() })

	entry := Entry{Timestamp: 1, Data: "benchmark entry"}

	b.ResetTimer()
	for b.Loop() {
		_ = wal.Append([]Entry{entry})
	}
}

// BenchmarkWALAppendParallel models the real ingest shape: many concurrent
// single-entry Appends. This is where group commit pays off — the concurrent
// fsyncs coalesce — so throughput should far exceed the serial BenchmarkWALAppend.
func BenchmarkWALAppendParallel(b *testing.B) {
	dir := b.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(b, err)
	b.Cleanup(func() { _ = wal.Close() })

	entry := Entry{Timestamp: 1, Data: "benchmark entry"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = wal.Append([]Entry{entry})
		}
	})
}

func BenchmarkWALAppendBatch(b *testing.B) {
	dir := b.TempDir()
	batchSize := 100

	entries := make([]Entry, batchSize)
	for i := range entries {
		entries[i] = Entry{Timestamp: int64(i), Data: "benchmark entry"}
	}

	// Benchmark the ceiling: single Append of many entries.
	// Each iteration creates a fresh WAL to avoid state artifacts.
	b.ResetTimer()
	for i := range b.N {
		path := filepath.Join(dir, fmt.Sprintf("wal-%d.jsonl", i))
		wal, err := OpenWAL(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = wal.Append(entries)
		_ = wal.Close()
	}
}
