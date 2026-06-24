package shrimpwal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWALSealDiscard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	wal, err := OpenWAL(path)
	require.NoError(t, err)

	require.NoError(t, wal.Append([]Entry{{Timestamp: 1, Data: "a"}}))
	require.NoError(t, wal.Append([]Entry{{Timestamp: 2, Data: "b"}}))

	// Seal: the two entries are now in the sealed segment; new writes go elsewhere.
	sealed, err := wal.Seal()
	require.NoError(t, err)

	require.NoError(t, wal.Append([]Entry{{Timestamp: 3, Data: "c"}}))

	// Before discard, every entry must still be recoverable.
	got, err := wal.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 1, Data: "a"}, {Timestamp: 2, Data: "b"}, {Timestamp: 3, Data: "c"}}, got)

	// After discarding the sealed segment, only the post-seal entry remains.
	require.NoError(t, wal.Discard(sealed))
	got, err = wal.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 3, Data: "c"}}, got)

	require.NoError(t, wal.Close())
}

// TestWALCrashBetweenSealAndDiscard simulates a crash after Seal but before
// Discard: the sealed entries must replay on restart (no data loss).
func TestWALCrashBetweenSealAndDiscard(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append([]Entry{{Timestamp: 1, Data: "pending"}}))
	_, err = wal.Seal()
	require.NoError(t, err)
	// A concurrent write lands in the fresh active segment during the "flush".
	require.NoError(t, wal.Append([]Entry{{Timestamp: 2, Data: "concurrent"}}))
	// Crash: close without discarding.
	require.NoError(t, wal.Close())

	wal2, err := OpenWAL(path)
	require.NoError(t, err)
	defer func() { _ = wal2.Close() }()
	got, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 1, Data: "pending"}, {Timestamp: 2, Data: "concurrent"}}, got,
		"both the sealed and the concurrently-written entries must survive a crash")
}

// TestWALReopenContinues verifies the highest segment is reused as active across
// reopen, so unflushed entries are not stranded and appends continue after them.
func TestWALReopenContinues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal.Append([]Entry{{Timestamp: 1, Data: "x"}}))
	require.NoError(t, wal.Close())

	wal2, err := OpenWAL(path)
	require.NoError(t, err)
	require.NoError(t, wal2.Append([]Entry{{Timestamp: 2, Data: "y"}}))
	got, err := wal2.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 1, Data: "x"}, {Timestamp: 2, Data: "y"}}, got)
	require.NoError(t, wal2.Close())
}

// TestWALLegacyMigration verifies a pre-segments single-file WAL is migrated and
// recovered rather than silently abandoned on upgrade.
func TestWALLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.jsonl")

	// Write a legacy single-file WAL by hand (one NDJSON line).
	require.NoError(t, os.WriteFile(path, []byte(`{"timestamp":7,"data":"legacy"}`+"\n"), 0o600))

	wal, err := OpenWAL(path)
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	got, err := wal.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 7, Data: "legacy"}}, got)

	// The legacy file should have been renamed into a segment.
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "legacy file should be migrated away")
}

// TestWALGroupCommitConcurrent runs many concurrent Appends (the group-commit
// path) and verifies every entry is durable and none are lost or corrupted.
// Run with -race to catch batching data races.
func TestWALGroupCommitConcurrent(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	const n = 200
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, wal.Append([]Entry{{Timestamp: int64(i), Data: "x"}}))
		}(i)
	}
	wg.Wait()

	got, err := wal.Recover()
	require.NoError(t, err)
	require.Len(t, got, n, "every concurrently-appended entry must be durable")

	seen := make(map[int64]bool, n)
	for _, e := range got {
		require.Equal(t, "x", e.Data)
		require.False(t, seen[e.Timestamp], "duplicate entry %d", e.Timestamp)
		seen[e.Timestamp] = true
	}
	require.Len(t, seen, n)
}

// TestWALGroupCommitCoalesces proves the fsyncs of concurrent appends are
// batched: with injected fsync latency, N concurrent appends complete with far
// fewer than N fsyncs. The assertion is on the fsync count (not wall-clock), so
// it is not timing-flaky.
func TestWALGroupCommitCoalesces(t *testing.T) {
	orig := syncFile
	syncFile = func(f *os.File) error {
		time.Sleep(2 * time.Millisecond) // make concurrent appends overlap
		return orig(f)
	}
	t.Cleanup(func() { syncFile = orig })

	dir := t.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	const n = 64
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, wal.Append([]Entry{{Timestamp: int64(i), Data: "x"}}))
		}(i)
	}
	wg.Wait()

	syncs := wal.sl.syncs.Load()
	require.Less(t, syncs, uint64(n),
		"group commit should coalesce fsyncs: got %d fsyncs for %d concurrent appends", syncs, n)

	got, err := wal.Recover()
	require.NoError(t, err)
	require.Len(t, got, n, "no entry lost despite batching")
}

// TestWALSealDrainsPending verifies Seal flushes bytes that were enqueued but
// not yet waited on, so nothing buffered is lost across a seal.
func TestWALSealDrainsPending(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	// Enqueue without waiting: the bytes sit in the pending batch.
	commit := wal.Enqueue([]Entry{{Timestamp: 1, Data: "buffered"}})

	// Seal must drain the pending batch to the sealed segment.
	sealed, err := wal.Seal()
	require.NoError(t, err)
	require.NoError(t, commit.Wait(), "the buffered entry was flushed by Seal")

	got, err := wal.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 1, Data: "buffered"}}, got)

	require.NoError(t, wal.Discard(sealed))
	got, err = wal.Recover()
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestWALDiscardIdempotent verifies Discard tolerates repeated/over-broad calls.
func TestWALDiscardIdempotent(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	require.NoError(t, wal.Append([]Entry{{Timestamp: 1, Data: "a"}}))
	sealed, err := wal.Seal()
	require.NoError(t, err)

	require.NoError(t, wal.Discard(sealed))
	require.NoError(t, wal.Discard(sealed))      // already gone
	require.NoError(t, wal.Discard(sealed+1000)) // never removes the active segment

	// Active segment is intact and writable.
	require.NoError(t, wal.Append([]Entry{{Timestamp: 2, Data: "b"}}))
	got, err := wal.Recover()
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 2, Data: "b"}}, got)
}
