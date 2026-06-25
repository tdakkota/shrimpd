package shrimplication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/tdakkota/shrimpd/internal/shrimpwal"
)

func newTestLSM(t *testing.T) (*LSM, *stubRegistry, string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	reg := &stubRegistry{}
	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, reg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lsm.Close() })
	return lsm, reg, dir
}

func writeTestPart(t *testing.T, dir, id string, entries []shrimptypes.Entry) shrimptypes.PartMeta {
	t.Helper()
	path := filepath.Join(dir, "parts", id+".json")
	headers, err := shrimpblock.WritePartV2(path, entries)
	require.NoError(t, err)
	return shrimptypes.PartMeta{
		ID:            id,
		NodeID:        "n1",
		Level:         0,
		MinTimestamp:  entries[0].Timestamp,
		MaxTimestamp:  entries[len(entries)-1].Timestamp,
		Count:         len(entries),
		FormatVersion: 1,
		BlockCount:    len(headers),
	}
}

// TestOpMergeClearsPendingParts verifies that applying an OpMerge log entry removes
// the old part IDs from pendingParts/pendingAttempts. Without this fix a stale pending
// download could resurrect the superseded part and produce duplicate query results.
func TestOpMergeClearsPendingParts(t *testing.T) {
	lsm, _, dir := newTestLSM(t)

	old1 := writeTestPart(t, dir, "old-1", []shrimptypes.Entry{{Timestamp: 1, Data: "a"}})
	old2 := writeTestPart(t, dir, "old-2", []shrimptypes.Entry{{Timestamp: 2, Data: "b"}})
	merged := writeTestPart(t, dir, "merged", []shrimptypes.Entry{
		{Timestamp: 1, Data: "a"},
		{Timestamp: 2, Data: "b"},
	})

	lsm.SetParts([]shrimptypes.PartMeta{old1, old2})

	// Simulate old-1 being in pendingParts (e.g. replication was deferred).
	lsm.mu.Lock()
	lsm.pendingParts[old1.ID] = old1
	lsm.pendingParts[old2.ID] = old2
	lsm.mu.Unlock()

	// A peer announces that old-1 and old-2 were merged into merged.
	entry := LogEntry{
		NodeID:   "n2",
		Op:       OpMerge,
		Part:     merged,
		OldParts: []string{old1.ID, old2.ID},
	}

	// Serve merged part from a test HTTP server so applyLogEntry can fetch it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(dir, "parts", "merged.json"))
	}))
	t.Cleanup(srv.Close)
	entry.Part.Addr = srv.Listener.Addr().String()

	require.NoError(t, lsm.applyLogEntry(context.Background(), entry, nil))

	lsm.mu.RLock()
	_, has1 := lsm.pendingParts[old1.ID]
	_, has2 := lsm.pendingParts[old2.ID]
	lsm.mu.RUnlock()

	require.False(t, has1, "old-1 should have been removed from pendingParts after OpMerge")
	require.False(t, has2, "old-2 should have been removed from pendingParts after OpMerge")
}

// TestBootstrapFromPartsPrunesPending verifies that bootstrapFromParts removes
// pendingParts entries whose IDs are no longer in the snapshot (merged/GC'd).
func TestBootstrapFromPartsPrunesPending(t *testing.T) {
	lsm, reg, dir := newTestLSM(t)

	current := writeTestPart(t, dir, "current", []shrimptypes.Entry{{Timestamp: 1, Data: "x"}})
	// Write meta file so bootstrapFromParts finds it on disk and skips the remote fetch.
	require.NoError(t, WriteMeta(lsm.partMetaPath(current.ID), current))

	// Seed a stale pending entry for a part not in the new snapshot.
	lsm.mu.Lock()
	lsm.pendingParts["stale-part"] = shrimptypes.PartMeta{ID: "stale-part", Addr: "127.0.0.1:0"}
	lsm.mu.Unlock()

	// Snapshot contains only "current".
	reg.bootstrapSnap = BootstrapSnapshot{
		Parts: map[string]shrimptypes.PartMeta{current.ID: current},
	}

	require.NoError(t, lsm.bootstrapFromParts(context.Background()))

	lsm.mu.RLock()
	_, staleStillPresent := lsm.pendingParts["stale-part"]
	lsm.mu.RUnlock()

	require.False(t, staleStillPresent, "stale-part should have been pruned from pendingParts after bootstrapFromParts")
}

// TestFetchRemotePartFallback verifies that fetchRemotePart falls back to an extra
// candidate when the origin address returns a 404.
func TestFetchRemotePartFallback(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))

	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello"},
		{Timestamp: 2, Data: "world"},
	}
	partPath := filepath.Join(dir, "parts", "p1.json")
	_, err := shrimpblock.WritePartV2(partPath, entries)
	require.NoError(t, err)

	// Origin always 404s (simulates the part being GC'd after a merge).
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(origin.Close)

	// A live peer that has the part.
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, partPath)
	}))
	t.Cleanup(peer.Close)

	meta := shrimptypes.PartMeta{
		ID:            "p1",
		Addr:          origin.Listener.Addr().String(),
		FormatVersion: 1,
	}
	candidates := []string{peer.Listener.Addr().String()}

	raw, block, err := fetchRemotePart(context.Background(), meta, candidates, &http.Client{})
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.Len(t, block.Data, 2, "expected 2 entries decoded from the peer")
}

// stubRegistry satisfies registryAPI; its fields and methods are defined in lsm_compact_test.go.
var _ registryAPI = (*stubRegistry)(nil)
