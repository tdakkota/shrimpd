package shrimplication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/tdakkota/shrimpd/internal/shrimpwal"
)

type stubRegistry struct {
	appendOp      LogOp
	appendPart    string
	appendOld     []string
	appendMeta    shrimptypes.PartMeta
	bootstrapSnap BootstrapSnapshot
}

func (s *stubRegistry) RegisterNode(context.Context, string) error         { return nil }
func (s *stubRegistry) GetLogs(context.Context, int64) ([]LogEntry, error) { return nil, nil }
func (s *stubRegistry) GetActiveParts(context.Context) (map[string]shrimptypes.PartMeta, error) {
	return nil, nil
}

func (s *stubRegistry) GetBootstrapSnapshot(context.Context) (BootstrapSnapshot, error) {
	return s.bootstrapSnap, nil
}
func (s *stubRegistry) logEntryExists(context.Context, int64) (bool, error)        { return false, nil }
func (s *stubRegistry) LogCleanupLoop(context.Context)                             {}
func (s *stubRegistry) GetQueuePointer(context.Context) (int64, error)             { return 0, nil }
func (s *stubRegistry) SetQueuePointer(context.Context, int64) error               { return nil }
func (s *stubRegistry) GetLivePeerAddrs(context.Context, string) ([]string, error) { return nil, nil }
func (s *stubRegistry) AppendLog(_ context.Context, op LogOp, part shrimptypes.PartMeta, oldParts []string) (int64, error) {
	s.appendOp = op
	s.appendPart = part.ID
	s.appendMeta = part
	s.appendOld = append([]string(nil), oldParts...)
	return 1, nil
}

func TestStreamingCompaction(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer wal.Close()

	reg := &stubRegistry{}
	lsm, err := NewLSM("n1", "127.0.0.1:0", dir, wal, reg)
	require.NoError(t, err)
	defer lsm.Close()

	parts := []shrimptypes.PartMeta{}
	for i := range 4 {
		id := fmt.Sprintf("part-%d", i)
		path := filepath.Join(dir, "parts", id+".json")
		entries := []shrimptypes.Entry{
			{Timestamp: int64(i*2 + 1), Data: "hello"},
			{Timestamp: int64(i*2 + 2), Data: "world"},
		}
		headers, err := shrimpblock.WritePartV2(path, entries)
		require.NoError(t, err)
		meta := shrimptypes.PartMeta{
			ID:            id,
			NodeID:        "n1",
			Level:         0,
			MinTimestamp:  entries[0].Timestamp,
			MaxTimestamp:  entries[len(entries)-1].Timestamp,
			Count:         len(entries),
			FormatVersion: 1,
			BlockCount:    len(headers),
		}
		parts = append(parts, meta)
	}
	lsm.SetParts(parts)

	require.NoError(t, lsm.compactLevel(context.Background(), 0, true))
	require.Len(t, reg.appendOld, 4)
	require.Equal(t, OpMerge, reg.appendOp)
	require.Equal(t, 8, reg.appendMeta.Count)
	require.Equal(t, int64(1), reg.appendMeta.MinTimestamp)
	require.Equal(t, int64(8), reg.appendMeta.MaxTimestamp)
	require.Equal(t, 1, reg.appendMeta.Level)
}
