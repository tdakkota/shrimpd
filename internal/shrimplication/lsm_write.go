package shrimplication

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// Write is safe for concurrent use. Durable after WAL fsync.
func (l *LSM) Write(_ context.Context, entries []shrimptypes.Entry) error {
	if err := l.wal.Append(entries); err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	l.mem.Write(entries)
	if l.mem.Len() >= flushThreshold {
		select {
		case l.flushSig <- struct{}{}:
		default: // already signaled
		}
	}
	return nil
}

// flush drains the memtable, writes an immutable part file atomically,
// commits metadata, and appends a PUT operation to the etcd log.
func (l *LSM) flush(ctx context.Context) error {
	entries := l.mem.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	slices.SortFunc(entries, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	slog.DebugContext(ctx, "creating new part", "id", id, "count", len(entries))

	blockHeaders, err := shrimpblock.WritePartV2(path, entries)
	if err != nil {
		l.mem.Write(entries) // restore on failure so next flush retries
		return fmt.Errorf("write v2 part: %w", err)
	}

	meta := shrimptypes.PartMeta{
		ID:            id,
		NodeID:        l.nodeID,
		Level:         0,
		MinTimestamp:  entries[0].Timestamp,
		MaxTimestamp:  entries[len(entries)-1].Timestamp,
		Count:         len(entries),
		Addr:          l.addr,
		Tokens:        shrimpblock.BuildTokenSet(entries),
		Compression:   shrimpblock.CompressionZstd,
		FormatVersion: 1,
		BlockCount:    len(blockHeaders),
	}

	if err := WriteMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		l.mem.Write(entries)
		return fmt.Errorf("write meta: %w", err)
	}

	if _, err := l.reg.AppendLog(ctx, OpPut, meta, nil); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaPath)
		l.mem.Write(entries)
		return fmt.Errorf("append log: %w", err)
	}

	// Safe to truncate WAL: entries are now durable in the part file and etcd log.
	if err := l.wal.Rotate(); err != nil {
		slog.WarnContext(ctx, "wal rotate failed", "error", err)
	}

	l.mu.Lock()
	has := false
	for _, p := range l.parts {
		if p.ID == meta.ID {
			has = true
			break
		}
	}
	if !has {
		l.parts = append(l.parts, meta)
	}
	l.mu.Unlock()

	idxEntries := shrimpblock.BuildIndexEntries(meta.ID, entries)
	if err := l.idxEngine.Write(idxEntries); err != nil {
		slog.WarnContext(ctx, "failed to write index entries on flush", "id", meta.ID, "error", err)
	} else {
		if err := l.idxEngine.MarkCovered([]string{meta.ID}); err != nil {
			slog.WarnContext(ctx, "failed to mark covered on flush", "id", meta.ID, "error", err)
		}
	}

	slog.InfoContext(ctx, "flushed part", "id", id, "level", 0, "count", meta.Count, "min_timestamp", meta.MinTimestamp, "max_timestamp", meta.MaxTimestamp)
	return nil
}

// Flush forces an immediate flush of the data memtable and the index memtable.
func (l *LSM) Flush(ctx context.Context) error {
	if err := l.flush(ctx); err != nil {
		return err
	}
	return l.idxEngine.Flush(ctx)
}

// writeRawPart writes raw part bytes to path atomically via a temp-file rename,
// preserving whatever on-disk format (V2 binary or compressed JSON) was fetched.
func writeRawPart(path string, raw []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, writeErr := tmp.Write(raw)
	closeErr := tmp.Close()
	if writeErr != nil {
		_ = os.Remove(name)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(name)
		return closeErr
	}
	return os.Rename(name, path)
}
