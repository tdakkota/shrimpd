package shrimplication

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// Write is safe for concurrent use. Durable after WAL fsync.
//
// The WAL bytes are enqueued and the memtable updated under writeMu (so the
// snapshot+seal in flush captures exactly the enqueued set), but the fsync wait
// happens after releasing writeMu — letting concurrent writers share one fsync
// via group commit. The entry is in the memtable before the fsync confirms; on a
// (catastrophic, rare) fsync error the caller is told the write failed even
// though a later flush may still persist it, i.e. at-least-once semantics.
func (l *LSM) Write(_ context.Context, entries []shrimptypes.Entry) error {
	l.writeMu.Lock()
	commit := l.wal.Enqueue(entries)
	l.mem.Write(entries)
	full := l.mem.Len() >= flushThreshold
	l.writeMu.Unlock()

	if err := commit.Wait(); err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	if full {
		select {
		case l.flushSig <- struct{}{}:
		default: // already signaled
		}
	}
	return nil
}

// flush drains the memtable, writes an immutable part file atomically,
// commits metadata, and appends a PUT operation to the etcd log.
//
// The only lock held across the heavy I/O is flushMu (serializing flushes with
// each other). writeMu is taken only briefly to snapshot the memtable and seal
// the WAL together, so concurrent Write is not blocked on disk or etcd latency.
func (l *LSM) flush(ctx context.Context) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	// Atomically snapshot the memtable and seal the WAL: every entry in `entries`
	// is now in the sealed segments (<= sealedSeq), and writes arriving after this
	// point land in the fresh active segment + memtable.
	l.writeMu.Lock()
	entries := l.mem.Snapshot()
	if len(entries) == 0 {
		l.writeMu.Unlock()
		return nil
	}
	sealedSeq, sealErr := l.wal.Seal()
	l.writeMu.Unlock()
	if sealErr != nil {
		l.mem.Write(entries) // nothing was sealed; put entries back for a later retry
		return fmt.Errorf("seal wal: %w", sealErr)
	}
	slices.SortFunc(entries, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	slog.DebugContext(ctx, "creating new part", "id", id, "count", len(entries))
	blockHeaders, err := shrimpblock.WritePartV2(path, entries)
	if err != nil {
		l.restoreAfterFailedFlush(entries) // keep sealed segment as the durable copy
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
		l.restoreAfterFailedFlush(entries)
		return fmt.Errorf("write meta: %w", err)
	}

	if _, err := l.reg.AppendLog(ctx, OpPut, meta, nil); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaPath)
		l.restoreAfterFailedFlush(entries)
		return fmt.Errorf("append log: %w", err)
	}

	// Entries are now durable in the part file and the etcd log: the sealed WAL
	// segments are redundant and can be deleted.
	if err := l.wal.Discard(sealedSeq); err != nil {
		slog.WarnContext(ctx, "wal discard failed", "error", err)
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

// restoreAfterFailedFlush puts a failed flush's entries back into the memtable
// for a later retry. The sealed WAL segment is intentionally NOT discarded: it
// remains the durable copy of these entries until a subsequent flush succeeds
// and discards it. This preserves the invariant that the memtable equals the
// union of all not-yet-discarded WAL segments. Callers hold flushMu, so no
// concurrent flush can seal/discard segments underneath this.
func (l *LSM) restoreAfterFailedFlush(entries []shrimptypes.Entry) {
	l.mem.Write(entries)
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
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}
