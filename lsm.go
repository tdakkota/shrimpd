// Package shrimpd provides a small LSM-backed distributed log store.
package shrimpd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	flushThreshold  = 100             // entries: eager flush when memtable exceeds this
	flushInterval   = 5 * time.Second // time-based flush regardless of size
	compactTrigger  = 4               // L0 parts before compaction kicks in
	compactInterval = 15 * time.Second
)

var remoteHTTP = &http.Client{Timeout: 10 * time.Second}

// LSM owns local writes, local parts, compaction, and distributed reads.
type LSM struct {
	nodeID  string
	addr    string
	dataDir string

	mem      *MemTable
	wal      *WAL
	reg      *Registry
	flushSig chan struct{} // buffered(1): signal from Write when threshold crossed

	mu    sync.RWMutex
	parts []PartMeta // this node's parts, kept in sync with etcd
}

// NewLSM creates an LSM instance and replays unflushed entries from the WAL.
func NewLSM(nodeID, addr, dataDir string, wal *WAL, reg *Registry) (*LSM, error) {
	l := &LSM{
		nodeID:   nodeID,
		addr:     addr,
		dataDir:  dataDir,
		mem:      &MemTable{},
		wal:      wal,
		reg:      reg,
		flushSig: make(chan struct{}, 1),
	}
	// Replay WAL to recover any entries not yet flushed to a part.
	entries, err := wal.Recover()
	if err != nil {
		return nil, fmt.Errorf("wal recover: %w", err)
	}
	if len(entries) > 0 {
		slog.Info("recovered entries from wal", "count", len(entries))
		l.mem.Write(entries)
	}
	return l, nil
}

// Write is safe for concurrent use. Durable after WAL fsync.
func (l *LSM) Write(_ context.Context, entries []Entry) error {
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

// Run registers the node, loads existing parts, then drives the flush/compact loop.
// Returns when ctx is canceled (after a final flush attempt).
func (l *LSM) Run(ctx context.Context) error {
	if err := l.startup(ctx); err != nil {
		return err
	}

	flushTick := time.NewTicker(flushInterval)
	compactTick := time.NewTicker(compactInterval)
	defer flushTick.Stop()
	defer compactTick.Stop()

	for {
		select {
		case <-ctx.Done():
			if l.mem.Len() > 0 {
				_ = l.flush(context.Background())
			}
			return ctx.Err()
		case <-l.flushSig:
			if err := l.flush(ctx); err != nil {
				slog.ErrorContext(ctx, "flush failed", "error", err)
			}
		case <-flushTick.C:
			if l.mem.Len() > 0 {
				if err := l.flush(ctx); err != nil {
					slog.ErrorContext(ctx, "flush failed", "error", err)
				}
			}
		case <-compactTick.C:
			if err := l.compact(ctx, false); err != nil {
				slog.ErrorContext(ctx, "compact failed", "error", err)
			}
		}
	}
}

func (l *LSM) startup(ctx context.Context) error {
	if err := l.reg.RegisterNode(ctx, l.addr); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	// Reload this node's parts from etcd in case we restarted.
	parts, err := l.reg.ListParts(ctx)
	if err != nil {
		return fmt.Errorf("list parts: %w", err)
	}
	l.mu.Lock()
	for _, p := range parts {
		if p.NodeID == l.nodeID {
			l.parts = append(l.parts, p)
		}
	}
	l.mu.Unlock()
	slog.InfoContext(ctx, "loaded local parts from etcd", "count", len(l.parts))
	return nil
}

// flush drains the memtable, writes an immutable part file atomically via
// temp-file + rename, then commits the PartMeta to etcd. WAL is truncated
// only after the etcd commit so recovery is always possible.
func (l *LSM) flush(ctx context.Context) error {
	entries := l.mem.Snapshot()
	if len(entries) == 0 {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	slog.DebugContext(ctx, "creating new part", "id", id, "count", len(entries))
	if err := writeBlock(path, Block{Data: entries}); err != nil {
		l.mem.Write(entries) // restore on failure so next flush retries
		return fmt.Errorf("write block: %w", err)
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        0,
		MinTimestamp: entries[0].Timestamp,
		MaxTimestamp: entries[len(entries)-1].Timestamp,
		Count:        len(entries),
		Addr:         l.addr,
	}
	if err := l.reg.PutPart(ctx, meta); err != nil {
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.WarnContext(ctx, "remove uncommitted part failed", "id", id, "error", removeErr)
		}
		l.mem.Write(entries)
		return fmt.Errorf("put part: %w", err)
	}

	// Safe to truncate WAL: entries are now durable in the part file and etcd.
	if err := l.wal.Rotate(); err != nil {
		slog.WarnContext(ctx, "wal rotate failed", "error", err)
	}

	l.mu.Lock()
	l.parts = append(l.parts, meta)
	l.mu.Unlock()

	slog.InfoContext(ctx, "flushed part", "id", id, "level", 0, "count", meta.Count, "min_timestamp", meta.MinTimestamp, "max_timestamp", meta.MaxTimestamp)
	return nil
}

// compact merges all L0 parts for this node into a single L1 part.
// Uses SwapParts for an atomic etcd transition so no reader sees a gap.
func (l *LSM) compact(ctx context.Context, force bool) error {
	l.mu.RLock()
	var l0 []PartMeta
	for _, p := range l.parts {
		if p.Level == 0 {
			l0 = append(l0, p)
		}
	}
	l.mu.RUnlock()

	if !force && len(l0) < compactTrigger {
		return nil
	}
	if len(l0) == 0 {
		if force {
			slog.DebugContext(ctx, "compaction skipped: no L0 parts to compact")
		}
		return nil
	}

	var merged []Entry
	for _, meta := range l0 {
		b, err := l.readLocalPart(meta.ID)
		if err != nil {
			return fmt.Errorf("read %s: %w", meta.ID, err)
		}
		merged = append(merged, b.Data...)
	}
	if len(merged) == 0 {
		if force {
			slog.DebugContext(ctx, "compaction skipped: no data in L0 parts")
		}
		return nil
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Timestamp < merged[j].Timestamp })

	id := newPartID(l.nodeID)
	if err := writeBlock(l.partPath(id), Block{Data: merged}); err != nil {
		return err
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        1,
		MinTimestamp: merged[0].Timestamp,
		MaxTimestamp: merged[len(merged)-1].Timestamp,
		Count:        len(merged),
		Addr:         l.addr,
	}
	oldIDs := make([]string, len(l0))
	for i, p := range l0 {
		oldIDs[i] = p.ID
	}
	slog.DebugContext(ctx, "compacting parts", "old_ids", oldIDs, "new_id", id, "count", len(merged))
	if err := l.reg.SwapParts(ctx, meta, oldIDs); err != nil {
		if removeErr := os.Remove(l.partPath(id)); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.WarnContext(ctx, "remove uncommitted compacted part failed", "id", id, "error", removeErr)
		}
		return fmt.Errorf("swap parts: %w", err)
	}
	for _, p := range l0 {
		if err := os.Remove(l.partPath(p.ID)); err != nil && !os.IsNotExist(err) {
			slog.WarnContext(ctx, "remove old part failed", "id", p.ID, "error", err)
		}
	}

	oldSet := make(map[string]bool, len(l0))
	for _, p := range l0 {
		oldSet[p.ID] = true
	}
	l.mu.Lock()
	next := make([]PartMeta, 0, len(l.parts))
	for _, p := range l.parts {
		if !oldSet[p.ID] {
			next = append(next, p)
		}
	}
	next = append(next, meta)
	l.parts = next
	l.mu.Unlock()

	slog.InfoContext(ctx, "compacted parts", "level0_count", len(l0), "id", id, "level", 1, "count", len(merged))
	return nil
}

// Query returns all entries in [from, to] across all nodes (via etcd part list),
// including the local memtable. Remote parts are fetched via HTTP.
func (l *LSM) Query(ctx context.Context, from, to int64) ([]Entry, error) {
	allParts, err := l.reg.ListParts(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]Entry, 0)
	for _, meta := range allParts {
		if !meta.overlaps(from, to) {
			continue
		}
		var block Block
		if meta.NodeID == l.nodeID {
			block, err = l.readLocalPart(meta.ID)
		} else {
			block, err = fetchRemotePart(meta)
		}
		if err != nil {
			slog.WarnContext(ctx, "skip part", "id", meta.ID, "error", err)
			continue
		}
		for _, e := range block.Data {
			if e.Timestamp >= from && e.Timestamp <= to {
				result = append(result, e)
			}
		}
	}

	// Include memtable (not yet flushed to any part).
	for _, e := range l.mem.All() {
		if e.Timestamp >= from && e.Timestamp <= to {
			result = append(result, e)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

// AllParts returns the global part list from etcd.
func (l *LSM) AllParts(ctx context.Context) ([]PartMeta, error) {
	return l.reg.ListParts(ctx)
}

// ServeLocalPart streams the raw part file to w. Used by /part/{id}.
func (l *LSM) ServeLocalPart(id string, w io.Writer) error {
	f, err := os.Open(l.partPath(id))
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(w, f)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (l *LSM) partPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".json")
}

func (l *LSM) readLocalPart(id string) (Block, error) {
	f, err := os.Open(l.partPath(id))
	if err != nil {
		return Block{}, err
	}
	var b Block
	decodeErr := json.NewDecoder(f).Decode(&b)
	closeErr := f.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	return b, closeErr
}

// writeBlock writes b to path atomically via a temp-file + rename.
func writeBlock(path string, b Block) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(b); err != nil {
		if closeErr := tmp.Close(); closeErr != nil {
			slog.Warn("close temp part after encode failure", "path", name, "error", closeErr)
		}
		if removeErr := os.Remove(name); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.Warn("remove temp part after encode failure", "path", name, "error", removeErr)
		}
		return err
	}
	if err := tmp.Sync(); err != nil {
		if closeErr := tmp.Close(); closeErr != nil {
			slog.Warn("close temp part after sync failure", "path", name, "error", closeErr)
		}
		if removeErr := os.Remove(name); removeErr != nil && !os.IsNotExist(removeErr) {
			slog.Warn("remove temp part after sync failure", "path", name, "error", removeErr)
		}
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func fetchRemotePart(meta PartMeta) (Block, error) {
	resp, err := remoteHTTP.Get("http://" + meta.Addr + "/part/" + meta.ID)
	if err != nil {
		return Block{}, err
	}
	if resp.StatusCode != http.StatusOK {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("close remote part response", "id", meta.ID, "error", err)
		}
		return Block{}, fmt.Errorf("remote %s: HTTP %d", meta.ID, resp.StatusCode)
	}
	var b Block
	decodeErr := json.NewDecoder(resp.Body).Decode(&b)
	closeErr := resp.Body.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	return b, closeErr
}
