// Package shrimpd provides a small LSM-backed distributed log store.
package shrimpd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	parts []PartMeta // all parts replicated locally, kept in sync with etcd log

	idxEngine *IndexEngine // Separate Index Engine
}

// NewLSM creates an LSM instance and replays unflushed entries from the WAL.
func NewLSM(nodeID, addr, dataDir string, wal *WAL, reg *Registry) (*LSM, error) {
	idx, err := NewIndexEngine(nodeID, dataDir)
	if err != nil {
		return nil, fmt.Errorf("new index engine: %w", err)
	}

	l := &LSM{
		nodeID:    nodeID,
		addr:      addr,
		dataDir:   dataDir,
		mem:       &MemTable{},
		wal:       wal,
		reg:       reg,
		flushSig:  make(chan struct{}, 1),
		idxEngine: idx,
	}
	// Replay WAL to recover any entries not yet flushed to a part.
	entries, err := wal.Recover()
	if err != nil {
		_ = idx.Close()
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

	// Start background replication loop.
	go func() {
		if err := l.replicationLoop(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "replication loop failed", "error", err)
		}
	}()

	// Start background garbage collection loop.
	go l.gcLoop(ctx)

	// Start background log cleanup loop (truncates old log entries).
	go l.reg.LogCleanupLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			if l.mem.Len() > 0 {
				_ = l.flush(context.Background())
			}
			_ = l.idxEngine.Close()
			return ctx.Err()
		case <-l.flushSig:
			if err := l.flush(ctx); err != nil {
				slog.ErrorContext(ctx, "flush failed", "error", err)
			}
		case <-l.idxEngine.flushSig:
			if err := l.idxEngine.Flush(ctx); err != nil {
				slog.ErrorContext(ctx, "index flush failed", "error", err)
			}
		case <-flushTick.C:
			if l.mem.Len() > 0 {
				if err := l.flush(ctx); err != nil {
					slog.ErrorContext(ctx, "flush failed", "error", err)
				}
			}
			if l.idxEngine.mem.Len() > 0 {
				if err := l.idxEngine.Flush(ctx); err != nil {
					slog.ErrorContext(ctx, "index flush failed", "error", err)
				}
			}
		case <-compactTick.C:
			if err := l.compact(ctx, false); err != nil {
				slog.ErrorContext(ctx, "compact failed", "error", err)
			}
			// Trigger index compaction with active data part IDs
			l.mu.RLock()
			activeIDs := make(map[string]struct{})
			for _, p := range l.parts {
				activeIDs[p.ID] = struct{}{}
			}
			l.mu.RUnlock()
			if err := l.idxEngine.Compact(ctx, activeIDs); err != nil {
				slog.ErrorContext(ctx, "index compaction failed", "error", err)
			}
		}
	}
}

func (l *LSM) startup(ctx context.Context) error {
	if err := l.reg.RegisterNode(ctx, l.addr); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	snap, err := l.reg.GetBootstrapSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("get bootstrap snapshot: %w", err)
	}

	// Download any missing part files from peers before advertising a high queue pointer.
	// This ensures we have the physical data for snap.Parts prior to advancing our pointer
	// (so LogCleanup won't drop logs we might still conceptually need during transition).
	var loaded []PartMeta
	for id, meta := range snap.Parts {
		if _, err := os.Stat(l.partMetaPath(id)); err == nil {
			loaded = append(loaded, meta)
			continue
		}
		block, err := fetchRemotePart(meta)
		if err != nil {
			return fmt.Errorf("bootstrap fetch %s: %w", id, err)
		}
		if err := writeBlock(l.partPath(id), block, compressionZstd); err != nil {
			return err
		}
		meta.Compression = compressionZstd
		if err := writeMeta(l.partMetaPath(id), meta); err != nil {
			_ = os.Remove(l.partPath(id))
			return err
		}
		loaded = append(loaded, meta)
	}

	l.mu.Lock()
	l.parts = loaded
	l.mu.Unlock()

	if snap.LogIndex > 0 {
		if err := l.reg.SetQueuePointer(ctx, snap.LogIndex); err != nil {
			return fmt.Errorf("set pointer: %w", err)
		}
	}

	// Repair missing local sidecars (L1 sparse index).
	for _, p := range l.parts {
		if _, err := readSidecar(l.sidecarPath(p.ID)); os.IsNotExist(err) {
			if b, err := l.readLocalPart(p.ID); err == nil {
				if err := writeSidecar(l.sidecarPath(p.ID), buildSparse(b.Data, 32)); err != nil {
					slog.WarnContext(ctx, "repair sidecar failed", "id", p.ID, "error", err)
				}
			} else {
				slog.WarnContext(ctx, "repair sidecar: read part failed", "id", p.ID, "error", err)
			}
		}
	}

	// Reconcile index coverage for all loaded parts.
	for _, p := range l.parts {
		l.idxEngine.mu.RLock()
		_, covered := l.idxEngine.covered[p.ID]
		l.idxEngine.mu.RUnlock()
		if !covered {
			block, err := l.readLocalPart(p.ID)
			if err != nil {
				slog.WarnContext(ctx, "startup index reconciliation: read part failed", "id", p.ID, "error", err)
				continue
			}
			if err := l.idxEngine.ReindexPart(ctx, p, block); err != nil {
				slog.WarnContext(ctx, "startup index reconciliation: reindex failed", "id", p.ID, "error", err)
			}
		}
	}
	if l.idxEngine.mem.Len() > 0 {
		if err := l.idxEngine.Flush(ctx); err != nil {
			slog.WarnContext(ctx, "startup index reconciliation: flush failed", "error", err)
		}
	}

	slog.InfoContext(ctx, "bootstrapped from etcd parts", "log_index", snap.LogIndex, "count", len(loaded))
	return nil
}

// replicationLoop polls etcd for global mutation log entries and applies them.
func (l *LSM) replicationLoop(ctx context.Context) error {
	pointer, err := l.reg.GetQueuePointer(ctx)
	if err != nil {
		return fmt.Errorf("get queue pointer: %w", err)
	}
	slog.InfoContext(ctx, "started replication loop", "pointer", pointer)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check for log gap after possible truncation
			if pointer > 0 {
				exists, err := l.reg.logEntryExists(ctx, pointer+1)
				if err == nil && !exists {
					// gap detected: bootstrap
					if err := l.bootstrapFromParts(ctx); err != nil {
						slog.WarnContext(ctx, "bootstrap from parts after gap failed", "error", err)
					} else if p, err := l.reg.GetQueuePointer(ctx); err == nil {
						pointer = p
					} else {
						slog.WarnContext(ctx, "failed to update queue pointer", "error", err)
					}
				}
			}

			entries, err := l.reg.GetLogs(ctx, pointer+1)
			if err != nil {
				slog.WarnContext(ctx, "failed to get logs from etcd", "error", err)
				continue
			}

			for _, entry := range entries {
				if entry.Index <= pointer {
					continue
				}

				if entry.Index > pointer+1 {
					// Log gap detected (e.g. after truncation while offline). Bootstrap from parts.
					slog.WarnContext(ctx, "log gap detected in replication", "expected", pointer+1, "got", entry.Index)
					if err := l.bootstrapFromParts(ctx); err != nil {
						slog.WarnContext(ctx, "bootstrap from parts after gap failed", "error", err)
					} else if p, err := l.reg.GetQueuePointer(ctx); err == nil {
						pointer = p
					} else {
						slog.WarnContext(ctx, "failed to update queue pointer", "error", err)
					}
					break
				}

				if err := l.applyLogEntry(ctx, entry); err != nil {
					slog.ErrorContext(ctx, "failed to apply log entry", "index", entry.Index, "op", entry.Op, "error", err)
					break // Retry from the same pointer next time
				}

				pointer = entry.Index
				if err := l.reg.SetQueuePointer(ctx, pointer); err != nil {
					slog.WarnContext(ctx, "failed to save queue pointer", "pointer", pointer, "error", err)
				}
			}
		}
	}
}

func (l *LSM) bootstrapFromParts(ctx context.Context) error {
	snap, err := l.reg.GetBootstrapSnapshot(ctx)
	if err != nil {
		return err
	}
	if snap.LogIndex > 0 {
		if err := l.reg.SetQueuePointer(ctx, snap.LogIndex); err != nil {
			return fmt.Errorf("set queue pointer: %w", err)
		}
	}
	var loaded []PartMeta
	for id, meta := range snap.Parts {
		if _, err := os.Stat(l.partMetaPath(id)); err == nil {
			loaded = append(loaded, meta)
			continue
		}
		block, err := fetchRemotePart(meta)
		if err != nil {
			return fmt.Errorf("bootstrap fetch failed: %w", err)
		}
		if err := writeBlock(l.partPath(id), block, compressionZstd); err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		meta.Compression = compressionZstd
		if err := writeMeta(l.partMetaPath(id), meta); err != nil {
			_ = os.Remove(l.partPath(id))
			return fmt.Errorf("write meta: %w", err)
		}
		loaded = append(loaded, meta)
	}
	l.mu.Lock()
	l.parts = loaded
	l.mu.Unlock()

	// Reconcile index coverage for all loaded parts.
	for _, p := range loaded {
		l.idxEngine.mu.RLock()
		_, covered := l.idxEngine.covered[p.ID]
		l.idxEngine.mu.RUnlock()
		if !covered {
			block, err := l.readLocalPart(p.ID)
			if err != nil {
				slog.WarnContext(ctx, "bootstrap index reconciliation: read part failed", "id", p.ID, "error", err)
				continue
			}
			if err := l.idxEngine.ReindexPart(ctx, p, block); err != nil {
				slog.WarnContext(ctx, "bootstrap index reconciliation: reindex failed", "id", p.ID, "error", err)
			}
		}
	}
	if l.idxEngine.mem.Len() > 0 {
		if err := l.idxEngine.Flush(ctx); err != nil {
			slog.WarnContext(ctx, "bootstrap index reconciliation: flush failed", "error", err)
		}
	}
	return nil
}

func (l *LSM) applyLogEntry(ctx context.Context, entry LogEntry) error {
	if entry.NodeID == l.nodeID {
		slog.DebugContext(ctx, "skip own log entry", "index", entry.Index, "op", entry.Op)
		return nil
	}

	switch entry.Op {
	case OpPut:
		slog.InfoContext(ctx, "replicating PUT part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		block, err := fetchRemotePart(entry.Part)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeBlock(path, block, compressionZstd); err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		entry.Part.Compression = compressionZstd
		if err := writeMeta(metaPath, entry.Part); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write meta: %w", err)
		}

		l.mu.Lock()
		has := false
		for _, p := range l.parts {
			if p.ID == entry.Part.ID {
				has = true
				break
			}
		}
		if !has {
			l.parts = append(l.parts, entry.Part)
		}
		l.mu.Unlock()

		if err := l.idxEngine.ReindexPart(ctx, entry.Part, block); err != nil {
			slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
		}

	case OpMerge:
		slog.InfoContext(ctx, "replicating MERGE part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		block, err := fetchRemotePart(entry.Part)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeBlock(path, block, compressionZstd); err != nil {
			return fmt.Errorf("write block: %w", err)
		}
		entry.Part.Compression = compressionZstd
		if err := writeMeta(metaPath, entry.Part); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write meta: %w", err)
		}

		oldSet := make(map[string]bool, len(entry.OldParts))
		for _, id := range entry.OldParts {
			oldSet[id] = true
			// Deletion deferred to runGC with safety cutoff
		}

		l.mu.Lock()
		next := make([]PartMeta, 0, len(l.parts))
		has := false
		for _, p := range l.parts {
			if !oldSet[p.ID] {
				if p.ID == entry.Part.ID {
					has = true
				}
				next = append(next, p)
			}
		}
		if !has {
			next = append(next, entry.Part)
		}
		l.parts = next
		l.mu.Unlock()

		if err := l.idxEngine.ReindexPart(ctx, entry.Part, block); err != nil {
			slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
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
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	slog.DebugContext(ctx, "creating new part", "id", id, "count", len(entries))
	if err := writeBlock(path, Block{SourceReplica: l.nodeID, CreatedAt: time.Now().UnixNano(), Data: entries}, compressionZstd); err != nil {
		l.mem.Write(entries) // restore on failure so next flush retries
		return fmt.Errorf("write block: %w", err)
	}

	sparse := buildSparse(entries, 32)
	if err := writeSidecar(l.sidecarPath(id), sparse); err != nil {
		_ = os.Remove(path)
		l.mem.Write(entries)
		return fmt.Errorf("write sidecar: %w", err)
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        0,
		MinTimestamp: entries[0].Timestamp,
		MaxTimestamp: entries[len(entries)-1].Timestamp,
		Count:        len(entries),
		Addr:         l.addr,
		Tokens:       buildTokenSet(entries),
		Compression:  compressionZstd,
	}

	if err := writeMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(l.sidecarPath(id))
		l.mem.Write(entries)
		return fmt.Errorf("write meta: %w", err)
	}

	if _, err := l.reg.AppendLog(ctx, OpPut, meta, nil); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaPath)
		_ = os.Remove(l.sidecarPath(id))
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

	idxEntries := BuildIndexEntries(meta.ID, entries)
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

// compact merges all L0 parts for this node into a single L1 part.
// Emits a MERGE operation to the etcd log.
func (l *LSM) compact(ctx context.Context, force bool) error {
	l.mu.RLock()
	var l0 []PartMeta
	for _, p := range l.parts {
		if p.Level == 0 && p.NodeID == l.nodeID {
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

	oldIDs := make([]string, len(l0))
	for i, p := range l0 {
		oldIDs[i] = p.ID
	}

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	if err := writeBlock(path, Block{SourceReplica: l.nodeID, CreatedAt: time.Now().UnixNano(), SourceBlocks: oldIDs, Data: merged}, compressionZstd); err != nil {
		return err
	}

	sparse := buildSparse(merged, 32)
	if err := writeSidecar(l.sidecarPath(id), sparse); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write sidecar: %w", err)
	}

	meta := PartMeta{
		ID:           id,
		NodeID:       l.nodeID,
		Level:        1,
		MinTimestamp: merged[0].Timestamp,
		MaxTimestamp: merged[len(merged)-1].Timestamp,
		Count:        len(merged),
		Addr:         l.addr,
		Tokens:       buildTokenSet(merged),
		Compression:  compressionZstd,
	}

	if err := writeMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(l.sidecarPath(id))
		return fmt.Errorf("write meta: %w", err)
	}

	slog.DebugContext(ctx, "compacting parts", "old_ids", oldIDs, "new_id", id, "count", len(merged))

	if _, err := l.reg.AppendLog(ctx, OpMerge, meta, oldIDs); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaPath)
		_ = os.Remove(l.sidecarPath(id))
		return fmt.Errorf("append log: %w", err)
	}

	// Defer deletions to runGC (5m safety cutoff) to prevent 404s during peer replication.

	oldSet := make(map[string]bool, len(l0))
	for _, p := range l0 {
		oldSet[p.ID] = true
	}
	l.mu.Lock()
	next := make([]PartMeta, 0, len(l.parts))
	has := false
	for _, p := range l.parts {
		if !oldSet[p.ID] {
			if p.ID == meta.ID {
				has = true
			}
			next = append(next, p)
		}
	}
	if !has {
		next = append(next, meta)
	}
	l.parts = next
	l.mu.Unlock()

	idxEntries := BuildIndexEntries(meta.ID, merged)
	if err := l.idxEngine.Write(idxEntries); err != nil {
		slog.WarnContext(ctx, "failed to write index entries on compaction", "id", meta.ID, "error", err)
	} else {
		if err := l.idxEngine.MarkCovered([]string{meta.ID}); err != nil {
			slog.WarnContext(ctx, "failed to mark covered on compaction", "id", meta.ID, "error", err)
		}
	}

	slog.InfoContext(ctx, "compacted parts", "level0_count", len(l0), "id", id, "level", 1, "count", len(merged))
	return nil
}

// Query returns entries within the given timestamp range, optionally filtered by term.
func (l *LSM) Query(ctx context.Context, from, to int64, term string) ([]Entry, error) {
	l.mu.RLock()
	allParts := make([]PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()

	// Step 1: Filter data parts by timestamp range
	var timeParts []PartMeta
	for _, meta := range allParts {
		if meta.overlaps(from, to) {
			timeParts = append(timeParts, meta)
		}
	}

	// Step 2-4: Filter by index or fall back to old behavior
	useIndexFilter := false
	var indexedPartIDs map[string]struct{}
	if term != "" {
		matches, complete, err := l.idxEngine.Lookup(ctx, term, timeParts)
		if err != nil {
			slog.WarnContext(ctx, "index lookup failed, falling back to scanning", "error", err)
		} else if complete {
			useIndexFilter = true
			indexedPartIDs = matches
		}
	}

	result := make([]Entry, 0)
	for _, meta := range timeParts {
		if useIndexFilter {
			if _, matched := indexedPartIDs[meta.ID]; !matched {
				continue
			}
		} else {
			// Fallback: use legacy PartMeta.Tokens pruning if available
			if term != "" && !hasToken(meta.Tokens, term) {
				continue
			}
		}

		block, err := l.readLocalPart(meta.ID)
		if err != nil {
			slog.WarnContext(ctx, "skip part", "id", meta.ID, "error", err)
			continue
		}
		sparse, _ := readSidecar(l.sidecarPath(meta.ID))
		lo, hi := sparseRange(sparse, from, to)
		if lo < 0 {
			lo = 0
		}
		if hi > len(block.Data) {
			hi = len(block.Data)
		}
		for _, e := range block.Data[lo:hi] {
			if e.Timestamp >= from && e.Timestamp <= to &&
				(term == "" || strings.Contains(strings.ToLower(e.Data), strings.ToLower(term))) {
				result = append(result, e)
			}
		}
	}

	// Include memtable (not yet flushed to any part).
	for _, e := range l.mem.All() {
		if e.Timestamp >= from && e.Timestamp <= to &&
			(term == "" || strings.Contains(strings.ToLower(e.Data), strings.ToLower(term))) {
			result = append(result, e)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

// AllParts returns the copy of current memory parts list.
func (l *LSM) AllParts(_ context.Context) ([]PartMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copied := make([]PartMeta, len(l.parts))
	copy(copied, l.parts)
	return copied, nil
}

// ServeLocalPart streams the part file to w, used by /part/{id}.
// If the part is zstd-compressed on disk and the client advertises Accept-Encoding: zstd,
// the compressed bytes are streamed verbatim with Content-Encoding: zstd; otherwise the
// part is decompressed on the fly so legacy peers and humans get plain JSON.
func (l *LSM) ServeLocalPart(r *http.Request, w http.ResponseWriter) error {
	id := r.PathValue("id")
	f, err := os.Open(l.partPath(id)) // #nosec G304 -- trusted internal part path
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReaderSize(f, 512)
	head, err := br.Peek(4)
	if err != nil && err != io.EOF {
		return err
	}
	onDisk := detectAlgo(head)
	acceptZstd := strings.Contains(r.Header.Get("Accept-Encoding"), compressionZstd)

	if onDisk == compressionZstd && acceptZstd {
		w.Header().Set("Content-Encoding", compressionZstd)
		_, copyErr := io.Copy(w, br)
		return copyErr
	}

	if onDisk == compressionZstd {
		dec, err := newZstdDecompressReader(br)
		if err != nil {
			return err
		}
		defer func() { _ = dec.Close() }()
		_, copyErr := io.Copy(w, dec)
		return copyErr
	}

	_, copyErr := io.Copy(w, br)
	return copyErr
}

func (l *LSM) partPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".json")
}

func (l *LSM) partMetaPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".meta")
}

func (l *LSM) sidecarPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".sparse.json")
}

func (l *LSM) readLocalPart(id string) (Block, error) {
	f, err := os.Open(l.partPath(id)) // #nosec G304 -- trusted internal part path
	if err != nil {
		return Block{}, err
	}
	r, _, err := openBlockReader(f)
	if err != nil {
		_ = f.Close()
		return Block{}, err
	}
	var b Block
	decodeErr := json.NewDecoder(r).Decode(&b)
	rCloseErr := r.Close()
	fCloseErr := f.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	if rCloseErr != nil {
		return Block{}, rCloseErr
	}
	return b, fCloseErr
}

// writeBlock writes b to path atomically via a temp-file + rename.
func writeBlock(path string, b Block, algo string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	cw, err := newCompressingWriter(tmp, algo)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	encErr := json.NewEncoder(cw).Encode(b)
	closeErr := cw.Close()
	if encErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return encErr
	}
	if closeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return closeErr
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// writeMeta writes meta to path atomically via a temp-file + rename.
func writeMeta(path string, meta PartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-meta-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(meta); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func fetchRemotePart(meta PartMeta) (Block, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+meta.Addr+"/part/"+meta.ID, http.NoBody)
	if err != nil {
		return Block{}, err
	}
	req.Header.Set("Accept-Encoding", compressionZstd)
	resp, err := remoteHTTP.Do(req)
	if err != nil {
		return Block{}, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return Block{}, fmt.Errorf("remote %s: HTTP %d", meta.ID, resp.StatusCode)
	}
	r, _, err := openBlockReader(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return Block{}, err
	}
	var b Block
	decodeErr := json.NewDecoder(r).Decode(&b)
	rCloseErr := r.Close()
	bodyCloseErr := resp.Body.Close()
	if decodeErr != nil {
		return Block{}, decodeErr
	}
	if rCloseErr != nil {
		return Block{}, rCloseErr
	}
	return b, bodyCloseErr
}

func newPartID(nodeID string) string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), nodeID)
}

// gcLoop periodically fetches the materialized active parts from etcd (/lsm/parts/)
// and removes any local files that are not part of the active set. It also reconciles
// the in-memory l.parts list to match the global state.
func (l *LSM) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.runGC(ctx); err != nil {
				slog.WarnContext(ctx, "garbage collection failed", "error", err)
			}
		}
	}
}

func (l *LSM) runGC(ctx context.Context) error {
	active, err := l.reg.GetActiveParts(ctx)
	if err != nil {
		return fmt.Errorf("get active parts: %w", err)
	}

	// Clean up stale files on disk
	files, err := os.ReadDir(filepath.Join(l.dataDir, "parts"))
	if err != nil {
		return fmt.Errorf("read parts dir: %w", err)
	}

	cutoff := time.Now().Add(-5 * time.Minute)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		var id string
		switch {
		case strings.HasSuffix(name, ".sparse.json"):
			id = strings.TrimSuffix(name, ".sparse.json")
		default:
			ext := filepath.Ext(name)
			if ext != ".meta" && ext != ".json" {
				continue
			}
			id = name[:len(name)-len(ext)]
		}
		if _, ok := active[id]; !ok {
			info, err := f.Info()
			if err != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				path := filepath.Join(l.dataDir, "parts", name)
				if err := os.Remove(path); err == nil {
					slog.InfoContext(ctx, "gc removed stale part file", "file", name)
				}
			}
		}
	}

	// Safely reconcile local l.parts without dropping recently flushed parts
	l.mu.Lock()
	var reconciled []PartMeta
	for _, p := range l.parts {
		if _, ok := active[p.ID]; ok {
			reconciled = append(reconciled, p)
		} else {
			info, err := os.Stat(l.partMetaPath(p.ID))
			if err == nil && info.ModTime().After(cutoff) {
				reconciled = append(reconciled, p)
			}
		}
	}
	l.parts = reconciled
	l.mu.Unlock()

	return nil
}
