package shrimplication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/oteldb/shrimpd/internal/shrimpblock"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

const logReplicationPageLimit = 1024

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
	var loaded []shrimptypes.PartMeta
	for id, meta := range snap.Parts {
		metaPath := l.partMetaPath(id)
		if _, err := os.Stat(metaPath); err == nil {
			// Prefer disk meta — it retains tokens stripped from etcd.
			if diskMeta, err := ReadMeta(metaPath); err == nil {
				loaded = append(loaded, diskMeta)
			} else {
				loaded = append(loaded, meta)
			}
			continue
		}
		raw, err := fetchRemotePart(ctx, meta, nil, remoteHTTP)
		if err != nil {
			slog.WarnContext(ctx, "bootstrap: peer unreachable, will retry", "id", id, "error", err)
			l.mu.Lock()
			l.pendingParts[id] = meta
			l.mu.Unlock()
			continue
		}
		if err := writeRawPart(l.partPath(id), raw); err != nil {
			return err
		}
		meta.Compression = shrimpblock.DetectAlgo(raw)
		if err := rebuildPartTokens(l.partPath(id), &meta); err != nil {
			_ = os.Remove(l.partPath(id))
			return fmt.Errorf("rebuild tokens: %w", err)
		}
		if err := WriteMeta(l.partMetaPath(id), meta); err != nil {
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
		if _, err := shrimpblock.ReadSidecar(l.sidecarPath(p.ID)); os.IsNotExist(err) {
			if pf, err := l.partMgr.Get(p.ID, p); err == nil && pf != nil {
				if err := shrimpblock.WriteSidecar(l.sidecarPath(p.ID), shrimpblock.BuildSparseFromPart(pf, 32)); err != nil {
					slog.WarnContext(ctx, "repair sidecar failed", "id", p.ID, "error", err)
				}
			} else if err != nil {
				slog.WarnContext(ctx, "repair sidecar: open part failed", "id", p.ID, "error", err)
			}
		}
	}

	// Reconcile index coverage for all loaded parts.
	for _, p := range l.parts {
		l.idxEngine.mu.RLock()
		_, covered := l.idxEngine.covered[p.ID]
		l.idxEngine.mu.RUnlock()
		if !covered {
			pf, err := l.partMgr.Get(p.ID, p)
			if err != nil || pf == nil {
				slog.WarnContext(ctx, "startup index reconciliation: open part failed", "id", p.ID, "error", err)
				continue
			}
			if err := l.idxEngine.ReindexPartFile(ctx, p, pf); err != nil {
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

func rebuildPartTokens(path string, meta *shrimptypes.PartMeta) error {
	pf, err := shrimpblock.OpenPartV2(path, *meta)
	if err != nil {
		return err
	}
	if pf == nil {
		return fmt.Errorf("part not found: %s", meta.ID)
	}
	defer func() { _ = pf.Close() }()

	meta.Tokens, meta.TokensTruncated = shrimpblock.BuildTokenSetFromPart(pf)
	meta.BlockCount = len(pf.Headers)
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
			// Resolve live peers once per tick; used by both pending retry and log
			// entry replication to avoid an etcd round-trip per log entry.
			peers, _ := l.reg.GetLivePeerAddrs(ctx, l.nodeID)

			l.retryPendingParts(ctx, peers)

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

			for {
				entries, err := l.reg.GetLogs(ctx, pointer+1, logReplicationPageLimit)
				if err != nil {
					slog.WarnContext(ctx, "failed to get logs from etcd", "error", err)
					break
				}
				if len(entries) == 0 {
					break
				}

				appliedAll := true
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
						appliedAll = false
						break
					}

					if err := l.applyLogEntry(ctx, entry, peers); err != nil {
						slog.ErrorContext(ctx, "failed to apply log entry", "index", entry.Index, "op", entry.Op, "error", err)
						appliedAll = false
						break // Retry from the same pointer next time
					}

					pointer = entry.Index
					if err := l.reg.SetQueuePointer(ctx, pointer); err != nil {
						slog.WarnContext(ctx, "failed to save queue pointer", "pointer", pointer, "error", err)
					}
				}
				if !appliedAll || len(entries) < logReplicationPageLimit {
					break
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
	var loaded []shrimptypes.PartMeta
	for id, meta := range snap.Parts {
		metaPath := l.partMetaPath(id)
		if _, err := os.Stat(metaPath); err == nil {
			// Prefer disk meta — it retains tokens stripped from etcd.
			if diskMeta, err := ReadMeta(metaPath); err == nil {
				loaded = append(loaded, diskMeta)
			} else {
				loaded = append(loaded, meta)
			}
			continue
		}
		raw, err := fetchRemotePart(ctx, meta, nil, remoteHTTP)
		if err != nil {
			slog.WarnContext(ctx, "bootstrap: peer unreachable, will retry", "id", id, "error", err)
			l.mu.Lock()
			l.pendingParts[id] = meta
			l.mu.Unlock()
			continue
		}
		if err := writeRawPart(l.partPath(id), raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		meta.Compression = shrimpblock.DetectAlgo(raw)
		if err := rebuildPartTokens(l.partPath(id), &meta); err != nil {
			_ = os.Remove(l.partPath(id))
			return fmt.Errorf("rebuild tokens: %w", err)
		}
		if err := WriteMeta(l.partMetaPath(id), meta); err != nil {
			_ = os.Remove(l.partPath(id))
			return fmt.Errorf("write meta: %w", err)
		}
		loaded = append(loaded, meta)
	}
	l.mu.Lock()
	l.parts = loaded
	// Prune pending parts that are no longer in the snapshot (merged/GC'd).
	for id := range l.pendingParts {
		if _, inSnap := snap.Parts[id]; !inSnap {
			delete(l.pendingParts, id)
			delete(l.pendingAttempts, id)
		}
	}
	l.mu.Unlock()

	// Reconcile index coverage for all loaded parts.
	for _, p := range loaded {
		l.idxEngine.mu.RLock()
		_, covered := l.idxEngine.covered[p.ID]
		l.idxEngine.mu.RUnlock()
		if !covered {
			pf, err := l.partMgr.Get(p.ID, p)
			if err != nil || pf == nil {
				slog.WarnContext(ctx, "bootstrap index reconciliation: open part failed", "id", p.ID, "error", err)
				continue
			}
			if err := l.idxEngine.ReindexPartFile(ctx, p, pf); err != nil {
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

func (l *LSM) applyLogEntry(ctx context.Context, entry LogEntry, peers []string) error {
	if entry.NodeID == l.nodeID {
		slog.DebugContext(ctx, "skip own log entry", "index", entry.Index, "op", entry.Op)
		return nil
	}

	switch entry.Op {
	case OpPut:
		slog.InfoContext(ctx, "replicating PUT part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		raw, err := fetchRemotePart(ctx, entry.Part, peers, remoteHTTP)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeRawPart(path, raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		entry.Part.Compression = shrimpblock.DetectAlgo(raw)
		if err := rebuildPartTokens(path, &entry.Part); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("rebuild tokens: %w", err)
		}
		if err := WriteMeta(metaPath, entry.Part); err != nil {
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

		if pf, err := l.partMgr.Get(entry.Part.ID, entry.Part); err == nil && pf != nil {
			if err := l.idxEngine.ReindexPartFile(ctx, entry.Part, pf); err != nil {
				slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
			}
		}

	case OpMerge:
		slog.InfoContext(ctx, "replicating MERGE part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		raw, err := fetchRemotePart(ctx, entry.Part, peers, remoteHTTP)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeRawPart(path, raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		entry.Part.Compression = shrimpblock.DetectAlgo(raw)
		if err := rebuildPartTokens(path, &entry.Part); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("rebuild tokens: %w", err)
		}
		if err := WriteMeta(metaPath, entry.Part); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write meta: %w", err)
		}

		oldSet := make(map[string]bool, len(entry.OldParts))
		for _, id := range entry.OldParts {
			oldSet[id] = true
			// Deletion deferred to runGC with safety cutoff
		}

		l.mu.Lock()
		next := make([]shrimptypes.PartMeta, 0, len(l.parts))
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
		// The merged part supersedes all old parts; a pending download of any
		// old part would resurrect a stale entry and produce duplicate results.
		for _, id := range entry.OldParts {
			delete(l.pendingParts, id)
			delete(l.pendingAttempts, id)
		}
		l.mu.Unlock()

		if pf, err := l.partMgr.Get(entry.Part.ID, entry.Part); err == nil && pf != nil {
			if err := l.idxEngine.ReindexPartFile(ctx, entry.Part, pf); err != nil {
				slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
			}
		}
	}

	return nil
}

const (
	pendingBackoffBase = 5 * time.Second
	pendingBackoffMax  = 5 * time.Minute
)

type pendingAttempt struct {
	count  int
	nextAt time.Time
}

func (pa pendingAttempt) delay() time.Duration {
	if pa.count == 0 {
		return 0
	}
	// 5s, 10s, 20s, 40s, 80s, 160s, 320s → capped at 300s
	d := pendingBackoffBase << min(pa.count-1, 6)
	return min(d, pendingBackoffMax)
}

// bumpPendingBackoff increments the retry counter for id and schedules the next attempt.
func (l *LSM) bumpPendingBackoff(id string, now time.Time) {
	l.mu.Lock()
	pa := l.pendingAttempts[id]
	pa.count++
	pa.nextAt = now.Add(pa.delay())
	l.pendingAttempts[id] = pa
	l.mu.Unlock()
}

// retryPendingParts attempts to fetch parts that were unreachable at bootstrap,
// using exponential backoff per part (5s → 5m cap).
// peers is the current live-peer address list, resolved once per tick by the caller.
func (l *LSM) retryPendingParts(ctx context.Context, peers []string) {
	if ctx.Err() != nil {
		return
	}

	now := time.Now()

	l.mu.RLock()
	if len(l.pendingParts) == 0 {
		l.mu.RUnlock()
		return
	}
	type work struct {
		id   string
		meta shrimptypes.PartMeta
	}
	var due []work
	for id, meta := range l.pendingParts {
		if pa := l.pendingAttempts[id]; now.Before(pa.nextAt) {
			continue
		}
		due = append(due, work{id, meta})
	}
	l.mu.RUnlock()

	for _, w := range due {
		if ctx.Err() != nil {
			return
		}

		raw, err := fetchRemotePart(ctx, w.meta, peers, remoteHTTP)
		if err != nil {
			l.bumpPendingBackoff(w.id, now)
			l.mu.RLock()
			next := l.pendingAttempts[w.id].nextAt
			l.mu.RUnlock()
			slog.DebugContext(ctx, "pending part still unreachable", "id", w.id, "next_retry", next.Round(time.Second), "error", err)
			continue
		}

		if err := writeRawPart(l.partPath(w.id), raw); err != nil {
			slog.WarnContext(ctx, "pending part: write failed", "id", w.id, "error", err)
			l.bumpPendingBackoff(w.id, now)
			continue
		}
		w.meta.Compression = shrimpblock.DetectAlgo(raw)
		if err := rebuildPartTokens(l.partPath(w.id), &w.meta); err != nil {
			_ = os.Remove(l.partPath(w.id))
			slog.WarnContext(ctx, "pending part: rebuild tokens failed", "id", w.id, "error", err)
			l.bumpPendingBackoff(w.id, now)
			continue
		}
		if err := WriteMeta(l.partMetaPath(w.id), w.meta); err != nil {
			_ = os.Remove(l.partPath(w.id))
			slog.WarnContext(ctx, "pending part: write meta failed", "id", w.id, "error", err)
			l.bumpPendingBackoff(w.id, now)
			continue
		}

		l.mu.Lock()
		l.parts = append(l.parts, w.meta)
		delete(l.pendingParts, w.id)
		delete(l.pendingAttempts, w.id)
		l.mu.Unlock()

		slog.InfoContext(ctx, "fetched previously pending part", "id", w.id)

		if pf, err := l.partMgr.Get(w.id, w.meta); err == nil && pf != nil {
			if err := l.idxEngine.ReindexPartFile(ctx, w.meta, pf); err != nil {
				slog.WarnContext(ctx, "pending part: reindex failed", "id", w.id, "error", err)
			}
		}
	}
}
