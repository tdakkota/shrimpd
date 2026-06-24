package shrimplication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

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
		raw, _, err := fetchRemotePart(meta, remoteHTTP)
		if err != nil {
			return fmt.Errorf("bootstrap fetch %s: %w", id, err)
		}
		if err := writeRawPart(l.partPath(id), raw); err != nil {
			return err
		}
		meta.Compression = shrimpblock.DetectAlgo(raw)
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
			if b, err := l.readPartBlock(p); err == nil {
				if err := shrimpblock.WriteSidecar(l.sidecarPath(p.ID), shrimpblock.BuildSparse(b.Data, 32)); err != nil {
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
			block, err := l.readPartBlock(p)
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
		raw, _, err := fetchRemotePart(meta, remoteHTTP)
		if err != nil {
			return fmt.Errorf("bootstrap fetch failed: %w", err)
		}
		if err := writeRawPart(l.partPath(id), raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		meta.Compression = shrimpblock.DetectAlgo(raw)
		if err := WriteMeta(l.partMetaPath(id), meta); err != nil {
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
			block, err := l.readPartBlock(p)
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
		raw, block, err := fetchRemotePart(entry.Part, remoteHTTP)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeRawPart(path, raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		entry.Part.Compression = shrimpblock.DetectAlgo(raw)
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

		if err := l.idxEngine.ReindexPart(ctx, entry.Part, block); err != nil {
			slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
		}

	case OpMerge:
		slog.InfoContext(ctx, "replicating MERGE part", "index", entry.Index, "part_id", entry.Part.ID, "from", entry.NodeID)
		raw, block, err := fetchRemotePart(entry.Part, remoteHTTP)
		if err != nil {
			return fmt.Errorf("fetch remote part: %w", err)
		}

		path := l.partPath(entry.Part.ID)
		metaPath := l.partMetaPath(entry.Part.ID)

		if err := writeRawPart(path, raw); err != nil {
			return fmt.Errorf("write part: %w", err)
		}
		entry.Part.Compression = shrimpblock.DetectAlgo(raw)
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
		l.mu.Unlock()

		if err := l.idxEngine.ReindexPart(ctx, entry.Part, block); err != nil {
			slog.WarnContext(ctx, "failed to reindex replicated part", "id", entry.Part.ID, "error", err)
		}
	}

	return nil
}
