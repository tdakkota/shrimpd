package shrimplication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oteldb/shrimpd/internal/shrimpblock"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

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

	// Evict deleted parts from caches
	for id := range active {
		_ = id // parts still active, keep in cache
	}

	// Safely reconcile local l.parts without dropping recently flushed parts
	l.mu.Lock()
	var reconciled []shrimptypes.PartMeta
	for _, p := range l.parts {
		if _, ok := active[p.ID]; ok {
			reconciled = append(reconciled, p)
		} else {
			info, err := os.Stat(l.partMetaPath(p.ID))
			if err == nil && info.ModTime().After(cutoff) {
				reconciled = append(reconciled, p)
			} else {
				// Part is being evicted: clean up caches and close fd
				l.evictPart(p.ID)
			}
		}
	}
	l.parts = reconciled
	l.mu.Unlock()

	return nil
}

// evictPart cleans up cached resources for the given part ID.
func (l *LSM) evictPart(id string) {
	l.rowBlockCache.DeleteByFunc(func(k shrimptypes.RowCacheKey, _ *shrimpblock.BinBlock) bool {
		return k.PartID == id
	})
	l.partMgr.Release(id)
}
