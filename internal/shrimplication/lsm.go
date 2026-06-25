// Package shrimplication provides a small LSM-backed distributed log store.
package shrimplication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/maypok86/otter"

	"github.com/oteldb/shrimpd/internal/shrimpblock"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
	"github.com/oteldb/shrimpd/internal/shrimpwal"
)

const (
	flushThreshold  = 100             // entries: eager flush when memtable exceeds this
	flushInterval   = 5 * time.Second // time-based flush regardless of size
	compactTrigger  = 4               // parts per level before compaction kicks in
	compactInterval = 15 * time.Second
)

var remoteHTTP = &http.Client{Timeout: 10 * time.Second}

type registryAPI interface {
	RegisterNode(ctx context.Context, addr string) error
	AppendLog(ctx context.Context, op LogOp, part shrimptypes.PartMeta, oldParts []string) (int64, error)
	GetLogs(ctx context.Context, fromIndex int64) ([]LogEntry, error)
	GetActiveParts(ctx context.Context) (map[string]shrimptypes.PartMeta, error)
	GetBootstrapSnapshot(ctx context.Context) (BootstrapSnapshot, error)
	logEntryExists(ctx context.Context, idx int64) (bool, error)
	LogCleanupLoop(ctx context.Context)
	GetQueuePointer(ctx context.Context) (int64, error)
	SetQueuePointer(ctx context.Context, index int64) error
	GetLivePeerAddrs(ctx context.Context, excludeID string) ([]string, error)
}

// LSM owns local writes, local parts, compaction, and distributed reads.
type LSM struct {
	nodeID  string
	addr    string
	dataDir string

	// writeMu guards the pairing of mem.Write+wal.Enqueue (in Write) against
	// mem snapshot+wal.Seal (in flush), so the sealed WAL segment's contents
	// exactly equal the flushed snapshot. Held only across that brief boundary,
	// never across the heavy flush I/O.
	writeMu sync.Mutex
	// flushMu serializes whole flushes against each other (both the Run loop and
	// the HTTP-triggered Flush call flush). Serial flushes are required for the
	// WAL seal/discard invariant. It is separate from mu because mu is held only
	// briefly here (see below), so it cannot itself serialize flushes.
	flushMu sync.Mutex
	mem     *MemTable      // own internal lock; see writeMu for cross-field invariants
	wal     *shrimpwal.WAL // own internal lock; see writeMu
	reg     registryAPI

	flushSig chan struct{} // buffered(1): signal from Write when threshold crossed

	// mu guards parts (briefly, when a flush/compaction/replication publishes a
	// new part). Unlike the index engine, it is NOT held across flush I/O: the
	// query path reads the mem+parts union and tolerates a part appearing slightly
	// later, so there is no in-flight-visibility hazard to protect against.
	mu              sync.RWMutex
	parts           []shrimptypes.PartMeta          // all parts replicated locally, kept in sync with etcd log
	pendingParts    map[string]shrimptypes.PartMeta // parts whose peer was unreachable at bootstrap; retried periodically
	pendingAttempts map[string]pendingAttempt       // per-part retry state

	idxEngine *IndexEngine // Separate Index Engine

	rowBlockCache otter.Cache[shrimptypes.RowCacheKey, *shrimpblock.BinBlock] // keyed by (partID, block index)
	partMgr       *PartManager                                                // manages open V2 part files
}

// Close releases resources held by the LSM without flushing. It is intended for
// tests and benchmarks that drive the LSM directly without calling Run.
func (l *LSM) Close() error {
	l.partMgr.Close()
	l.rowBlockCache.Close()
	return l.idxEngine.Close()
}

// SetParts replaces the in-memory part list.
// It is intended for tests and benchmarks that bypass startup/bootstrap.
func (l *LSM) SetParts(parts []shrimptypes.PartMeta) {
	l.mu.Lock()
	l.parts = append([]shrimptypes.PartMeta(nil), parts...)
	l.mu.Unlock()
}

// NewLSM creates an LSM instance and replays unflushed entries from the WAL.
func NewLSM(nodeID, addr, dataDir string, wal *shrimpwal.WAL, reg registryAPI) (*LSM, error) {
	idx, err := NewIndexEngine(nodeID, dataDir)
	if err != nil {
		return nil, fmt.Errorf("new index engine: %w", err)
	}

	rowBlockCache, _ := otter.MustBuilder[shrimptypes.RowCacheKey, *shrimpblock.BinBlock](256 << 20).
		Cost(func(_ shrimptypes.RowCacheKey, bb *shrimpblock.BinBlock) uint32 {
			return bb.HeapCost()
		}).
		Build()

	l := &LSM{
		nodeID:          nodeID,
		addr:            addr,
		dataDir:         dataDir,
		mem:             &MemTable{},
		wal:             wal,
		reg:             reg,
		flushSig:        make(chan struct{}, 1),
		idxEngine:       idx,
		rowBlockCache:   rowBlockCache,
		partMgr:         NewPartManager(dataDir),
		pendingParts:    make(map[string]shrimptypes.PartMeta),
		pendingAttempts: make(map[string]pendingAttempt),
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
			l.partMgr.Close()
			l.rowBlockCache.Close()
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
			l.mu.RLock()
			activeIDs := make(map[string]struct{}, len(l.parts))
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

func newPartID(nodeID string) string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), nodeID)
}
