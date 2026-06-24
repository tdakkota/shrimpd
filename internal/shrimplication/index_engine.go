package shrimplication

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/maypok86/otter"
	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/tdakkota/shrimpd/internal/shrimpwal"
)

const indexFlushThreshold = 1000

// IndexEngine is a local-only index table for fast text-token lookup.
type IndexEngine struct {
	nodeID  string
	dataDir string

	// writeMu guards the pairing of mem.Write+wal.Enqueue (in Write) against
	// mem snapshot+wal.Seal (in Flush), so the sealed WAL segment's contents
	// exactly equal the flushed snapshot. Held only across that brief boundary,
	// never across the heavy index-block write.
	writeMu sync.Mutex
	mem     *IndexMemTable      // own internal lock; see writeMu/mu for cross-field invariants
	wal     *shrimpwal.IndexWAL // own internal lock; see writeMu

	flushSig chan struct{} // buffered(1): signals the Run loop to flush

	// mu guards parts and covered, and is also held for the whole duration of a
	// Flush so Lookup never observes the memtable drained before the new part is
	// published (which would falsely prune). Because it is held across the entire
	// flush, it also serializes flushes against each other.
	mu      sync.RWMutex
	parts   []shrimptypes.IndexPartMeta
	covered map[string]struct{}

	idxBlockCache otter.Cache[string, shrimptypes.IndexBlock] // keyed by absolute path; own internal lock
}

// NewIndexEngine initializes the IndexEngine, recovers WAL, and loads metadata.
func NewIndexEngine(nodeID, dataDir string) (*IndexEngine, error) {
	indexDir := filepath.Join(dataDir, "index")
	if err := os.MkdirAll(indexDir, 0o750); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dataDir, "index-wal.jsonl")
	wal, err := shrimpwal.OpenIndexWAL(walPath)
	if err != nil {
		return nil, err
	}
	idxBlockCache, _ := otter.MustBuilder[string, shrimptypes.IndexBlock](64 << 20).
		Cost(func(_ string, b shrimptypes.IndexBlock) uint32 {
			n := 0
			for _, e := range b.Entries {
				n += len(e.Token) + len(e.DataID)
			}
			return uint32(n)
		}).
		Build()

	engine := &IndexEngine{
		nodeID:        nodeID,
		dataDir:       dataDir,
		mem:           &IndexMemTable{},
		wal:           wal,
		flushSig:      make(chan struct{}, 1),
		idxBlockCache: idxBlockCache,
	}

	// Recover unflushed index entries from WAL
	entries, err := wal.Recover()
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("index wal recover: %w", err)
	}
	if len(entries) > 0 {
		engine.mem.Write(entries)
	}

	// Load local index part metadata
	if err := engine.loadParts(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("load index parts: %w", err)
	}

	// Load covered parts
	if err := engine.loadCovered(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("load covered: %w", err)
	}

	return engine, nil
}

func (e *IndexEngine) loadParts() error {
	indexDir := filepath.Join(e.dataDir, "index")
	files, err := os.ReadDir(indexDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var parts []shrimptypes.IndexPartMeta
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if filepath.Ext(f.Name()) == ".meta" {
			metaPath := filepath.Join(indexDir, f.Name())
			meta, err := shrimpblock.ReadIndexMeta(metaPath)
			if err != nil {
				slog.Warn("failed to read index part meta", "path", metaPath, "error", err)
				continue
			}
			parts = append(parts, meta)
		}
	}

	slices.SortFunc(parts, func(a, b shrimptypes.IndexPartMeta) int {
		return cmp.Compare(a.ID, b.ID)
	})

	e.parts = parts
	return nil
}

func (e *IndexEngine) loadCovered() error {
	path := filepath.Join(e.dataDir, "index", "covered.json")
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		if os.IsNotExist(err) {
			e.covered = make(map[string]struct{})
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()
	var list []string
	if err := json.NewDecoder(f).Decode(&list); err != nil {
		return err
	}
	e.covered = make(map[string]struct{})
	for _, id := range list {
		e.covered[id] = struct{}{}
	}
	return nil
}

func (e *IndexEngine) saveCovered() error {
	path := filepath.Join(e.dataDir, "index", "covered.json")
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-covered-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	var list []string
	for id := range e.covered {
		list = append(list, id)
	}
	if err := json.NewEncoder(tmp).Encode(list); err != nil {
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
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}

// MarkCovered marks data part IDs as covered by the index and persists the list.
func (e *IndexEngine) MarkCovered(dataIDs []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range dataIDs {
		e.covered[id] = struct{}{}
	}
	return e.saveCovered()
}

// Write appends entries to the index WAL and writes to memtable. The fsync wait
// happens after releasing writeMu (group commit); see LSM.Write for the rationale.
func (e *IndexEngine) Write(entries []shrimptypes.IndexEntry) error {
	if len(entries) == 0 {
		return nil
	}
	e.writeMu.Lock()
	commit := e.wal.Enqueue(entries)
	e.mem.Write(entries)
	full := e.mem.Len() >= indexFlushThreshold
	e.writeMu.Unlock()

	if err := commit.Wait(); err != nil {
		return fmt.Errorf("index wal append: %w", err)
	}
	if full {
		select {
		case e.flushSig <- struct{}{}:
		default:
		}
	}
	return nil
}

// Flush sorts/deduplicates the index memtable and writes an immutable index part.
//
// Lock order is mu → writeMu. mu is held for the whole flush: it both serializes
// flushes against each other and prevents a concurrent Lookup from observing the
// memtable drained before the new part is published (which would falsely prune).
// writeMu is held only across the brief "snapshot memtable + seal WAL" boundary,
// then released so concurrent Write is not blocked on the heavy index-block write.
func (e *IndexEngine) Flush(ctx context.Context) error {
	e.mu.Lock()

	// Brief boundary: snapshot the memtable and seal the WAL atomically vs Write.
	e.writeMu.Lock()
	var entries []shrimptypes.IndexEntry
	e.mem.SnapshotView(func(snapshot []shrimptypes.IndexEntry) {
		entries = make([]shrimptypes.IndexEntry, len(snapshot))
		copy(entries, snapshot)
	})
	if len(entries) == 0 {
		e.writeMu.Unlock()
		e.mu.Unlock()
		return nil
	}
	sealedSeq, sealErr := e.wal.Seal()
	e.writeMu.Unlock() // Write may now append to the fresh active segment + memtable.
	if sealErr != nil {
		e.mem.Write(entries) // nothing sealed cleanly; put entries back for a retry
		e.mu.Unlock()
		return fmt.Errorf("seal index wal: %w", sealErr)
	}

	slices.SortFunc(entries, func(a, b shrimptypes.IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	entries = slices.CompactFunc(entries, func(a, b shrimptypes.IndexEntry) bool {
		return a.Token == b.Token && a.DataID == b.DataID
	})

	id := fmt.Sprintf("%d-%s", time.Now().UnixNano(), e.nodeID)
	indexDir := filepath.Join(e.dataDir, "index")
	path := filepath.Join(indexDir, id+".json")
	metaPath := filepath.Join(indexDir, id+".meta")
	compression := shrimpblock.CompressionZstd

	// On any failure, restore the entries to the memtable and keep the sealed WAL
	// segment as their durable copy (a later flush discards it).
	if err := shrimpblock.WriteIndexBlock(path, shrimptypes.IndexBlock{Entries: entries}, compression); err != nil {
		e.mem.Write(entries)
		e.mu.Unlock()
		return fmt.Errorf("write index block: %w", err)
	}

	meta := shrimptypes.IndexPartMeta{
		ID:          id,
		NodeID:      e.nodeID,
		Level:       0,
		MinToken:    entries[0].Token,
		MaxToken:    entries[len(entries)-1].Token,
		Count:       len(entries),
		CreatedAt:   time.Now().UnixNano(),
		Compression: compression,
	}

	if err := shrimpblock.WriteIndexMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		e.mem.Write(entries)
		e.mu.Unlock()
		return fmt.Errorf("write index meta: %w", err)
	}

	e.parts = append(e.parts, meta)
	e.mu.Unlock()

	// Entries are now durable in the index part: the sealed WAL segments are
	// redundant and can be deleted.
	if err := e.wal.Discard(sealedSeq); err != nil {
		slog.WarnContext(ctx, "index wal discard failed", "error", err)
	}

	slog.InfoContext(ctx, "flushed index part", "id", id, "count", meta.Count)
	return nil
}

// Compact merges L0 index parts into a higher-level part and drops stale DataIDs.
func (e *IndexEngine) Compact(ctx context.Context, activeDataIDs map[string]struct{}) error {
	e.mu.Lock()
	var l0 []shrimptypes.IndexPartMeta
	for _, p := range e.parts {
		if p.Level == 0 {
			l0 = append(l0, p)
		}
	}
	e.mu.Unlock()

	if len(l0) < 2 {
		return nil
	}

	var merged []shrimptypes.IndexEntry
	for _, meta := range l0 {
		blockPath := filepath.Join(e.dataDir, "index", meta.ID+".json")
		block, err := shrimpblock.ReadIndexBlock(blockPath)
		if err != nil {
			return fmt.Errorf("read index part %s: %w", meta.ID, err)
		}
		merged = append(merged, block.Entries...)
	}

	var active []shrimptypes.IndexEntry
	for _, entry := range merged {
		if _, ok := activeDataIDs[entry.DataID]; ok {
			active = append(active, entry)
		}
	}

	slices.SortFunc(active, func(a, b shrimptypes.IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	active = slices.CompactFunc(active, func(a, b shrimptypes.IndexEntry) bool {
		return a.Token == b.Token && a.DataID == b.DataID
	})

	e.mu.Lock()
	defer e.mu.Unlock()

	oldSet := make(map[string]bool)
	for _, p := range l0 {
		oldSet[p.ID] = true
	}

	var next []shrimptypes.IndexPartMeta
	for _, p := range e.parts {
		if !oldSet[p.ID] {
			next = append(next, p)
		}
	}

	compression := shrimpblock.CompressionZstd
	if len(active) > 0 {
		id := fmt.Sprintf("%d-%s-compact", time.Now().UnixNano(), e.nodeID)
		indexDir := filepath.Join(e.dataDir, "index")
		path := filepath.Join(indexDir, id+".json")
		metaPath := filepath.Join(indexDir, id+".meta")

		if err := shrimpblock.WriteIndexBlock(path, shrimptypes.IndexBlock{Entries: active}, compression); err != nil {
			return fmt.Errorf("write compacted index block: %w", err)
		}

		meta := shrimptypes.IndexPartMeta{
			ID:          id,
			NodeID:      e.nodeID,
			Level:       1,
			MinToken:    active[0].Token,
			MaxToken:    active[len(active)-1].Token,
			Count:       len(active),
			CreatedAt:   time.Now().UnixNano(),
			Compression: compression,
		}

		if err := shrimpblock.WriteIndexMeta(metaPath, meta); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write compacted index meta: %w", err)
		}

		next = append(next, meta)
		slog.InfoContext(ctx, "compacted index parts", "old_count", len(l0), "new_id", id, "count", meta.Count)
	}

	e.parts = next

	// Clean up files and evict cache entries
	for _, p := range l0 {
		blockPath := filepath.Join(e.dataDir, "index", p.ID+".json")
		e.idxBlockCache.Delete(blockPath)
		_ = os.Remove(blockPath)
		_ = os.Remove(filepath.Join(e.dataDir, "index", p.ID+".meta"))
	}

	// Clean up covered map to only contain activeDataIDs
	for id := range e.covered {
		if _, ok := activeDataIDs[id]; !ok {
			delete(e.covered, id)
		}
	}
	if err := e.saveCovered(); err != nil {
		return fmt.Errorf("save covered: %w", err)
	}

	return nil
}

// Lookup queries the index for matching data part IDs.
func (e *IndexEngine) Lookup(ctx context.Context, term string, candidates []shrimptypes.PartMeta) (matchedIDs map[string]struct{}, complete bool, err error) {
	if term == "" {
		return nil, false, nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.parts) == 0 && len(e.covered) == 0 {
		return nil, false, nil
	}

	for _, pm := range candidates {
		if _, ok := e.covered[pm.ID]; !ok {
			return nil, false, nil
		}
	}

	var tokens []string
	for tok := range shrimpblock.Tokenize(term) {
		tokens = append(tokens, tok)
	}
	if len(tokens) == 0 {
		return nil, false, nil
	}

	var finalMatches map[string]struct{}
	for i, tok := range tokens {
		matches := make(map[string]struct{})

		e.mem.Lookup(tok, func(dataID string) {
			matches[dataID] = struct{}{}
		})

		for _, part := range e.parts {
			if tok < part.MinToken || tok > part.MaxToken {
				continue
			}

			blockPath := filepath.Join(e.dataDir, "index", part.ID+".json")
			block, ok := e.idxBlockCache.Get(blockPath)
			if !ok {
				var err error
				block, err = shrimpblock.ReadIndexBlock(blockPath)
				if err != nil {
					slog.WarnContext(ctx, "failed to read index block", "path", blockPath, "error", err)
					return nil, false, err
				}
				e.idxBlockCache.Set(blockPath, block)
			}

			idx, found := slices.BinarySearchFunc(block.Entries, tok, func(entry shrimptypes.IndexEntry, target string) int {
				return cmp.Compare(entry.Token, target)
			})
			if found {
				for j := idx; j >= 0 && block.Entries[j].Token == tok; j-- {
					matches[block.Entries[j].DataID] = struct{}{}
				}
				for j := idx + 1; j < len(block.Entries) && block.Entries[j].Token == tok; j++ {
					matches[block.Entries[j].DataID] = struct{}{}
				}
			}
		}

		if i == 0 {
			finalMatches = matches
		} else {
			intersected := make(map[string]struct{})
			for id := range matches {
				if _, ok := finalMatches[id]; ok {
					intersected[id] = struct{}{}
				}
			}
			finalMatches = intersected
		}
	}

	return finalMatches, true, nil
}

// ReindexPart derives and writes index entries for an existing data part.
func (e *IndexEngine) ReindexPart(_ context.Context, meta shrimptypes.PartMeta, block shrimptypes.Block) error {
	entries := shrimpblock.BuildIndexEntries(meta.ID, block.Data)
	if err := e.Write(entries); err != nil {
		return err
	}
	return e.MarkCovered([]string{meta.ID})
}

// Close flushes memory and closes WAL.
func (e *IndexEngine) Close() error {
	if e.mem.Len() > 0 {
		_ = e.Flush(context.Background())
	}
	return e.wal.Close()
}
