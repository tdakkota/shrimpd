package shrimplication

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/blevesearch/vellum"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
	"github.com/tdakkota/shrimpd/internal/shrimpwal"
)

const indexFlushThreshold = 1000

// IndexEngine is a local-only index table for fast text-token and label lookup.
// Index data is stored as vellum FST files with composite keys (token+"\x00"+dataID).
type IndexEngine struct {
	nodeID  string
	dataDir string

	// writeMu guards the pairing of mem.Write+wal.Enqueue (in Write) against
	// mem snapshot+wal.Seal (in Flush), so the sealed WAL segment's contents
	// exactly equal the flushed snapshot. Held only across that brief boundary,
	// never across the heavy index-block write.
	writeMu sync.Mutex
	mem     *IndexMemTable
	wal     *shrimpwal.IndexWAL

	flushSig chan struct{}

	// mu guards parts, covered, and fstCache. Held for the whole duration of a
	// Flush so Lookup never observes the memtable drained before the new part is
	// published (which would falsely prune). Also serializes flushes.
	mu       sync.RWMutex
	parts    []shrimptypes.IndexPartMeta
	covered  map[string]struct{}
	fstCache map[string]*vellum.FST // keyed by fstPath(id); closed on eviction
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

	engine := &IndexEngine{
		nodeID:   nodeID,
		dataDir:  dataDir,
		mem:      &IndexMemTable{},
		wal:      wal,
		flushSig: make(chan struct{}, 1),
		fstCache: make(map[string]*vellum.FST),
	}

	entries, err := wal.Recover()
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("index wal recover: %w", err)
	}
	if len(entries) > 0 {
		engine.mem.Write(entries)
	}

	if err := engine.loadParts(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("load index parts: %w", err)
	}

	if err := engine.loadCovered(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("load covered: %w", err)
	}

	return engine, nil
}

func (e *IndexEngine) fstPath(id string) string {
	return filepath.Join(e.dataDir, "index", id+".fst")
}

// openFST opens (or returns cached) the mmap'd FST for the given index part ID.
// Must be called with mu held (at least read).
func (e *IndexEngine) openFST(id string) (*vellum.FST, error) {
	path := e.fstPath(id)
	if f, ok := e.fstCache[path]; ok {
		return f, nil
	}
	f, err := vellum.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // old-format part; treat as not covered
		}
		return nil, fmt.Errorf("open fst %s: %w", path, err)
	}
	e.fstCache[path] = f
	return f, nil
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
	return nil
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

// Write appends entries to the index WAL and writes to memtable.
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

// Flush sorts/deduplicates the index memtable and writes an immutable FST index part.
func (e *IndexEngine) Flush(ctx context.Context) error {
	e.mu.Lock()

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
	e.writeMu.Unlock()
	if sealErr != nil {
		e.mem.Write(entries)
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
	fstPath := filepath.Join(indexDir, id+".fst")
	metaPath := filepath.Join(indexDir, id+".meta")

	if err := shrimpblock.BuildIndexFST(fstPath, entries); err != nil {
		e.mem.Write(entries)
		e.mu.Unlock()
		return fmt.Errorf("write index fst: %w", err)
	}

	meta := shrimptypes.IndexPartMeta{
		ID:        id,
		NodeID:    e.nodeID,
		Level:     0,
		MinToken:  entries[0].Token,
		MaxToken:  entries[len(entries)-1].Token,
		Count:     len(entries),
		CreatedAt: time.Now().UnixNano(),
	}

	if err := shrimpblock.WriteIndexMeta(metaPath, meta); err != nil {
		_ = os.Remove(fstPath)
		e.mem.Write(entries)
		e.mu.Unlock()
		return fmt.Errorf("write index meta: %w", err)
	}

	e.parts = append(e.parts, meta)
	e.mu.Unlock()

	if err := e.wal.Discard(sealedSeq); err != nil {
		slog.WarnContext(ctx, "index wal discard failed", "error", err)
	}

	slog.InfoContext(ctx, "flushed index part", "id", id, "count", meta.Count)
	return nil
}

// filteringIterator wraps an FSTIterator and skips composite keys whose DataID
// is not in the active set. Implements vellum.Iterator for use with vellum.Merge.
type filteringIterator struct {
	inner  *vellum.FSTIterator
	active map[string]struct{}
	currK  []byte
	currV  uint64
}

func newFilteringIterator(inner *vellum.FSTIterator, active map[string]struct{}) *filteringIterator {
	fi := &filteringIterator{inner: inner, active: active}
	fi.advance()
	return fi
}

func (fi *filteringIterator) advance() {
	for {
		k, v := fi.inner.Current()
		if k == nil {
			fi.currK = nil
			return
		}
		_, after, ok := bytes.Cut(k, []byte{'\x00'})
		if ok {
			if _, ok := fi.active[string(after)]; ok {
				fi.currK = append(fi.currK[:0], k...)
				fi.currV = v
				return
			}
		}
		if err := fi.inner.Next(); err != nil {
			fi.currK = nil
			return
		}
	}
}

func (fi *filteringIterator) Current() (key []byte, val uint64) {
	return fi.currK, fi.currV
}

func (fi *filteringIterator) Next() error {
	if fi.currK == nil {
		return vellum.ErrIteratorDone
	}
	if err := fi.inner.Next(); err != nil {
		fi.currK = nil
		return err
	}
	fi.advance()
	if fi.currK == nil {
		return vellum.ErrIteratorDone
	}
	return nil
}

func (fi *filteringIterator) Seek(key []byte) error {
	if err := fi.inner.Seek(key); err != nil {
		fi.currK = nil
		return err
	}
	fi.advance()
	if fi.currK == nil {
		return vellum.ErrIteratorDone
	}
	return nil
}

func (fi *filteringIterator) Reset(_ *vellum.FST, _, _ []byte, _ vellum.Automaton) error {
	return errors.New("filteringIterator: Reset not supported")
}

func (fi *filteringIterator) Close() error {
	return fi.inner.Close()
}

// Compact merges L0 index parts into a higher-level part using vellum.Merge,
// filtering out stale DataIDs in a streaming fashion.
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

	// Open FSTs and create filtering iterators (streaming — O(1) memory per part).
	itrs := make([]vellum.Iterator, 0, len(l0))
	fsts := make([]*vellum.FST, 0, len(l0))
	for _, meta := range l0 {
		e.mu.Lock()
		f, err := e.openFST(meta.ID)
		e.mu.Unlock()
		if err != nil {
			for _, itr := range itrs {
				_ = itr.Close()
			}
			return fmt.Errorf("open fst for compact %s: %w", meta.ID, err)
		}
		if f == nil {
			// Old-format part without FST; skip it.
			continue
		}
		inner, err := f.Iterator(nil, nil)
		if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
			for _, itr := range itrs {
				_ = itr.Close()
			}
			return fmt.Errorf("fst iterator for compact %s: %w", meta.ID, err)
		}
		if err == nil {
			itrs = append(itrs, newFilteringIterator(inner, activeDataIDs))
			fsts = append(fsts, f)
		}
	}

	if len(itrs) == 0 {
		return nil
	}

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

	id := fmt.Sprintf("%d-%s-compact", time.Now().UnixNano(), e.nodeID)
	indexDir := filepath.Join(e.dataDir, "index")
	fstPath := filepath.Join(indexDir, id+".fst")
	metaPath := filepath.Join(indexDir, id+".meta")

	tmp, err := os.CreateTemp(indexDir, ".tmp-compact-fst-")
	if err != nil {
		for _, itr := range itrs {
			_ = itr.Close()
		}
		return fmt.Errorf("create compact temp: %w", err)
	}
	tmpName := tmp.Name()

	mergeErr := vellum.Merge(tmp, nil, itrs, vellum.MergeMin)
	_ = tmp.Sync()
	_ = tmp.Close()
	for _, itr := range itrs {
		_ = itr.Close()
	}

	if mergeErr != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("vellum merge: %w", mergeErr)
	}

	// Determine MinToken/MaxToken by opening the merged FST briefly.
	var minToken, maxToken string
	{
		merged, err := vellum.Open(tmpName)
		if err == nil {
			// GetMinKey/GetMaxKey panic (index out of range [-1]) on an empty
			// FST, which the filtering merge can legitimately produce when every
			// entry points at an inactive data part. Guard with the key count.
			if merged.Len() > 0 {
				if k, err2 := merged.GetMinKey(); err2 == nil && len(k) > 0 {
					minToken = string(k)
				}
				if k, err2 := merged.GetMaxKey(); err2 == nil && len(k) > 0 {
					maxToken = string(k)
				}
			}
			_ = merged.Close()
		}
	}

	if err := os.Rename(tmpName, fstPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename compact fst: %w", err)
	}

	meta := shrimptypes.IndexPartMeta{
		ID:        id,
		NodeID:    e.nodeID,
		Level:     1,
		MinToken:  minToken,
		MaxToken:  maxToken,
		CreatedAt: time.Now().UnixNano(),
	}
	if err := shrimpblock.WriteIndexMeta(metaPath, meta); err != nil {
		_ = os.Remove(fstPath)
		return fmt.Errorf("write compacted index meta: %w", err)
	}

	next = append(next, meta)
	e.parts = next

	// Evict old FSTs from cache and remove files.
	for _, p := range l0 {
		path := e.fstPath(p.ID)
		if f, ok := e.fstCache[path]; ok {
			_ = f.Close()
			delete(e.fstCache, path)
		}
		_ = os.Remove(path)
		_ = os.Remove(filepath.Join(indexDir, p.ID+".meta"))
	}

	// Clean up covered map.
	for id := range e.covered {
		if _, ok := activeDataIDs[id]; !ok {
			delete(e.covered, id)
		}
	}
	if err := e.saveCovered(); err != nil {
		return fmt.Errorf("save covered: %w", err)
	}

	slog.InfoContext(ctx, "compacted index parts", "old_count", len(l0), "new_id", id)
	_ = fsts // fsts opened via fstCache, closed above
	return nil
}

// lookupToken scans all index parts for a single composite-key prefix
// (token+"\x00") and collects DataIDs into matches. Must be called with mu RLock held.
func (e *IndexEngine) lookupToken(ctx context.Context, tok string, matches map[string]struct{}) {
	start := []byte(tok + "\x00")
	end := []byte(tok + "\x01")

	e.mem.Lookup(tok, func(dataID string) {
		matches[dataID] = struct{}{}
	})

	for _, part := range e.parts {
		if tok < part.MinToken || tok > part.MaxToken {
			continue
		}
		f, err := e.openFST(part.ID)
		if err != nil {
			slog.WarnContext(ctx, "failed to open index fst", "id", part.ID, "error", err)
			continue
		}
		if f == nil {
			continue
		}
		itr, err := f.Iterator(start, end)
		if err != nil {
			if errors.Is(err, vellum.ErrIteratorDone) {
				continue
			}
			slog.WarnContext(ctx, "fst iterator error", "id", part.ID, "error", err)
			continue
		}
		for {
			k, _ := itr.Current()
			if k == nil {
				break
			}
			_, after, ok := bytes.Cut(k, []byte{'\x00'})
			if ok {
				matches[string(after)] = struct{}{}
			}
			if err := itr.Next(); err != nil {
				break
			}
		}
		_ = itr.Close()
	}
}

// Lookup queries the index for matching data part IDs by term (text tokens).
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
		e.lookupToken(ctx, tok, matches)
		if i == 0 {
			finalMatches = matches
		} else {
			for id := range finalMatches {
				if _, ok := matches[id]; !ok {
					delete(finalMatches, id)
				}
			}
		}
	}

	return finalMatches, true, nil
}

// LookupTokens queries the index for matching data part IDs by pre-built tokens
// (e.g. label tokens like "lbl:service_name=svc-a"). Used by QueryMatcherWithStats.
func (e *IndexEngine) LookupTokens(ctx context.Context, tokens []string, candidates []shrimptypes.PartMeta) (matchedIDs map[string]struct{}, complete bool, err error) {
	if len(tokens) == 0 {
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

	var finalMatches map[string]struct{}
	for i, tok := range tokens {
		matches := make(map[string]struct{})
		e.lookupToken(ctx, tok, matches)
		if i == 0 {
			finalMatches = matches
		} else {
			for id := range finalMatches {
				if _, ok := matches[id]; !ok {
					delete(finalMatches, id)
				}
			}
		}
	}

	return finalMatches, true, nil
}

// ReindexPart derives and writes index entries for an existing data part (legacy Block path).
func (e *IndexEngine) ReindexPart(_ context.Context, meta shrimptypes.PartMeta, block shrimptypes.Block) error {
	entries := shrimpblock.BuildIndexEntries(meta.ID, block.Data)
	if err := e.Write(entries); err != nil {
		return err
	}
	return e.MarkCovered([]string{meta.ID})
}

// ReindexPartFile walks a part file and reindexes without full Block materialization.
func (e *IndexEngine) ReindexPartFile(_ context.Context, meta shrimptypes.PartMeta, pf *shrimpblock.PartFileV2) error {
	entries := shrimpblock.BuildIndexEntriesFromPart(meta.ID, pf)
	if err := e.Write(entries); err != nil {
		return err
	}
	return e.MarkCovered([]string{meta.ID})
}

// Close flushes memory and closes WAL and all cached FSTs.
func (e *IndexEngine) Close() error {
	if e.mem.Len() > 0 {
		_ = e.Flush(context.Background())
	}
	e.mu.Lock()
	for _, f := range e.fstCache {
		_ = f.Close()
	}
	e.mu.Unlock()
	return e.wal.Close()
}
