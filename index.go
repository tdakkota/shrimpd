package shrimpd

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
)

func tokenize(s string) iter.Seq[string] {
	return func(yield func(tok string) bool) {
		token := func(tok string) bool {
			tok = strings.ToLower(tok)
			return yield(tok)
		}
		seq := strings.FieldsFuncSeq(s, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		})
		for tok := range seq {
			if !token(tok) {
				return
			}
		}
	}
}

func buildTokenSet(entries []Entry) []string {
	var (
		seen = make(map[string]struct{})
		out  []string
	)
	for _, e := range entries {
		for tok := range tokenize(e.Data) {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				out = append(out, tok)
			}
		}
	}
	slices.Sort(out) // deterministic
	return out
}

func buildSparse(entries []Entry, every int) []SparseEntry {
	var out []SparseEntry
	for i := 0; i < len(entries); i += every {
		out = append(out, SparseEntry{Timestamp: entries[i].Timestamp, Idx: i})
	}
	return out
}

func writeSidecar(path string, idx []SparseEntry) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-sparse-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(idx); err != nil {
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
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

func readSidecar(path string) ([]SparseEntry, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal sidecar path
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	var idx []SparseEntry
	if err := json.NewDecoder(f).Decode(&idx); err != nil {
		return nil, err
	}
	return idx, nil
}

func hasToken(tokens []string, term string) bool {
	if term == "" || len(tokens) == 0 {
		return true // empty term or no Tokens (legacy part or remote not reindexed locally): cannot prune
	}
	// tokens are sorted; require every sub-token from term to be present (case-insensitive)
	for tok := range tokenize(term) {
		if _, found := slices.BinarySearch(tokens, tok); !found {
			return false
		}
	}
	return true
}

func sparseRange(sparse []SparseEntry, from, to int64) (lo, hi int) {
	const hiNotFound = 1<<31 - 1
	if len(sparse) == 0 {
		return 0, hiNotFound
	}

	// find first index where Ts >= from
	loIdx, _ := slices.BinarySearchFunc(sparse, from, func(e SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if loIdx > 0 {
		loIdx-- // include previous sample
	}
	lo = sparse[loIdx].Idx

	// find first index where Ts > to
	// Search for to+1 with a standard three-way comparator so BinarySearchFunc
	// converges correctly (handles equality and produces exact insertion point).
	hiIdx, _ := slices.BinarySearchFunc(sparse, to+1, func(e SparseEntry, target int64) int {
		return cmp.Compare(e.Timestamp, target)
	})
	if hiIdx < len(sparse) {
		hi = sparse[hiIdx].Idx
	} else {
		hi = hiNotFound // will be clamped later
	}
	return lo, hi
}

const indexFlushThreshold = 1000

// IndexMemTable holds unflushed index entries in memory.
type IndexMemTable struct {
	mu      sync.Mutex
	entries []IndexEntry
}

func (m *IndexMemTable) Write(entries []IndexEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
}

// Len returns the number of entries in the memtable.
func (m *IndexMemTable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// Snapshot atomically swaps the memtable contents and returns the snapshot.
func (m *IndexMemTable) Snapshot() []IndexEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return nil
	}
	out := make([]IndexEntry, len(m.entries))
	copy(out, m.entries)
	m.entries = nil
	return out
}

// All returns a copy of all entries in the memtable.
func (m *IndexMemTable) All() []IndexEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return nil
	}
	out := make([]IndexEntry, len(m.entries))
	copy(out, m.entries)
	return out
}

// IndexEngine is a local-only index table for fast text-token lookup.
type IndexEngine struct {
	nodeID   string
	dataDir  string
	mem      *IndexMemTable
	wal      *IndexWAL
	flushSig chan struct{}
	mu       sync.RWMutex
	parts    []IndexPartMeta
	covered  map[string]struct{}
}

// NewIndexEngine initializes the IndexEngine, recovers WAL, and loads metadata.
func NewIndexEngine(nodeID, dataDir string) (*IndexEngine, error) {
	indexDir := filepath.Join(dataDir, "index")
	if err := os.MkdirAll(indexDir, 0o750); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dataDir, "index-wal.jsonl")
	wal, err := OpenIndexWAL(walPath)
	if err != nil {
		return nil, err
	}
	engine := &IndexEngine{
		nodeID:   nodeID,
		dataDir:  dataDir,
		mem:      &IndexMemTable{},
		wal:      wal,
		flushSig: make(chan struct{}, 1),
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

	var parts []IndexPartMeta
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if filepath.Ext(f.Name()) == ".meta" {
			metaPath := filepath.Join(indexDir, f.Name())
			meta, err := readIndexMeta(metaPath)
			if err != nil {
				slog.Warn("failed to read index part meta", "path", metaPath, "error", err)
				continue
			}
			parts = append(parts, meta)
		}
	}

	slices.SortFunc(parts, func(a, b IndexPartMeta) int {
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
	return os.Rename(name, path)
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

// BuildIndexEntries tokenizes entries and returns sorted, deduplicated IndexEntry values.
func BuildIndexEntries(dataID string, entries []Entry) []IndexEntry {
	seen := make(map[string]struct{})
	var out []IndexEntry
	for _, e := range entries {
		for tok := range tokenize(e.Data) {
			if _, ok := seen[tok]; !ok {
				seen[tok] = struct{}{}
				out = append(out, IndexEntry{Token: tok, DataID: dataID})
			}
		}
	}
	slices.SortFunc(out, func(a, b IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	return out
}

// Write appends entries to the index WAL and writes to memtable.
func (e *IndexEngine) Write(entries []IndexEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := e.wal.Append(entries); err != nil {
		return fmt.Errorf("index wal append: %w", err)
	}
	e.mem.Write(entries)
	if e.mem.Len() >= indexFlushThreshold {
		select {
		case e.flushSig <- struct{}{}:
		default:
		}
	}
	return nil
}

// Flush sorts/deduplicates index memtable and writes an immutable index part.
// It holds e.mu for the entire duration so a concurrent Lookup never observes a
// state where the memtable has been snapshotted but the new part is not yet in
// e.parts (which would cause false negatives while coverage reports complete).
func (e *IndexEngine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	entries := e.mem.Snapshot()
	if len(entries) == 0 {
		return nil
	}

	slices.SortFunc(entries, func(a, b IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	entries = slices.CompactFunc(entries, func(a, b IndexEntry) bool {
		return a.Token == b.Token && a.DataID == b.DataID
	})

	if len(entries) == 0 {
		return nil
	}

	id := fmt.Sprintf("%d-%s", time.Now().UnixNano(), e.nodeID)
	indexDir := filepath.Join(e.dataDir, "index")
	path := filepath.Join(indexDir, id+".json")
	metaPath := filepath.Join(indexDir, id+".meta")

	if err := writeIndexBlock(path, IndexBlock{Entries: entries}); err != nil {
		e.mem.Write(entries) // restore on failure
		return fmt.Errorf("write index block: %w", err)
	}

	meta := IndexPartMeta{
		ID:        id,
		NodeID:    e.nodeID,
		Level:     0,
		MinToken:  entries[0].Token,
		MaxToken:  entries[len(entries)-1].Token,
		Count:     len(entries),
		CreatedAt: time.Now().UnixNano(),
	}

	if err := writeIndexMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		e.mem.Write(entries)
		return fmt.Errorf("write index meta: %w", err)
	}

	if err := e.wal.Rotate(); err != nil {
		slog.WarnContext(ctx, "index wal rotate failed", "error", err)
	}

	e.parts = append(e.parts, meta)

	slog.InfoContext(ctx, "flushed index part", "id", id, "count", meta.Count)
	return nil
}

// Compact merges L0 index parts into a higher-level part and drops stale DataIDs.
func (e *IndexEngine) Compact(ctx context.Context, activeDataIDs map[string]struct{}) error {
	e.mu.Lock()
	var l0 []IndexPartMeta
	for _, p := range e.parts {
		if p.Level == 0 {
			l0 = append(l0, p)
		}
	}
	e.mu.Unlock()

	if len(l0) < 2 {
		return nil
	}

	var merged []IndexEntry
	for _, meta := range l0 {
		blockPath := filepath.Join(e.dataDir, "index", meta.ID+".json")
		block, err := readIndexBlock(blockPath)
		if err != nil {
			return fmt.Errorf("read index part %s: %w", meta.ID, err)
		}
		merged = append(merged, block.Entries...)
	}

	var active []IndexEntry
	for _, entry := range merged {
		if _, ok := activeDataIDs[entry.DataID]; ok {
			active = append(active, entry)
		}
	}

	slices.SortFunc(active, func(a, b IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	active = slices.CompactFunc(active, func(a, b IndexEntry) bool {
		return a.Token == b.Token && a.DataID == b.DataID
	})

	e.mu.Lock()
	defer e.mu.Unlock()

	oldSet := make(map[string]bool)
	for _, p := range l0 {
		oldSet[p.ID] = true
	}

	var next []IndexPartMeta
	for _, p := range e.parts {
		if !oldSet[p.ID] {
			next = append(next, p)
		}
	}

	if len(active) > 0 {
		id := fmt.Sprintf("%d-%s-compact", time.Now().UnixNano(), e.nodeID)
		indexDir := filepath.Join(e.dataDir, "index")
		path := filepath.Join(indexDir, id+".json")
		metaPath := filepath.Join(indexDir, id+".meta")

		if err := writeIndexBlock(path, IndexBlock{Entries: active}); err != nil {
			return fmt.Errorf("write compacted index block: %w", err)
		}

		meta := IndexPartMeta{
			ID:        id,
			NodeID:    e.nodeID,
			Level:     1,
			MinToken:  active[0].Token,
			MaxToken:  active[len(active)-1].Token,
			Count:     len(active),
			CreatedAt: time.Now().UnixNano(),
		}

		if err := writeIndexMeta(metaPath, meta); err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write compacted index meta: %w", err)
		}

		next = append(next, meta)
		slog.InfoContext(ctx, "compacted index parts", "old_count", len(l0), "new_id", id, "count", meta.Count)
	}

	e.parts = next

	// Clean up files
	for _, p := range l0 {
		_ = os.Remove(filepath.Join(e.dataDir, "index", p.ID+".json"))
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
func (e *IndexEngine) Lookup(ctx context.Context, term string, candidates []PartMeta) (matchedIDs map[string]struct{}, complete bool, err error) {
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
	for tok := range tokenize(term) {
		tokens = append(tokens, tok)
	}
	if len(tokens) == 0 {
		return nil, false, nil
	}

	var finalMatches map[string]struct{}

	for i, tok := range tokens {
		matches := make(map[string]struct{})

		memEntries := e.mem.All()
		for _, entry := range memEntries {
			if entry.Token == tok {
				matches[entry.DataID] = struct{}{}
			}
		}

		for _, part := range e.parts {
			if tok < part.MinToken || tok > part.MaxToken {
				continue
			}

			blockPath := filepath.Join(e.dataDir, "index", part.ID+".json")
			block, err := readIndexBlock(blockPath)
			if err != nil {
				slog.WarnContext(ctx, "failed to read index block", "path", blockPath, "error", err)
				return nil, false, err
			}

			idx, found := slices.BinarySearchFunc(block.Entries, tok, func(entry IndexEntry, target string) int {
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
func (e *IndexEngine) ReindexPart(_ context.Context, meta PartMeta, block Block) error {
	entries := BuildIndexEntries(meta.ID, block.Data)
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

func readIndexMeta(path string) (IndexPartMeta, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		return IndexPartMeta{}, err
	}
	defer func() { _ = f.Close() }()
	var meta IndexPartMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return IndexPartMeta{}, err
	}
	return meta, nil
}

func writeIndexMeta(path string, meta IndexPartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-meta-")
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

func writeIndexBlock(path string, b IndexBlock) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-index-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(b); err != nil {
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

func readIndexBlock(path string) (IndexBlock, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal path
	if err != nil {
		return IndexBlock{}, err
	}
	defer func() { _ = f.Close() }()
	var b IndexBlock
	if err := json.NewDecoder(f).Decode(&b); err != nil {
		return IndexBlock{}, err
	}
	return b, nil
}
