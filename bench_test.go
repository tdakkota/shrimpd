package shrimpd

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maypok86/otter"
	"github.com/stretchr/testify/require"
)

var benchSink any

// benchEntry produces a deterministic Entry with diverse tokens.
func benchEntry(i int) Entry {
	components := []string{"api", "db", "cache", "worker", "gateway", "scheduler", "indexer", "compactor"}
	msgs := []string{
		"processing completed successfully",
		"connection established to upstream",
		"retrieved batch of records",
		"request handled in",
		"resource utilization at threshold",
		"health check passed",
		"replicating data to peer",
		"compaction finished",
	}
	c := components[i%len(components)]
	m := msgs[(i/len(components))%len(msgs)]
	return Entry{
		Timestamp: int64(i),
		Data:      fmt.Sprintf("component=%s request_id=%d msg=%q", c, i, m),
	}
}

func benchEntries(n int) []Entry {
	out := make([]Entry, n)
	for i := range out {
		out[i] = benchEntry(i)
	}
	return out
}

// benchTokens generates n unique token strings.
func benchTokens(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tok%d", i)
	}
	return out
}

// makeBloomBlock adds tokens from the given slice into a bloom filter and returns it.
func makeBloomBlock(tokens []string) *[bloomBytes]byte {
	var b [bloomBytes]byte
	for _, tok := range tokens {
		bloomAdd(&b, tok)
	}
	return &b
}

func BenchmarkBloomMightContain(b *testing.B) {
	// 500 tokens added to bloom, 500 not added → 1000 probes total per iteration
	present := benchTokens(500)
	absent := benchTokens(1000)[500:] // tok500 … tok999
	bloom := makeBloomBlock(present)

	probes := make([]string, 0, 1000)
	probes = append(probes, present...)
	probes = append(probes, absent...)

	b.ResetTimer()
	for b.Loop() {
		for _, tok := range probes {
			benchSink = bloomMightContain(bloom, tok)
		}
	}
}

func BenchmarkBloomAdd(b *testing.B) {
	tokens := benchTokens(512)

	b.ResetTimer()
	for b.Loop() {
		var bl [bloomBytes]byte
		for _, tok := range tokens {
			bloomAdd(&bl, tok)
		}
		benchSink = bl
	}
}

func BenchmarkBuildFlushArtifacts(b *testing.B) {
	entries := benchEntries(1000)

	b.ResetTimer()
	for b.Loop() {
		tokens := buildTokenSet(entries)
		_ = BuildIndexEntries("bench-id", entries)
		benchSink = tokens
	}
}

func BenchmarkIndexLookup_MultiToken(b *testing.B) {
	dir := b.TempDir()

	idx, err := NewIndexEngine("bench", dir)
	require.NoError(b, err)
	b.Cleanup(func() { _ = idx.Close() })

	// Build 100K index entries across 100 data IDs.
	const numDataIDs = 100
	const entriesPerID = 1000
	totalTokens := 5000

	var all []IndexEntry
	for d := range numDataIDs {
		did := fmt.Sprintf("part-%d", d)
		for t := range entriesPerID {
			all = append(all, IndexEntry{
				Token:  fmt.Sprintf("tok%d", t%totalTokens),
				DataID: did,
			})
		}
	}
	slices.SortFunc(all, func(a, b IndexEntry) int {
		if c := cmp.Compare(a.Token, b.Token); c != 0 {
			return c
		}
		return cmp.Compare(a.DataID, b.DataID)
	})
	all = slices.CompactFunc(all, func(a, b IndexEntry) bool {
		return a.Token == b.Token && a.DataID == b.DataID
	})

	require.NoError(b, idx.Write(all))
	require.NoError(b, idx.Flush(context.Background()))

	// Unflushed memtable entries.
	require.NoError(b, idx.Write([]IndexEntry{
		{Token: "alpha", DataID: "part-new"},
		{Token: "beta", DataID: "part-new"},
		{Token: "gamma", DataID: "part-new"},
	}))

	covered := make([]string, numDataIDs)
	for d := range numDataIDs {
		covered[d] = fmt.Sprintf("part-%d", d)
	}
	require.NoError(b, idx.MarkCovered(append(covered, "part-new")))

	candidates := make([]PartMeta, 0, numDataIDs+1)
	for d := range numDataIDs {
		candidates = append(candidates, PartMeta{
			ID: fmt.Sprintf("part-%d", d),
		})
	}
	candidates = append(candidates, PartMeta{ID: "part-new"})

	term := "tok1 tok2 tok3"

	b.ResetTimer()
	for b.Loop() {
		matches, complete, err := idx.Lookup(context.Background(), term, candidates)
		require.NoError(b, err)
		benchSink = matches
		_ = complete
	}
}

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "wal.jsonl")
	wal, err := OpenWAL(path)
	require.NoError(b, err)
	b.Cleanup(func() { _ = wal.Close() })

	entry := Entry{Timestamp: 1, Data: "benchmark entry"}

	b.ResetTimer()
	for b.Loop() {
		_ = wal.Append([]Entry{entry})
	}
}

func BenchmarkWALAppendBatch(b *testing.B) {
	dir := b.TempDir()
	batchSize := 100

	entries := make([]Entry, batchSize)
	for i := range entries {
		entries[i] = Entry{Timestamp: int64(i), Data: "benchmark entry"}
	}

	// Benchmark the ceiling: single Append of many entries.
	// Each iteration creates a fresh WAL to avoid state artifacts.
	b.ResetTimer()
	for i := range b.N {
		path := filepath.Join(dir, fmt.Sprintf("wal-%d.jsonl", i))
		wal, err := OpenWAL(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = wal.Append(entries)
		_ = wal.Close()
	}
}

func BenchmarkReadRowBlock(b *testing.B) {
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))

	entries := benchEntries(512)
	path := filepath.Join(dir, "parts", "bench.json")
	headers, err := writePartV2(path, entries)
	require.NoError(b, err)
	require.NotEmpty(b, headers)

	meta := PartMeta{ID: "bench", FormatVersion: 1}
	pf, err := openPartV2(path, meta)
	require.NoError(b, err)
	require.NotNil(b, pf)
	b.Cleanup(func() { _ = pf.Close() })

	b.ResetTimer()
	for b.Loop() {
		rb, err := readRowBlock(pf, 0)
		require.NoError(b, err)
		benchSink = rb
	}
}

func BenchmarkRowBlockCacheCost(b *testing.B) {
	rb := &RowBlock{
		Timestamps: make([]int64, 512),
		Data:       make([]string, 512),
	}
	for i := range rb.Timestamps {
		rb.Timestamps[i] = int64(i)
		rb.Data[i] = strings.Repeat("data-payload-", 10) // ~130 B
	}

	costFn := func(_ rowCacheKey, rb *RowBlock) uint32 {
		n := 0
		for i := range rb.Timestamps {
			n += 8 + len(rb.Data[i])
		}
		return uint32(n)
	}

	b.ResetTimer()
	var c uint32
	for b.Loop() {
		c = costFn(rowCacheKey{}, rb)
	}
	benchSink = c
}

func BenchmarkFetchRemotePartV2(b *testing.B) {
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))

	entries := benchEntries(512)
	id := "bench-remote"
	path := filepath.Join(dir, "parts", id+".json")
	_, err := writePartV2(path, entries)
	require.NoError(b, err)

	raw, err := os.ReadFile(path)
	require.NoError(b, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	}))
	b.Cleanup(srv.Close)

	meta := PartMeta{
		ID:            id,
		Addr:          srv.Listener.Addr().String(),
		FormatVersion: 1,
	}

	b.ResetTimer()
	for b.Loop() {
		body, block, err := fetchRemotePart(meta)
		require.NoError(b, err)
		benchSink = body
		_ = block
	}
}

func BenchmarkCompaction_4Parts(b *testing.B) {
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))

	const numParts = 4
	const entriesPerPart = 1000

	metas := make([]PartMeta, numParts)
	for i := range numParts {
		id := fmt.Sprintf("part-%d", i)
		entries := benchEntries(entriesPerPart)
		slices.SortFunc(entries, func(a, b Entry) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
		path := filepath.Join(dir, "parts", id+".json")
		_, err := writePartV2(path, entries)
		require.NoError(b, err)

		metas[i] = PartMeta{
			ID:            id,
			NodeID:        "bench",
			Level:         0,
			MinTimestamp:  entries[0].Timestamp,
			MaxTimestamp:  entries[len(entries)-1].Timestamp,
			Count:         len(entries),
			FormatVersion: 1,
		}
		metaPath := filepath.Join(dir, "parts", id+".meta")
		require.NoError(b, writeMeta(metaPath, metas[i]))
	}

	pm := NewPartManager(dir)
	b.Cleanup(pm.Close)

	for _, meta := range metas {
		pf, err := pm.Get(meta.ID, meta)
		require.NoError(b, err)
		require.NotNil(b, pf)
	}

	outPath := filepath.Join(dir, "parts", "compacted.json")

	b.ResetTimer()
	for b.Loop() {
		var merged []Entry
		for _, meta := range metas {
			pf, err := pm.Get(meta.ID, meta)
			require.NoError(b, err)
			for j := range pf.Headers {
				rb, err := readRowBlock(pf, j)
				require.NoError(b, err)
				for k := range rb.Timestamps {
					merged = append(merged, Entry{
						Timestamp: rb.Timestamps[k],
						Data:      rb.Data[k],
					})
				}
			}
		}
		slices.SortFunc(merged, func(a, b Entry) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
		_, err := writePartV2(outPath, merged)
		require.NoError(b, err)
		_ = os.Remove(outPath)
	}
}

func BenchmarkFlushWhileWriting(b *testing.B) {
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))

	mt := &MemTable{}
	mt.Write(benchEntries(500))

	// Use a channel to force sequential overlap: the goroutine takes Snapshot
	// (acquires the exclusive lock), and we start measuring writes only after
	// the lock is released. This ensures writes contend with the Snapshot.
	done := make(chan struct{})
	go func() {
		mt.Snapshot()
		close(done)
	}()
	<-done // Snapshot finished, lock released
	// Refill so the benchmark has data to work with.
	mt.Write(benchEntries(500))

	var mu sync.Mutex
	timings := make([]time.Duration, 0, b.N)
	var wg sync.WaitGroup

	wg.Go(func() {
		entries := mt.Snapshot()
		if len(entries) > 0 {
			slices.SortFunc(entries, func(a, b Entry) int {
				return cmp.Compare(a.Timestamp, b.Timestamp)
			})
			_, _ = writePartV2(filepath.Join(dir, "parts", "flush-out.json"), entries)
		}
	})

	b.ResetTimer()
	for i := range b.N {
		start := time.Now()
		mt.Write([]Entry{{Timestamp: int64(i), Data: "concurrent-write"}})
		elapsed := time.Since(start)
		mu.Lock()
		timings = append(timings, elapsed)
		mu.Unlock()
	}

	wg.Wait()

	if len(timings) > 0 {
		slices.Sort(timings)
		p99 := timings[int(float64(len(timings))*0.99)]
		b.ReportMetric(float64(p99.Nanoseconds()), "p99_ns")
	}
}

func BenchmarkQuery_MemTable(b *testing.B) {
	dir := b.TempDir()
	wal, walClose := benchWAL(b, dir)
	idx, idxClose := benchIndexEngine(b, dir)
	lsm := benchLSM(b, dir, wal, idx)
	b.Cleanup(func() {
		walClose()
		idxClose()
		lsm.Close()
	})

	const numEntries = 100_000
	for i := range numEntries {
		lsm.mem.Write([]Entry{benchEntry(i)})
	}

	b.ResetTimer()
	for b.Loop() {
		result, err := lsm.Query(context.Background(), 0, 1<<62, "")
		require.NoError(b, err)
		benchSink = result
	}
}

func BenchmarkQuery_L0Parts(b *testing.B) {
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))

	wal, walClose := benchWAL(b, dir)
	idx, idxClose := benchIndexEngine(b, dir)
	lsm := benchLSM(b, dir, wal, idx)
	b.Cleanup(func() {
		walClose()
		idxClose()
		lsm.Close()
	})

	// Create 3 L0 parts with 500 entries each.
	for p := range 3 {
		entries := benchEntries(500)
		slices.SortFunc(entries, func(a, b Entry) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
		id := fmt.Sprintf("l0-part-%d", p)
		path := filepath.Join(dir, "parts", id+".json")
		metaPath := filepath.Join(dir, "parts", id+".meta")

		headers, err := writePartV2(path, entries)
		require.NoError(b, err)

		meta := PartMeta{
			ID:            id,
			NodeID:        "bench",
			Level:         0,
			MinTimestamp:  entries[0].Timestamp,
			MaxTimestamp:  entries[len(entries)-1].Timestamp,
			Count:         len(entries),
			Compression:   compressionZstd,
			FormatVersion: 1,
			BlockCount:    len(headers),
		}
		require.NoError(b, writeMeta(metaPath, meta))

		lsm.mu.Lock()
		lsm.parts = append(lsm.parts, meta)
		lsm.mu.Unlock()
	}

	b.ResetTimer()
	for b.Loop() {
		result, err := lsm.Query(context.Background(), 0, 1<<62, "")
		require.NoError(b, err)
		benchSink = result
	}
}

func benchWAL(b *testing.B, dir string) (wal *WAL, cleanup func()) {
	b.Helper()
	path := filepath.Join(dir, "wal.jsonl")
	wal, err := OpenWAL(path)
	require.NoError(b, err)
	return wal, func() { _ = wal.Close() }
}

func benchIndexEngine(b *testing.B, dir string) (idx *IndexEngine, cleanup func()) {
	b.Helper()
	idx, err := NewIndexEngine("bench", dir)
	require.NoError(b, err)
	return idx, func() { _ = idx.Close() }
}

func benchLSM(b *testing.B, dir string, wal *WAL, idx *IndexEngine) *LSM {
	b.Helper()
	rowBlockCache, _ := otter.MustBuilder[rowCacheKey, *RowBlock](256 << 20).
		Cost(func(_ rowCacheKey, rb *RowBlock) uint32 {
			n := 0
			for i := range rb.Timestamps {
				n += 8 + len(rb.Data[i])
			}
			return uint32(n)
		}).
		Build()

	sparseCache, _ := otter.MustBuilder[string, []SparseEntry](8 << 20).
		Cost(func(_ string, s []SparseEntry) uint32 {
			return uint32(len(s) * 12)
		}).
		Build()

	return &LSM{
		nodeID:        "bench",
		dataDir:       dir,
		mem:           &MemTable{},
		wal:           wal,
		flushSig:      make(chan struct{}, 1),
		idxEngine:     idx,
		rowBlockCache: rowBlockCache,
		sparseCache:   sparseCache,
		partMgr:       NewPartManager(dir),
	}
}
