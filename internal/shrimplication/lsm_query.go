package shrimplication

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// Query returns entries within the given timestamp range, optionally filtered by term.
func (l *LSM) Query(ctx context.Context, from, to int64, term string) ([]shrimptypes.Entry, error) {
	result, _, err := l.QueryWithStats(ctx, from, to, term)
	return result, err
}

// QueryWithStats returns entries and execution statistics for the query.
func (l *LSM) QueryWithStats(ctx context.Context, from, to int64, term string) ([]shrimptypes.Entry, *shrimptypes.QueryStats, error) {
	started := time.Now()
	stats := &shrimptypes.QueryStats{}
	defer func() {
		stats.DurationMs = time.Since(started).Milliseconds()
	}()

	l.mu.RLock()
	allParts := make([]shrimptypes.PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()
	stats.PartsTotal = len(allParts)

	// Step 1: Filter data parts by timestamp range
	timeParts := make([]shrimptypes.PartMeta, 0, 4) // preallocate for common case of 1-4 parts
	for _, meta := range allParts {
		if meta.Overlaps(from, to) {
			timeParts = append(timeParts, meta)
			stats.BlocksTotal += meta.BlockCount
		} else {
			stats.PartsPrunedByTS++
		}
	}
	normalizedTerm := strings.ToLower(term)

	// Step 2-4: Filter by index or fall back to old behavior
	useIndexFilter := false
	var indexedPartIDs map[string]struct{}
	if normalizedTerm != "" {
		matches, complete, err := l.idxEngine.Lookup(ctx, normalizedTerm, timeParts)
		if err != nil {
			slog.WarnContext(ctx, "index lookup failed, falling back to scanning", "error", err)
		} else if complete {
			useIndexFilter = true
			indexedPartIDs = matches
			stats.UsedIndex = true
		}
	}

	result := make([]shrimptypes.Entry, 0)
	for _, meta := range timeParts {
		if useIndexFilter {
			if _, matched := indexedPartIDs[meta.ID]; !matched {
				stats.PartsPrunedByIndex++
				stats.BlocksPrunedByIndex += meta.BlockCount
				continue
			}
		} else {
			if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
				stats.PartsPrunedByIndex++
				stats.BlocksPrunedByIndex += meta.BlockCount
				continue
			}
		}
		stats.PartsScanned++

		pf, err := l.partMgr.Get(meta.ID, meta)
		if err != nil {
			return nil, stats, fmt.Errorf("open v2 part %s: %w", meta.ID, err)
		}
		if pf == nil {
			return nil, stats, fmt.Errorf("v2 part %s not found on disk (replication pending?)", meta.ID)
		}
		for i, hdr := range pf.Headers {
			if hdr.MaxTimestamp < from || hdr.MinTimestamp > to {
				stats.BlocksPrunedByTS++
				continue
			}
			if normalizedTerm != "" && !shrimpblock.BloomMightContain(&hdr.Bloom, normalizedTerm) {
				stats.BlocksPrunedByIndex++
				continue
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			rb, ok := l.rowBlockCache.Get(ck)
			if !ok {
				var err error
				rb, err = shrimpblock.ReadRowBlock(pf, i)
				if err != nil {
					slog.WarnContext(ctx, "read row block", "id", meta.ID, "block", i, "error", err)
					continue
				}
				l.rowBlockCache.Set(ck, rb)
			}

			for j := range rb.Timestamps {
				stats.EntriesScanned++
				e := shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]}
				if e.Matches(from, to, normalizedTerm) {
					result = append(result, e)
					stats.EntriesMatched++
				}
			}
		}
	}

	// Include memtable (not yet flushed to any part).
	l.mem.FilterToWithStats(from, to, normalizedTerm, &result, stats)

	slices.SortFunc(result, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return result, stats, nil
}

// QueryStream calls fn for each entry matching [from, to] and term, streaming
// results without building a result slice. Peak memory is O(one decoded block)
// rather than O(all matching entries), which avoids OOM for high-cardinality
// terms like "error" across large data sets.
//
// Results arrive in part/block order (roughly ascending timestamp) but are NOT
// globally sorted. fn must not retain the Entry after returning.
//
// For cached blocks the existing RowBlock strings are reused. For uncached blocks,
// streamRowBlock decodes with StrBytes and only allocates a Go string per match.
func (l *LSM) QueryStream(ctx context.Context, from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	_, err := l.QueryStreamWithStats(ctx, from, to, term, fn)
	return err
}

// QueryStreamWithStats streams query results and returns execution statistics.
func (l *LSM) QueryStreamWithStats(ctx context.Context, from, to int64, term string, fn func(shrimptypes.Entry) error) (*shrimptypes.QueryStats, error) {
	started := time.Now()
	stats := &shrimptypes.QueryStats{}
	defer func() {
		stats.DurationMs = time.Since(started).Milliseconds()
	}()

	l.mu.RLock()
	allParts := make([]shrimptypes.PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()
	stats.PartsTotal = len(allParts)

	timeParts := make([]shrimptypes.PartMeta, 0, 4)
	for _, meta := range allParts {
		if meta.Overlaps(from, to) {
			timeParts = append(timeParts, meta)
			stats.BlocksTotal += meta.BlockCount
		} else {
			stats.PartsPrunedByTS++
		}
	}
	normalizedTerm := strings.ToLower(term)

	useIndexFilter := false
	var indexedPartIDs map[string]struct{}
	if normalizedTerm != "" {
		matches, complete, err := l.idxEngine.Lookup(ctx, normalizedTerm, timeParts)
		if err != nil {
			slog.WarnContext(ctx, "index lookup failed, falling back to scanning", "error", err)
		} else if complete {
			useIndexFilter = true
			indexedPartIDs = matches
			stats.UsedIndex = true
		}
	}

	for _, meta := range timeParts {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if useIndexFilter {
			if _, matched := indexedPartIDs[meta.ID]; !matched {
				stats.PartsPrunedByIndex++
				stats.BlocksPrunedByIndex += meta.BlockCount
				continue
			}
		} else {
			if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
				stats.PartsPrunedByIndex++
				stats.BlocksPrunedByIndex += meta.BlockCount
				continue
			}
		}
		stats.PartsScanned++

		pf, err := l.partMgr.Get(meta.ID, meta)
		if err != nil {
			return stats, fmt.Errorf("open v2 part %s: %w", meta.ID, err)
		}
		if pf == nil {
			return stats, fmt.Errorf("v2 part %s not found on disk (replication pending?)", meta.ID)
		}
		for i, hdr := range pf.Headers {
			if hdr.MaxTimestamp < from || hdr.MinTimestamp > to {
				stats.BlocksPrunedByTS++
				continue
			}
			if normalizedTerm != "" && !shrimpblock.BloomMightContain(&hdr.Bloom, normalizedTerm) {
				stats.BlocksPrunedByIndex++
				continue
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			if rb, ok := l.rowBlockCache.Get(ck); ok {
				for j := range rb.Timestamps {
					stats.EntriesScanned++
					e := shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]}
					if e.Matches(from, to, normalizedTerm) {
						stats.EntriesMatched++
						if err := fn(e); err != nil {
							return stats, err
						}
					}
				}
				continue
			}

			// Cache miss: stream without building a RowBlock or populating cache.
			stats.EntriesScanned += int(hdr.Count)
			err := shrimpblock.StreamRowBlock(pf, i, from, to, normalizedTerm, func(e shrimptypes.Entry) error {
				stats.EntriesMatched++
				return fn(e)
			})
			if err != nil {
				slog.WarnContext(ctx, "stream row block", "id", meta.ID, "block", i, "error", err)
			}
		}
	}

	if err := l.mem.StreamToWithStats(from, to, normalizedTerm, fn, stats); err != nil {
		return stats, err
	}

	return stats, nil
}

// QueryMatcher streams entries in [from,to] that match the given Matcher.
// It is designed for the term-less LogQL pushdown case: only time-range pruning
// is applied; no index/bloom/token pruning is performed. When m.Empty(), every
// time-overlapping entry is emitted.
//
// V2 cached path applies MatchLine + label extract + MatchLabels before fn.
// V2 uncached uses StreamRowBlockMatcher (line filters on StrBytes subslice).
// Legacy blocks and memtable are supported.
func (l *LSM) QueryMatcher(ctx context.Context, from, to int64, m shrimpfilter.Matcher, fn func(shrimptypes.Entry) error) error {
	_, err := l.QueryMatcherWithStats(ctx, from, to, m, fn)
	return err
}

// QueryMatcherWithStats streams matcher query results and returns execution statistics.
func (l *LSM) QueryMatcherWithStats(ctx context.Context, from, to int64, m shrimpfilter.Matcher, fn func(shrimptypes.Entry) error) (*shrimptypes.QueryStats, error) {
	started := time.Now()
	stats := &shrimptypes.QueryStats{}
	defer func() {
		stats.DurationMs = time.Since(started).Milliseconds()
	}()

	l.mu.RLock()
	allParts := make([]shrimptypes.PartMeta, len(l.parts))
	copy(allParts, l.parts)
	l.mu.RUnlock()
	stats.PartsTotal = len(allParts)

	timeParts := make([]shrimptypes.PartMeta, 0, 4)
	for _, meta := range allParts {
		if meta.Overlaps(from, to) {
			timeParts = append(timeParts, meta)
			stats.BlocksTotal += meta.BlockCount
		} else {
			stats.PartsPrunedByTS++
		}
	}

	for _, meta := range timeParts {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		stats.PartsScanned++

		pf, err := l.partMgr.Get(meta.ID, meta)
		if err != nil {
			return stats, fmt.Errorf("open v2 part %s: %w", meta.ID, err)
		}
		if pf == nil {
			return stats, fmt.Errorf("v2 part %s not found on disk (replication pending?)", meta.ID)
		}
		for i, hdr := range pf.Headers {
			if hdr.MaxTimestamp < from || hdr.MinTimestamp > to {
				stats.BlocksPrunedByTS++
				continue
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			if rb, ok := l.rowBlockCache.Get(ck); ok {
				for j := range rb.Timestamps {
					stats.EntriesScanned++
					e := shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]}
					if e.Timestamp < from || e.Timestamp > to {
						continue
					}
					if !m.MatchLine(e.Data) {
						continue
					}
					if len(m.Labels) > 0 {
						labels := shrimpfilter.ExtractLabels(e.Data)
						if !m.MatchLabels(labels) {
							continue
						}
					}
					stats.EntriesMatched++
					if err := fn(e); err != nil {
						return stats, err
					}
				}
				continue
			}

			stats.EntriesScanned += int(hdr.Count)
			err := shrimpblock.StreamRowBlockMatcher(pf, i, from, to, m, func(e shrimptypes.Entry) error {
				stats.EntriesMatched++
				return fn(e)
			})
			if err != nil {
				slog.WarnContext(ctx, "stream row block matcher", "id", meta.ID, "block", i, "error", err)
			}
		}
	}

	if err := l.mem.StreamToMatcherWithStats(from, to, m, fn, stats); err != nil {
		return stats, err
	}

	return stats, nil
}
