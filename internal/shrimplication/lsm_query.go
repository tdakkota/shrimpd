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

	// Text terms are pruned via PartMeta.Tokens (HasToken) + block bloom.
	// Label terms use FST via LookupTokens in QueryMatcherWithStats.
	result := make([]shrimptypes.Entry, 0)
	for _, meta := range timeParts {
		if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
			stats.PartsPrunedByIndex++
			stats.BlocksPrunedByIndex += meta.BlockCount
			continue
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
				stats.BlocksPrunedByBloom++
				continue
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			bb, ok := l.rowBlockCache.Get(ck)
			if !ok {
				var err error
				bb, err = shrimpblock.ReadBinBlock(pf, i)
				if err != nil {
					slog.WarnContext(ctx, "read bin block", "id", meta.ID, "block", i, "error", err)
					continue
				}
				l.rowBlockCache.Set(ck, bb)
			}

			_ = bb.Iterate(from, to, normalizedTerm, func(ts int64, data []byte) error {
				stats.EntriesScanned++
				result = append(result, shrimptypes.Entry{Timestamp: ts, Data: string(data)})
				stats.EntriesMatched++
				return nil
			})
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

	// Text pruning via HasToken (Tokens) + bloom; labels use FST via LookupTokens.
	for _, meta := range timeParts {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if normalizedTerm != "" && !shrimpblock.HasToken(meta.Tokens, normalizedTerm) {
			stats.PartsPrunedByIndex++
			stats.BlocksPrunedByIndex += meta.BlockCount
			continue
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
				stats.BlocksPrunedByBloom++
				continue
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			if bb, ok := l.rowBlockCache.Get(ck); ok {
				_ = bb.Iterate(from, to, normalizedTerm, func(ts int64, data []byte) error {
					stats.EntriesScanned++
					e := shrimptypes.Entry{Timestamp: ts, Data: string(data)}
					if e.Matches(from, to, normalizedTerm) {
						stats.EntriesMatched++
						if err := fn(e); err != nil {
							return err
						}
					}
					return nil
				})
				continue
			}

			// Cache miss: stream without populating cache.
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

	// Build label tokens for OpLabelEq filters and look them up in the FST index.
	var (
		useLabelIndex   bool
		labelIndexedIDs map[string]struct{}
	)
	if len(m.Labels) > 0 {
		var labelTokens []string
		for _, lf := range m.Labels {
			if lf.Op == shrimpfilter.OpLabelEq {
				labelTokens = append(labelTokens, shrimpblock.LabelTokenPrefix+lf.Label+"="+lf.Value)
			}
		}
		if len(labelTokens) > 0 {
			ids, complete, err := l.idxEngine.LookupTokens(ctx, labelTokens, timeParts)
			if err != nil {
				slog.WarnContext(ctx, "label index lookup failed", "error", err)
			} else if complete {
				useLabelIndex = true
				labelIndexedIDs = ids
				stats.UsedIndex = true
			}
		}
	}

	for _, meta := range timeParts {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		// Fast token-set pre-filter (no I/O): check label name+value appear as tokens.
		if len(m.Labels) > 0 {
			pruned := false
			for _, lf := range m.Labels {
				if lf.Op == shrimpfilter.OpLabelEq && !shrimpblock.HasToken(meta.Tokens, lf.Label+"="+lf.Value) {
					pruned = true
					break
				}
			}
			if pruned {
				stats.PartsPrunedByIndex++
				stats.BlocksPrunedByIndex += meta.BlockCount
				continue
			}
		}
		// FST index lookup: prune parts not matched by the label index.
		if useLabelIndex {
			if _, ok := labelIndexedIDs[meta.ID]; !ok {
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
			if !m.Empty() && len(m.Labels) > 0 {
				pruned := false
				for _, lf := range m.Labels {
					if lf.Op == shrimpfilter.OpLabelEq && !shrimpblock.BloomMightContainLabel(&hdr.Bloom, lf.Label, lf.Value) {
						pruned = true
						break
					}
				}
				if pruned {
					stats.BlocksPrunedByBloom++
					continue
				}
			}
			stats.BlocksScanned++

			ck := shrimptypes.RowCacheKey{PartID: meta.ID, Block: i}
			if bb, ok := l.rowBlockCache.Get(ck); ok {
				_ = bb.IterateMatcher(from, to, m, func(ts int64, data []byte) error {
					stats.EntriesScanned++
					e := shrimptypes.Entry{Timestamp: ts, Data: string(data)}
					if len(m.Labels) > 0 {
						labels := shrimpfilter.ExtractLabels(e.Data)
						if !m.MatchLabels(labels) {
							return nil
						}
					}
					stats.EntriesMatched++
					if err := fn(e); err != nil {
						return err
					}
					return nil
				})
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
