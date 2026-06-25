package shrimplication

import (
	"cmp"
	"slices"
	"sync"

	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// MemTable is the in-memory write buffer. Drained atomically by flush,
// queried (without draining) by the query path.
type MemTable struct {
	mu      sync.RWMutex // guards entries
	entries []shrimptypes.Entry
}

func (m *MemTable) Write(entries []shrimptypes.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
}

// Len returns the number of buffered entries.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// Snapshot atomically copies and clears the table. Used by flush.
func (m *MemTable) Snapshot() []shrimptypes.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make([]shrimptypes.Entry, len(m.entries))
	copy(snap, m.entries)
	m.entries = m.entries[:0]
	return snap
}

// All returns a copy of the current entries without clearing. Used by query.
func (m *MemTable) All() []shrimptypes.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make([]shrimptypes.Entry, len(m.entries))
	copy(cp, m.entries)
	return cp
}

// FilterTo appends entries matching the given range and term directly to result,
// holding only a read lock. Avoids copying the entire memtable when few entries match.
func (m *MemTable) FilterTo(from, to int64, term string, result *[]shrimptypes.Entry) {
	m.FilterToWithStats(from, to, term, result, nil)
}

// FilterToWithStats is FilterTo with optional query stats accounting.
func (m *MemTable) FilterToWithStats(from, to int64, term string, result *[]shrimptypes.Entry, stats *shrimptypes.QueryStats) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.entries {
		if stats != nil {
			stats.EntriesScanned++
		}
		if e.Matches(from, to, term) {
			*result = append(*result, e)
			if stats != nil {
				stats.EntriesMatched++
			}
		}
	}
}

// StreamTo calls fn for each matching shrimptypes.entry in timestamp order, holding only a
// read lock. A sorted copy is made so callers see consistent ordering regardless
// of insertion order (flush also sorts, so this matches the on-disk invariant).
func (m *MemTable) StreamTo(from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	return m.StreamToWithStats(from, to, term, fn, nil)
}

// StreamToWithStats is StreamTo with optional query stats accounting.
func (m *MemTable) StreamToWithStats(from, to int64, term string, fn func(shrimptypes.Entry) error, stats *shrimptypes.QueryStats) error {
	m.mu.RLock()
	// Snapshot matching entries under lock; sort outside.
	var matched []shrimptypes.Entry
	for _, e := range m.entries {
		if stats != nil {
			stats.EntriesScanned++
		}
		if e.Matches(from, to, term) {
			matched = append(matched, e)
		}
	}
	m.mu.RUnlock()

	slices.SortFunc(matched, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	for _, e := range matched {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// StreamToMatcher calls fn for each entry in [from,to] that passes the matcher.
// Line filters first (cheap), then label extract + label filters only for survivors.
// A sorted copy is produced for consistent order.
func (m *MemTable) StreamToMatcher(from, to int64, mf shrimpfilter.Matcher, fn func(shrimptypes.Entry) error) error {
	return m.StreamToMatcherWithStats(from, to, mf, fn, nil)
}

// StreamToMatcherWithStats is StreamToMatcher with optional query stats accounting.
func (m *MemTable) StreamToMatcherWithStats(from, to int64, mf shrimpfilter.Matcher, fn func(shrimptypes.Entry) error, stats *shrimptypes.QueryStats) error {
	m.mu.RLock()
	var matched []shrimptypes.Entry
	for _, e := range m.entries {
		if stats != nil {
			stats.EntriesScanned++
		}
		if e.Timestamp < from || e.Timestamp > to {
			continue
		}
		if !mf.MatchLine(e.Data) {
			continue
		}
		if len(mf.Labels) > 0 {
			labels := shrimpfilter.ExtractLabels(e.Data)
			if !mf.MatchLabels(labels) {
				continue
			}
		}
		matched = append(matched, e)
	}
	m.mu.RUnlock()

	slices.SortFunc(matched, func(a, b shrimptypes.Entry) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	for _, e := range matched {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}
