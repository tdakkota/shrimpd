package shrimplication

import (
	"sync"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// IndexMemTable holds unflushed index entries in memory.
type IndexMemTable struct {
	mu         sync.Mutex // guards entries and tokenIndex
	entries    []shrimptypes.IndexEntry
	tokenIndex map[string]map[string]struct{}
}

// Write appends entries to the memtable and updates the token index.
func (m *IndexMemTable) Write(entries []shrimptypes.IndexEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	for _, e := range entries {
		if m.tokenIndex == nil {
			m.tokenIndex = make(map[string]map[string]struct{})
		}
		if _, ok := m.tokenIndex[e.Token]; !ok {
			m.tokenIndex[e.Token] = make(map[string]struct{})
		}
		m.tokenIndex[e.Token][e.DataID] = struct{}{}
	}
}

// Lookup queries the memtable for matching data part IDs for a given token.
func (m *IndexMemTable) Lookup(tok string, cb func(dataID string)) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	dataIDs, ok := m.tokenIndex[tok]
	if !ok {
		return false
	}
	for id := range dataIDs {
		cb(id)
	}
	return true
}

// Len returns the number of entries in the memtable.
func (m *IndexMemTable) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// Snapshot atomically swaps the memtable contents and returns the snapshot.
func (m *IndexMemTable) Snapshot() []shrimptypes.IndexEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return nil
	}
	out := make([]shrimptypes.IndexEntry, len(m.entries))
	copy(out, m.entries)
	m.entries = m.entries[:0]
	clear(m.tokenIndex)
	return out
}

// All returns a copy of all entries in the memtable.
func (m *IndexMemTable) All() []shrimptypes.IndexEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return nil
	}
	out := make([]shrimptypes.IndexEntry, len(m.entries))
	copy(out, m.entries)
	return out
}

// SnapshotView atomically swaps the memtable contents and passes the snapshot to the provided function for processing.
func (m *IndexMemTable) SnapshotView(f func([]shrimptypes.IndexEntry)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return
	}
	f(m.entries)
	m.entries = nil
	clear(m.tokenIndex)
}
