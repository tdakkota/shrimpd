package shrimpd

import "strings"

// Entry is the fundamental unit of data. Timestamp is used for ordering and pruning.
type Entry struct {
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
}

// Matches returns true if the entry matches the given time range and term.
func (e Entry) Matches(from, to int64, term string) bool {
	if !(e.Timestamp >= from && e.Timestamp <= to) {
		return false
	}
	if term == "" {
		return true
	}
	if strings.Contains(e.Data, term) {
		return true
	}
	return strings.Contains(strings.ToLower(e.Data), term)
}

// Block is the wire and file format for a collection of entries.
type Block struct {
	SourceReplica string   `json:"source_replica,omitempty"`
	CreatedAt     int64    `json:"created_at,omitempty"`
	SourceBlocks  []string `json:"source_blocks,omitempty"`
	Data          []Entry  `json:"data"`
}

// SparseEntry is a sparse timestamp index entry pointing into Block.Data.
type SparseEntry struct {
	Timestamp int64 `json:"ts"`
	Idx       int   `json:"idx"` // index into Block.Data
}

// PartMeta describes an immutable part stored on disk and registered in etcd.
// It lives at /lsm/parts/{id} and acts as both the part registry and the global WAL
// of committed parts (etcd revision gives total ordering across nodes).
type PartMeta struct {
	ID           string   `json:"id"`
	NodeID       string   `json:"node_id"`
	Level        int      `json:"level"` // 0 = freshly flushed, 1+ = compacted
	MinTimestamp int64    `json:"min_timestamp"`
	MaxTimestamp int64    `json:"max_timestamp"`
	Count        int      `json:"count"`
	Addr         string   `json:"addr"`             // host:port of the origin node's HTTP server
	Tokens       []string `json:"tokens,omitempty"` // token set for text pruning
	Compression  string   `json:"compression,omitempty"`
	// FormatVersion is 0 for legacy JSON parts, 1 for v2 binary parts.
	FormatVersion int `json:"fmt,omitempty"`
	BlockCount    int `json:"blocks,omitempty"`
}

func (m PartMeta) overlaps(from, to int64) bool {
	return m.MaxTimestamp >= from && m.MinTimestamp <= to
}

// BlockHeader is the in-memory descriptor for one block within a v2 part file.
type BlockHeader struct {
	Offset       int64 // byte offset in the part file (for ReadAt)
	CompressedSz int64 // exact byte count to read
	Count        int32 // number of rows in this block
	MinTimestamp int64
	MaxTimestamp int64
	Bloom        [1024]byte // 8192-bit blocked bloom filter, k=4
}

// RowBlock is the decoded content of one block.
type RowBlock struct {
	Timestamps []int64  // sorted, parallel to Data
	Data       []string
}

// rowCacheKey is the cache key for RowBlock caching.
type rowCacheKey struct {
	PartID string
	Block  int
}

// IndexEntry represents a mapping from a token to a data part ID.
type IndexEntry struct {
	Token  string `json:"token"`
	DataID string `json:"data_id"`
}

// IndexBlock is the file format for a collection of index entries.
type IndexBlock struct {
	Entries []IndexEntry `json:"entries"`
}

// IndexPartMeta describes an immutable index part stored on disk.
type IndexPartMeta struct {
	ID          string `json:"id"`
	NodeID      string `json:"node_id"`
	Level       int    `json:"level"`
	MinToken    string `json:"min_token"`
	MaxToken    string `json:"max_token"`
	Count       int    `json:"count"`
	CreatedAt   int64  `json:"created_at,omitempty"`
	Compression string `json:"compression,omitempty"`
}
