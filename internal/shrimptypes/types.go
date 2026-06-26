// Package shrimptypes defines the core data structures and types used in the shrimpd project.
package shrimptypes

// Entry is the fundamental unit of data. Timestamp is used for ordering and pruning.
type Entry struct {
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
}

// Matches returns true if the entry matches the given time range and term.
// term must already be lowercased by the caller.
func (e Entry) Matches(from, to int64, term string) bool {
	if !(e.Timestamp >= from && e.Timestamp <= to) {
		return false
	}
	if term == "" {
		return true
	}
	return FoldContains(e.Data, term)
}

// FoldContains reports whether s contains the ASCII-lowercased term anywhere,
// comparing case-insensitively without allocating. term must be pre-lowercased.
// Non-ASCII uppercase characters are not folded (acceptable for typical log data).
func FoldContains[Data, Term ~string | ~[]byte](s Data, term Term) bool {
	n := len(term)
	if n == 0 {
		return true
	}
	if len(s) < n {
		return false
	}
outer:
	for i := range len(s) - n + 1 {
		for j := range n {
			c := s[i+j]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != term[j] {
				continue outer
			}
		}
		return true
	}
	return false
}

// Block is the wire and file format for a collection of entries.
type Block struct {
	SourceReplica string      `json:"source_replica,omitempty"`
	CreatedAt     int64       `json:"created_at,omitempty"`
	SourceBlocks  []string    `json:"source_blocks,omitempty"`
	Data          []Entry     `json:"data"`
	Stats         *QueryStats `json:"stats,omitempty"`
}

// QueryStats contains basic execution statistics for a query.
type QueryStats struct {
	PartsTotal         int `json:"parts_total"`
	PartsPrunedByTS    int `json:"parts_pruned_by_ts"`
	PartsPrunedByIndex int `json:"parts_pruned_by_index"`
	PartsScanned       int `json:"parts_scanned"`

	BlocksTotal         int `json:"blocks_total"`
	BlocksPrunedByTS    int `json:"blocks_pruned_by_ts"`
	BlocksPrunedByIndex int `json:"blocks_pruned_by_index"`
	BlocksPrunedByBloom int `json:"blocks_pruned_by_bloom"`
	BlocksScanned       int `json:"blocks_scanned"`

	EntriesScanned int `json:"entries_scanned"`
	EntriesMatched int `json:"entries_matched"`

	UsedIndex  bool  `json:"used_index"`
	DurationMs int64 `json:"duration_ms"`
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
	ID              string   `json:"id"`
	NodeID          string   `json:"node_id"`
	Level           int      `json:"level"` // 0 = freshly flushed, 1+ = compacted
	MinTimestamp    int64    `json:"min_timestamp"`
	MaxTimestamp    int64    `json:"max_timestamp"`
	Count           int      `json:"count"`
	Addr            string   `json:"addr"`                       // host:port of the origin node's HTTP server
	Tokens          []string `json:"tokens,omitempty"`           // token set for text pruning
	TokensTruncated bool     `json:"tokens_truncated,omitempty"` // true when Tokens hit the cap and cannot safely prune
	Compression     string   `json:"compression,omitempty"`
	// FormatVersion is 0 for legacy JSON parts, 1 for v2 binary parts.
	FormatVersion int `json:"fmt,omitempty"`
	BlockCount    int `json:"blocks,omitempty"`
}

// Overlaps return true if this part overlaps given time range.
func (m PartMeta) Overlaps(from, to int64) bool {
	return m.MaxTimestamp >= from && m.MinTimestamp <= to
}

// BlockHeader is the in-memory descriptor for one block within a v2 part file.
type BlockHeader struct {
	Offset       int64 // byte offset in the part file (for ReadAt)
	CompressedSz int64 // exact byte count to read
	Count        int32 // number of rows in this block
	MinTimestamp int64
	MaxTimestamp int64
	Bloom        BloomFilter // 8192-bit blocked bloom filter, k=4
}

const (
	// BloomBits is the number of bits in the bloom filter.
	BloomBits = 8192
	// BloomBytes is the number of bytes in the bloom filter.
	BloomBytes = BloomBits / 8
	// BloomK is the number of hash functions used in the bloom filter.
	BloomK = 4
)

// BloomFilter is a fixed-size 8192-bit blocked bloom filter, k=4.
// It is used to quickly check whether a token might be present in a block of data.
type BloomFilter [BloomBytes]byte

// RowCacheKey is the cache key for BinBlock caching.
type RowCacheKey struct {
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
	ID          string   `json:"id"`
	NodeID      string   `json:"node_id"`
	Level       int      `json:"level"`
	MinToken    string   `json:"min_token"`
	MaxToken    string   `json:"max_token"`
	Count       int      `json:"count"`
	CreatedAt   int64    `json:"created_at,omitempty"`
	Compression string   `json:"compression,omitempty"`
	DataIDs     []string `json:"data_ids,omitempty"` // ordinal → dataID for interned FST keys
}
