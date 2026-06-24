# shrimplication — LSM Engine

This package (`shrimplication`) implements the distributed LSM engine. The storage
format lives in `internal/shrimpblock`; this package owns the lifecycle: write,
flush, compact, replicate.

---

## On-disk storage layout

All data lives under the configured `dataDir`. Each immutable **part** is a trio
of files:

```
parts/<id>.json        # part data (V2 binary)
parts/<id>.meta        # PartMeta JSON sidecar (includes Tokens, FormatVersion, etc.)
wal.jsonl              # write-ahead log (NDJSON, one Entry per line)
```

Part IDs have the form `<unix-nano>-<node-id>`, which provides a natural
temporal sort.

---

## Part file formats

### V2 binary (FormatVersion=1) — current format

Written by `shrimpblock.WritePartV2`. Magic bytes `SHMP` at offset 0.

```
[Header: 16 bytes]
  bytes  0-3   "SHMP" magic
  byte   4     version = 0x02
  bytes  5-7   reserved (zero)
  bytes  8-15  block count (uint64 LE)

[Block directory: blockCount × 1096 bytes]
  per block row (1096 bytes):
    bytes  0-7    block data offset in file (int64 LE)
    bytes  8-15   compressed size in bytes (int64 LE)
    bytes 16-19   entry count (uint32 LE)
    bytes 20-27   min timestamp (int64 LE)
    bytes 28-35   max timestamp (int64 LE)
    bytes 36-1059 bloom filter (1024 bytes / 8192 bits)
    bytes 1060-1095 padding (zeroed)

[Block data: variable]
  Each block is an independent zstd-compressed frame containing a BinBlock
  payload (see internal/shrimpblock/binblock.go):

    [ts: count*8 bytes LE int64] [offsets: (count+1)*4 bytes LE uint32] [blob: variable]

  The N-th entry's data is blob[offsets[n]:offsets[n+1]].
  Default block size: 512 entries.
```

The bloom filter in each block's directory entry is used to skip blocks whose
token sets do not contain the query term (see `shrimpblock.BloomCheck`).

### Legacy JSON (FormatVersion=0) — no longer written or readable

Part data was a `shrimptypes.Block` encoded as JSON, optionally wrapped in a
compression stream (zstd or gzip). No longer supported.

---

## Entry and Block types (`internal/shrimptypes`)

```go
type Entry struct {
    Timestamp int64  // nanoseconds since epoch
    Data      string // raw log line
}

type Block struct {
    SourceReplica string
    CreatedAt     int64
    Data          []Entry
}

type PartMeta struct {
    ID            string
    NodeID        string
    Level         int        // 0 = freshly flushed; increments on each compaction merge
    MinTimestamp  int64
    MaxTimestamp  int64
    Count         int
    Addr          string     // HTTP address of the originating node
    Tokens        []string   // token set built from all entries (stripped from etcd)
    Compression   string     // "zstd" etc.
    FormatVersion int        // 0 = legacy JSON, 1 = V2 binary
    BlockCount    int
}
```

---

## Write path

```
Write(entries)
  → WAL.Append (fsync)        durable before ack
  → MemTable.Write            in-memory accumulation
  → [if len >= 100] flush()   eager trigger

flush()
  → sort entries by Timestamp
  → shrimpblock.WritePartV2   atomic temp-file + rename
  → WriteMeta                 atomic .meta sidecar
  → Registry.AppendLog(OpPut) publish to etcd mutation log
  → WAL.Rotate                truncate WAL (entries now in part + etcd)
  → update l.parts slice
  → IndexEngine.Write + MarkCovered
```

Time-based flush fires every 5 s regardless of size.

---

## Compaction

Trigger: ≥ 4 parts at the same level (or forced). Cap: merge at most 4 parts
at once to bound peak memory.

```
compactLevel(level)
  → collect own parts at level (NodeID == l.nodeID)
  → read all their entries into memory (V2: ReadRowBlock)
  → sort merged entries by Timestamp
  → shrimpblock.WritePartV2   new part at level+1
  → WriteMeta
  → Registry.AppendLog(OpMerge, newPart, oldIDs)
  → update l.parts slice (remove old, add new)
  → IndexEngine.Write + MarkCovered
  → GC defers physical deletion of old part files (safety cutoff)
```

Levels are unbounded; compaction walks from level 0 upward each cycle.
Compaction interval: 15 s.

---

## Replication

Replication is **pull-based via etcd**. The global mutation log at
`/lsm/log/{index}` (zero-padded 16-digit decimal) is the source of truth.

Each node maintains a *queue pointer* at `/lsm/nodes/{id}/pointer` — the last
log index it has applied.

### Startup bootstrap

On `startup()`:

1. `RegisterNode` — write ephemeral lease entry at `/lsm/nodes/{id}`.
2. `GetBootstrapSnapshot` — read the latest log index and all `/lsm/parts/`
   keys at the same etcd revision for a consistent snapshot.
3. For each part in the snapshot, download it from the owning node if not
   already present locally (`GET /part/{id}` on the peer's HTTP server).
4. Advance the queue pointer to the snapshot's log index.
5. Reconcile index coverage.

### Steady-state replication loop (1 s tick)

```
for each new LogEntry since pointer:
  if entry.NodeID == own node → skip (already written locally)
  OpPut  → fetchRemotePart, writeRawPart, WriteMeta, update l.parts, ReindexPart
  OpMerge → same as above, then remove old parts from l.parts
  advance pointer, SetQueuePointer
```

Log gap detection: if the expected next index doesn't exist in etcd (e.g. after
log truncation while offline), the node re-bootstraps from the parts snapshot.

### Log cleanup

Every 30 s, the node that holds the minimum queue pointer among all active nodes
acts as cleanup coordinator: it deletes log entries with index < min-pointer
(`/lsm/log/{0}` through `/{min-pointer-1}`).

### etcd key schema

```
/lsm/log/{016d}          LogEntry JSON   (global mutation log)
/lsm/parts/{part-id}     PartMeta JSON   (active part set; Tokens stripped)
/lsm/nodes/{id}          nodeInfo JSON   (ephemeral, lease-backed)
/lsm/nodes/{id}/pointer  string int64    (persistent queue pointer)
```

---

## Index engine (`index_engine.go`)

A secondary LSM index mapping tokens → part IDs, stored under `dataDir` alongside
data parts. Used for `TermSearch` queries to skip parts that don't contain a
given token. Index parts are compacted in lockstep with data compaction; stale
index entries (pointing to deleted data parts) are pruned by `Compact(activeIDs)`.

---

## Caches

- `rowBlockCache` (otter, 256 MiB) — keyed by `(partID, blockIndex)`, caches
  decoded `*RowBlock`s for V2 parts.
- `partMgr` (`PartManager`) — LRU of open `*PartFileV2` file descriptors, so
  repeated block reads share the same `pread` fd without re-parsing the header.
