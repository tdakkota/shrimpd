# shrimpd

A distributed, LSM-tree-based log storage daemon written in Go. Each node owns
its data on local disk; **etcd** is the global metadata and replication plane.
Nodes discover parts from one another and pull them via HTTP — no shared
filesystem required.

## Architecture

```
┌──────────────┐   POST /ingest     ┌────────────────────────────────────────┐
│   Producer   │──────────────────▶ │               shrimpd node             │
│  (app/otel)  │                    │                                        │
└──────────────┘                    │  WAL ──▶ MemTable ──▶ Part (V2 binary) │
                                    │                  │                      │
                                    │            IndexEngine                  │
                                    │            (FST token index)            │
                                    └─────────────────┬──────────────────────┘
                                                      │
                                               etcd mutation log
                                              /lsm/log, /lsm/parts
                                                      │
                                    ┌─────────────────▼──────────────────────┐
                                    │           other shrimpd nodes           │
                                    │   (pull parts via GET /part/{id})       │
                                    └────────────────────────────────────────┘
```

### Write path

1. **WAL** — each `Entry{Timestamp, Data}` is fsynced to a segmented NDJSON
   write-ahead log before the call returns.
2. **MemTable** — entries accumulate in memory. When the memtable reaches 100
   entries **or** 5 s elapses, it is flushed.
3. **Flush** — entries are sorted by timestamp, serialized into an immutable
   **V2 binary part** (zstd-compressed blocks, bloom filters per block), and
   registered in etcd under `/lsm/parts/{id}`. The WAL segment is then
   discarded.

### Compaction

When a level accumulates ≥ 4 parts, up to 4 are merged into a single part at
the next level. The merged part replaces the inputs atomically in etcd. Old
part files are garbage-collected after a safety delay. Compaction runs every
15 s.

### Replication

Replication is **pull-based via the etcd mutation log** (`/lsm/log/{index}`).
On startup each node bootstraps from the latest etcd snapshot and downloads any
missing parts from peers (`GET /part/{id}`). A background loop (1 s tick) then
replays new log entries; for `OpPut` and `OpMerge` events it fetches the
relevant part from the originating node. Each node tracks its position with a
persistent *queue pointer* in etcd.

### Index engine

A secondary token-to-part index is maintained as [vellum](https://github.com/blevesearch/vellum)
FST files under `<dataDir>/index/`. The index enables queries to skip parts
that cannot contain a given token — pruning is reported in query stats as
`parts_pruned_by_index`. The index is compacted in lockstep with data
compaction and stale entries (pointing to deleted parts) are pruned automatically.

### Storage layout

```
<dataDir>/
  wal-000001.jsonl          # segmented WAL (active + sealed segments)
  index-wal.jsonl           # index WAL
  parts/
    <id>.json               # V2 binary part data (magic SHMP, zstd blocks)
    <id>.meta               # PartMeta JSON sidecar
  index/
    <id>.fst                # vellum FST index parts
    <id>.meta
```

Part IDs have the form `<unix-nano>-<node-id>`, giving a natural temporal sort.

## Binaries

| Binary | Description |
|--------|-------------|
| `shrimpd` | Storage daemon — ingest, query, compaction, replication |
| `shrimply` | Command-line query client |
| `shrimpgateway` | Round-robin HTTP gateway for multi-node deployments |
| `ch2shrimpd` | One-shot importer: reads from ClickHouse `logs` table, ingests into shrimpd |

## HTTP API

All endpoints are served by `shrimpd`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ingest` | Ingest a batch of log entries (JSON) |
| `POST` | `/ingest/otlp` | Ingest OTLP logs (JSON or protobuf) |
| `POST` | `/v1/logs` | OTLP HTTP receiver alias |
| `GET` | `/query` | Query by time range, optional term/matcher filter |
| `GET` | `/read` | Alias for `/query` |
| `GET` | `/part/{id}` | Serve a raw part to peer nodes |
| `GET` | `/parts` | List all active parts from etcd (debug) |
| `POST` | `/flush` | Force immediate memtable flush |
| `POST` | `/compact` | Force immediate compaction |

### Ingest

```http
POST /ingest
Content-Type: application/json

{"data":[{"timestamp":1700000000000000000,"data":"hello world"}]}
```

- `timestamp` — Unix nanoseconds
- `data` — raw log line string

### Query

```
GET /query?from=<ns>&to=<ns>&term=<substring>&q=<matcher-json>
```

- `from` / `to` — nanosecond timestamps (inclusive); omit for open bounds
- `term` — substring filter applied to each entry's `data` field
- `q` — structured matcher (JSON):

```json
{
  "line":   [{"op":"|=","v":"ERROR"}],
  "labels": [{"l":"service","op":"eq","v":"api"}]
}
```

Line ops: `|=` (eq), `!=` (ne), `|~` (re), `!~` (nre).  
Label ops: `eq`, `ne`, `re`, `nre`.

Response:

```json
{
  "data": [{"timestamp":1700000000000000000,"data":"hello world"}],
  "stats": {
    "parts_total": 5,
    "parts_pruned_by_ts": 2,
    "parts_pruned_by_index": 1,
    "parts_scanned": 2,
    "blocks_total": 40,
    "blocks_pruned_by_ts": 15,
    "blocks_pruned_by_index": 3,
    "blocks_scanned": 22,
    "entries_scanned": 1100,
    "entries_matched": 7,
    "used_index": true,
    "duration_ms": 12
  }
}
```

## Quick start

### Single node

```bash
# Start etcd
docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest

# Start shrimpd
go run ./cmd/shrimpd -id=node1 -addr=localhost:8080 -data=./data

# Ingest
curl -sX POST localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"data":[{"timestamp":1700000000000000000,"data":"hello from shrimpd"}]}'

# Query (last 5 minutes)
go run ./cmd/shrimply -from=5m
```

### Multi-node with Docker Compose

The `example/` directory contains a ready-made three-node cluster behind a
`shrimpgateway`.

```bash
cd example
docker compose up --build
```

Nodes are reachable at `shrimpd-1:8080`, `shrimpd-2:8080`, `shrimpd-3:8080`;
the gateway listens on `localhost:8080` and round-robins ingest across them.
Any node can answer queries covering the full dataset via pull replication.

## Configuration

### shrimpd flags

| Flag | Default | Description |
|------|---------|-------------|
| `-id` | `node1` | Unique node identifier |
| `-addr` | `localhost:8080` | HTTP listen/advertise address |
| `-data` | `./data` | Data directory (parts + WAL) |
| `-etcd` | `localhost:2379` | Comma-separated etcd endpoints |
| `-memlimit` | `0` | Soft memory limit (e.g. `400MiB`); sets `GOMEMLIMIT` |
| `-pprof` | *(disabled)* | Expose `net/http/pprof` on this address |
| `-cpuprofile` | *(disabled)* | Write CPU profile to file on exit |
| `-memprofile-dir` | *(disabled)* | Directory for automatic heap profiles |
| `-memprofile-threshold` | `0` | Heap size that triggers an automatic heap dump |
| `-memprofile-interval` | `30s` | How often to sample heap for threshold check |

### shrimpgateway flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:8080` | Listen address |
| `-upstreams` | *(see binary)* | Comma-separated shrimpd URLs |

### shrimply flags

| Flag | Default | Description |
|------|---------|-------------|
| `-server` / `-s` | `http://localhost:8080` | shrimpd address |
| `-from` | *(none)* | Start time: Go duration (`5m`) or Unix nanoseconds |
| `-to` | *(none)* | End time: Go duration or Unix nanoseconds |
| `term` (positional) | *(none)* | Substring filter |
| `-q` | *(none)* | Structured matcher JSON |
| `-n` / `-limit` | `100` | Max entries to display (0 = unlimited) |
| `-parse` | `false` | Pretty-print OTLP log JSON entries |
| `-stats` | `false` | Print query execution stats to stderr |

## Observability

- **Structured logging** — `log/slog` JSON to stderr; optionally fanned out to
  an OTLP endpoint when `OTEL_LOGS_EXPORTER` is set (and not `none`).
- **pprof** — expose via `-pprof=:6060`; heap profiles can be dumped
  automatically or on `SIGUSR1` when `-memprofile-dir` is set.

## Development

```bash
# Run all tests (normal, purego, race)
make test

# Fast check (skips integration tests)
make test_fast

# Coverage report
make coverage

# Format and lint
golangci-lint fmt ./...
golangci-lint run ./...
```

E2E tests live in `e2e/` and require Docker (testcontainers spins up etcd).
They are skipped automatically with `-short`.

## License

See [LICENSE](LICENSE).
