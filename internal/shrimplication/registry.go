package shrimplication

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// etcd key schema:
//
//	/lsm/log/{index}      → LogEntry JSON   (global mutation log; sequential %016d)
//	/lsm/nodes/{node-id}  → nodeInfo JSON   (ephemeral; disappears when lease expires on crash)
//	/lsm/nodes/{node-id}/pointer → string   (persistent; local node's processed log index)
const (
	logPrefix   = "/lsm/log"
	partsPrefix = "/lsm/parts/"
	nodePrefix  = "/lsm/nodes/"
	leaseTTL    = 10 // seconds

	// maxMergeTxnDeletes caps how many OpDelete ops are included in a single etcd
	// transaction. etcd's default gRPC max message size is 2 MiB; batching keeps
	// each request well under that limit.
	maxMergeTxnDeletes = 64
)

// LogOp represents the mutation operation.
type LogOp string

const (
	// OpPut and OpMerge are the supported log operations for part lifecycle.
	OpPut LogOp = "PUT"
	// OpMerge represents a compaction merge that replaces old parts with a new one.
	OpMerge LogOp = "MERGE"
)

// LogEntry describes an operation appended to the global event log.
type LogEntry struct {
	Index    int64                `json:"index"`
	Op       LogOp                `json:"op"`
	Part     shrimptypes.PartMeta `json:"part"`
	OldParts []string             `json:"old_parts,omitempty"`
	NodeID   string               `json:"node_id"`
}

// Registry stores node and part metadata in etcd.
type Registry struct {
	cli    *clientv3.Client
	nodeID string
}

// NewRegistry creates an etcd-backed metadata registry for nodeID.
func NewRegistry(cli *clientv3.Client, nodeID string) *Registry {
	return &Registry{cli: cli, nodeID: nodeID}
}

type nodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// RegisterNode writes an ephemeral node entry backed by a lease with keepalive.
// The entry disappears automatically if the node crashes and stops sending heartbeats.
func (r *Registry) RegisterNode(ctx context.Context, addr string) error {
	lease, err := r.cli.Grant(ctx, leaseTTL)
	if err != nil {
		return err
	}
	b, err := json.Marshal(nodeInfo{ID: r.nodeID, Addr: addr})
	if err != nil {
		return err
	}
	if _, err = r.cli.Put(ctx, nodePrefix+r.nodeID, string(b),
		clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ch, err := r.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	go func() {
		for {
			if _, ok := <-ch; !ok {
				break
			}
			// drain keepalive notifications
		}
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "etcd lease keepalive failed", "node_id", r.nodeID)
		}
	}()
	slog.InfoContext(ctx, "registered node", "node_id", r.nodeID, "addr", addr, "lease", lease.ID)
	return nil
}

// AppendLog appends a new mutation operation to the global log.
// Uses an optimistic transaction loop to determine the next sequential key.
func (r *Registry) AppendLog(ctx context.Context, op LogOp, part shrimptypes.PartMeta, oldParts []string) (int64, error) {
	baseKey := "__" + logPrefix
	for range 100 {
		resp, err := r.cli.Get(ctx, logPrefix+"/", clientv3.WithLastKey()...)
		if err != nil {
			return 0, fmt.Errorf("get current log revision: %w", err)
		}

		newSeqNum := int64(1)
		var revision int64

		if len(resp.Kvs) != 0 {
			seqNumStr := path.Base(string(resp.Kvs[0].Key))
			seqNum, err := strconv.ParseInt(seqNumStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse seq num %q: %w", seqNumStr, err)
			}
			newSeqNum = seqNum + 1
			revision = resp.Header.Revision
		} else {
			respBase, err := r.cli.Get(ctx, baseKey)
			if err != nil {
				return 0, err
			}
			if len(respBase.Kvs) != 0 && len(respBase.Kvs[0].Value) != 0 {
				seqNum, err := strconv.ParseInt(string(respBase.Kvs[0].Value), 10, 64)
				if err != nil {
					return 0, fmt.Errorf("parse base seq num: %w", err)
				}
				newSeqNum = seqNum + 1
			}
			revision = respBase.Header.Revision
		}

		newKey := fmt.Sprintf("%s/%016d", logPrefix, newSeqNum)
		// Tokens are large and available locally via .meta files; strip them from etcd.
		etcdPart := part
		etcdPart.Tokens = nil
		entry := LogEntry{
			Index:    newSeqNum,
			Op:       op,
			Part:     etcdPart,
			OldParts: oldParts,
			NodeID:   r.nodeID,
		}
		b, err := json.Marshal(entry)
		if err != nil {
			return 0, err
		}

		cmp := clientv3.Compare(clientv3.ModRevision(baseKey), "<", revision+1)
		reqPrefix := clientv3.OpPut(baseKey, fmt.Sprintf("%016d", newSeqNum))
		reqNewLog := clientv3.OpPut(newKey, string(b))

		partKey := partsPrefix + part.ID
		pb, err := json.Marshal(etcdPart)
		if err != nil {
			return 0, err
		}

		// First transaction: log entry + new part + first batch of deletes.
		ops := make([]clientv3.Op, 0, 3+min(len(oldParts), maxMergeTxnDeletes))
		ops = append(ops, reqPrefix, reqNewLog, clientv3.OpPut(partKey, string(pb)))
		firstBatch := oldParts
		var remaining []string
		if op == OpMerge && len(oldParts) > maxMergeTxnDeletes {
			firstBatch = oldParts[:maxMergeTxnDeletes]
			remaining = oldParts[maxMergeTxnDeletes:]
		}
		if op == OpMerge {
			for _, old := range firstBatch {
				ops = append(ops, clientv3.OpDelete(partsPrefix+old))
			}
		}

		txnResp, err := r.cli.Txn(ctx).If(cmp).Then(ops...).Commit()
		if err != nil {
			return 0, fmt.Errorf("commit log transaction: %w", err)
		}
		if txnResp.Succeeded {
			// Delete any remaining old parts that didn't fit in the first transaction.
			if len(remaining) > 0 {
				if err := r.deleteOldParts(ctx, remaining); err != nil {
					slog.WarnContext(ctx, "delete old parts after committed merge failed", "index", newSeqNum, "error", err)
				}
			}
			return newSeqNum, nil
		}
	}
	return 0, fmt.Errorf("can't create serial log record, high concurrency")
}

// deleteOldParts removes old part keys from etcd in batches.
//
// In a distributed system, partial failure is normal: the node may crash or lose
// connectivity after the merge log entry and new part key are committed but before
// all old part keys are deleted. On recovery the node must be able to resume
// cleanup from a consistent etcd state rather than rely on in-memory bookkeeping
// that was lost. Re-reading etcd at the start of each batch achieves this: already-
// deleted keys are silently skipped, so the function is safe to call multiple times
// and converges to a clean state regardless of where a previous attempt stopped.
func (r *Registry) deleteOldParts(ctx context.Context, oldIDs []string) error {
	// Build a set of IDs we need to delete.
	want := make(map[string]struct{}, len(oldIDs))
	for _, id := range oldIDs {
		want[id] = struct{}{}
	}

	for range 100 {
		// Re-read which of the old parts are still alive in etcd.
		resp, err := r.cli.Get(ctx, partsPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			return fmt.Errorf("list parts for cleanup: %w", err)
		}

		var alive []string
		for _, kv := range resp.Kvs {
			id := strings.TrimPrefix(string(kv.Key), partsPrefix)
			if _, ok := want[id]; ok {
				alive = append(alive, id)
			}
		}
		if len(alive) == 0 {
			return nil
		}

		// Delete in one batch (bounded by maxMergeTxnDeletes).
		batch := alive
		if len(batch) > maxMergeTxnDeletes {
			batch = alive[:maxMergeTxnDeletes]
		}
		delOps := make([]clientv3.Op, len(batch))
		for i, id := range batch {
			delOps[i] = clientv3.OpDelete(partsPrefix + id)
		}
		if _, err := r.cli.Txn(ctx).Then(delOps...).Commit(); err != nil {
			return fmt.Errorf("delete old parts batch: %w", err)
		}
	}
	return fmt.Errorf("deleteOldParts: too many retries")
}

// GetLogs retrieves up to limit sequential log entries starting from a given index (inclusive).
func (r *Registry) GetLogs(ctx context.Context, fromIndex, limit int64) ([]LogEntry, error) {
	startKey := fmt.Sprintf("%s/%016d", logPrefix, fromIndex)
	endKey := fmt.Sprintf("%s/%016d", logPrefix, 9999999999999999)
	if limit <= 0 {
		limit = 1
	}

	resp, err := r.cli.Get(ctx, startKey,
		clientv3.WithRange(endKey),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, err
	}

	var entries []LogEntry
	for _, kv := range resp.Kvs {
		var entry LogEntry
		if err := json.Unmarshal(kv.Value, &entry); err == nil {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

// GetActiveParts returns the current active parts materialized in etcd under /lsm/parts/.
func (r *Registry) GetActiveParts(ctx context.Context) (map[string]shrimptypes.PartMeta, error) {
	resp, err := r.cli.Get(ctx, partsPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	active := make(map[string]shrimptypes.PartMeta, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var p shrimptypes.PartMeta
		if err := json.Unmarshal(kv.Value, &p); err == nil {
			active[p.ID] = p
		}
	}
	return active, nil
}

// BootstrapSnapshot holds a consistent snapshot of the log tail and active parts.
type BootstrapSnapshot struct {
	LogIndex int64
	Parts    map[string]shrimptypes.PartMeta
	Revision int64
}

// GetBootstrapSnapshot reads the latest log index and /lsm/parts under one revision for safe bootstrap.
func (r *Registry) GetBootstrapSnapshot(ctx context.Context) (BootstrapSnapshot, error) {
	var snap BootstrapSnapshot
	respLog, err := r.cli.Get(ctx, logPrefix+"/", clientv3.WithLastKey()...)
	if err != nil {
		return snap, err
	}
	snap.Revision = respLog.Header.Revision
	if len(respLog.Kvs) != 0 {
		seqStr := path.Base(string(respLog.Kvs[0].Key))
		if idx, err := strconv.ParseInt(seqStr, 10, 64); err == nil {
			snap.LogIndex = idx
		}
	}
	respParts, err := r.cli.Get(ctx, partsPrefix, clientv3.WithPrefix(), clientv3.WithRev(snap.Revision))
	if err != nil {
		return snap, err
	}
	snap.Parts = make(map[string]shrimptypes.PartMeta, len(respParts.Kvs))
	for _, kv := range respParts.Kvs {
		var p shrimptypes.PartMeta
		if err := json.Unmarshal(kv.Value, &p); err == nil {
			snap.Parts[p.ID] = p
		}
	}
	return snap, nil
}

// logEntryExists reports whether a specific log index key exists.
func (r *Registry) logEntryExists(ctx context.Context, idx int64) (bool, error) {
	key := fmt.Sprintf("%s/%016d", logPrefix, idx)
	resp, err := r.cli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) != 0, nil
}

// LogCleanup removes log entries with index < min pointer among currently active nodes.
func (r *Registry) LogCleanup(ctx context.Context) error {
	active, err := r.GetActiveNodes(ctx)
	if err != nil {
		return err
	}
	ptrs, err := r.GetNodeQueuePointers(ctx)
	if err != nil {
		return err
	}
	minPtr := int64(-1)
	coordinator := ""
	for _, id := range active {
		p := int64(0)
		if v, ok := ptrs[id]; ok {
			p = v
		}
		if minPtr < 0 || p < minPtr || (p == minPtr && id < coordinator) {
			minPtr = p
			coordinator = id
		}
	}
	if coordinator != r.nodeID {
		return nil
	}
	if minPtr <= 0 {
		return nil
	}
	startKey := fmt.Sprintf("%s/%016d", logPrefix, 0)
	endKey := fmt.Sprintf("%s/%016d", logPrefix, minPtr)
	_, err = r.cli.Delete(ctx, startKey, clientv3.WithRange(endKey))
	return err
}

// LogCleanupLoop runs periodic log truncation while ctx is active.
func (r *Registry) LogCleanupLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.LogCleanup(ctx); err != nil {
				slog.WarnContext(ctx, "log cleanup failed", "error", err)
			}
		}
	}
}

// GetQueuePointer retrieves the last processed log index for this node.
func (r *Registry) GetQueuePointer(ctx context.Context) (int64, error) {
	key := fmt.Sprintf("%s%s/pointer", nodePrefix, r.nodeID)
	resp, err := r.cli.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	val := string(resp.Kvs[0].Value)
	pointer, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse pointer %q: %w", val, err)
	}
	return pointer, nil
}

// GetLatestLogIndex returns the highest log index present, or 0 if none.
func (r *Registry) GetLatestLogIndex(ctx context.Context) (int64, error) {
	resp, err := r.cli.Get(ctx, logPrefix+"/", clientv3.WithLastKey()...)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	seqStr := path.Base(string(resp.Kvs[0].Key))
	return strconv.ParseInt(seqStr, 10, 64)
}

// GetNodeQueuePointers returns map of nodeID -> pointer for all nodes under /lsm/nodes/.
func (r *Registry) GetNodeQueuePointers(ctx context.Context) (map[string]int64, error) {
	resp, err := r.cli.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ptrs := make(map[string]int64)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasSuffix(key, "/pointer") {
			continue
		}
		// key: /lsm/nodes/{id}/pointer
		id := key[len(nodePrefix) : len(key)-len("/pointer")]
		val := string(kv.Value)
		p, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			continue
		}
		ptrs[id] = p
	}
	return ptrs, nil
}

// GetActiveNodes returns node IDs that currently have an ephemeral lease entry.
func (r *Registry) GetActiveNodes(ctx context.Context) ([]string, error) {
	resp, err := r.cli.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, "/pointer") {
			continue
		}
		id := key[len(nodePrefix):]
		ids = append(ids, id)
	}
	return ids, nil
}

// GetLivePeerAddrs returns HTTP addresses of all live nodes except excludeID.
// It reads ephemeral node entries (not /pointer keys) and decodes their nodeInfo.
func (r *Registry) GetLivePeerAddrs(ctx context.Context, excludeID string) ([]string, error) {
	resp, err := r.cli.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.HasSuffix(key, "/pointer") {
			continue
		}
		var info nodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			continue
		}
		if info.ID == excludeID || info.Addr == "" {
			continue
		}
		addrs = append(addrs, info.Addr)
	}
	return addrs, nil
}

// SetQueuePointer stores the last processed log index for this node.
func (r *Registry) SetQueuePointer(ctx context.Context, index int64) error {
	key := fmt.Sprintf("%s%s/pointer", nodePrefix, r.nodeID)
	_, err := r.cli.Put(ctx, key, strconv.FormatInt(index, 10))
	return err
}
