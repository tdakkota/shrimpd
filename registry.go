package shrimpd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcd key schema:
//
//	/lsm/parts/{part-id}  → PartMeta JSON   (global part registry; etcd revision = WAL order)
//	/lsm/nodes/{node-id}  → nodeInfo JSON   (ephemeral; disappears when lease expires on crash)
const (
	partPrefix = "/lsm/parts/"
	nodePrefix = "/lsm/nodes/"
	leaseTTL   = 10 // seconds
)

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
		for range ch {
		}
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "etcd lease keepalive failed", "node_id", r.nodeID)
		}
	}()
	slog.InfoContext(ctx, "registered node", "node_id", r.nodeID, "addr", addr, "lease", lease.ID)
	return nil
}

// PutPart registers a part in etcd. This is the commit point: once this returns
// without error the part is globally visible to all nodes.
func (r *Registry) PutPart(ctx context.Context, meta PartMeta) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = r.cli.Put(ctx, partPrefix+meta.ID, string(b))
	return err
}

// SwapParts atomically commits a compacted part and removes its sources in a
// single etcd transaction. No node can observe a state where both old and new
// parts are absent.
func (r *Registry) SwapParts(ctx context.Context, newPart PartMeta, oldIDs []string) error {
	b, err := json.Marshal(newPart)
	if err != nil {
		return err
	}
	ops := make([]clientv3.Op, 0, len(oldIDs)+1)
	ops = append(ops, clientv3.OpPut(partPrefix+newPart.ID, string(b)))
	for _, id := range oldIDs {
		ops = append(ops, clientv3.OpDelete(partPrefix+id))
	}
	_, err = r.cli.Txn(ctx).Then(ops...).Commit()
	return err
}

// ListParts returns all known parts across all nodes.
func (r *Registry) ListParts(ctx context.Context) ([]PartMeta, error) {
	resp, err := r.cli.Get(ctx, partPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	metas := make([]PartMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var m PartMeta
		if json.Unmarshal(kv.Value, &m) == nil {
			metas = append(metas, m)
		}
	}
	return metas, nil
}

// newPartID returns a sortable, node-scoped unique part identifier.
func newPartID(nodeID string) string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), nodeID)
}
