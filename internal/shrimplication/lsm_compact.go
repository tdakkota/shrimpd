package shrimplication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"

	"github.com/oteldb/shrimpd/internal/shrimpblock"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// Compact forces compaction of data parts and then compacts the index, removing
// entries for data part IDs that no longer exist.
func (l *LSM) Compact(ctx context.Context) error {
	// Force-compact L0; use threshold logic for higher levels so a single
	// freshly-created part isn't immediately re-merged.
	if err := l.compactLevel(ctx, 0, true); err != nil {
		return err
	}
	for level := 1; level <= l.maxPartLevel(); level++ {
		if err := l.compactLevel(ctx, level, false); err != nil {
			return err
		}
	}
	l.mu.RLock()
	activeIDs := make(map[string]struct{}, len(l.parts))
	for _, p := range l.parts {
		activeIDs[p.ID] = struct{}{}
	}
	l.mu.RUnlock()
	return l.idxEngine.Compact(ctx, activeIDs)
}

// compact merges parts at all levels for this node.
func (l *LSM) compact(ctx context.Context, force bool) error {
	for level := 0; level <= l.maxPartLevel(); level++ {
		if err := l.compactLevel(ctx, level, force); err != nil {
			return err
		}
	}
	return nil
}

// maxPartLevel returns the highest level present across all local parts.
func (l *LSM) maxPartLevel() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	maxLevel := 0
	for _, p := range l.parts {
		if p.NodeID == l.nodeID && p.Level > maxLevel {
			maxLevel = p.Level
		}
	}
	return maxLevel
}

// compactLevel merges all parts at the given level into one part at level+1.
func (l *LSM) compactLevel(ctx context.Context, level int, force bool) error {
	l.mu.RLock()
	var levelParts []shrimptypes.PartMeta
	for _, p := range l.parts {
		if p.Level == level && p.NodeID == l.nodeID {
			levelParts = append(levelParts, p)
		}
	}
	l.mu.RUnlock()

	if !force && len(levelParts) < compactTrigger {
		return nil
	}
	if len(levelParts) == 0 {
		if force {
			slog.DebugContext(ctx, "compaction skipped: no parts to compact", "level", level)
		}
		return nil
	}
	// Cap the merge set to bound peak memory: compacting all parts at once can
	// hold O(N × part_size) entries in memory simultaneously.
	if len(levelParts) > compactTrigger {
		levelParts = levelParts[:compactTrigger]
	}

	var pfs []*shrimpblock.PartFileV2
	for _, meta := range levelParts {
		pf, err := l.partMgr.Get(meta.ID, meta)
		if err != nil {
			return fmt.Errorf("open v2 %s: %w", meta.ID, err)
		}
		if pf == nil {
			return fmt.Errorf("v2 part not available: %s", meta.ID)
		}
		pfs = append(pfs, pf)
	}
	mergedIt := shrimpblock.MergeParts(pfs)
	var (
		count      int
		minTS      int64
		maxTS      int64
		tokenSet   = make(map[string]struct{})
		seenAnyRow bool
	)

	oldIDs := make([]string, len(levelParts))
	for i, p := range levelParts {
		oldIDs[i] = p.ID
	}

	id := newPartID(l.nodeID)
	path := l.partPath(id)
	metaPath := l.partMetaPath(id)

	blockHeaders, err := shrimpblock.WritePartV2Seq(path, mergedIt, shrimpblock.DefaultV2BlockRows, func(block []shrimptypes.Entry) error {
		if len(block) == 0 {
			return nil
		}
		if !seenAnyRow {
			minTS = block[0].Timestamp
			seenAnyRow = true
		}
		maxTS = block[len(block)-1].Timestamp
		count += len(block)
		for _, e := range block {
			for tok := range shrimpblock.Tokenize(e.Data) {
				tokenSet[tok] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("write v2 part: %w", err)
	}
	if count == 0 {
		_ = os.Remove(path)
		if force {
			slog.DebugContext(ctx, "compaction skipped: no data in parts", "level", level)
		}
		return nil
	}
	tokens := make([]string, 0, len(tokenSet))
	for tok := range tokenSet {
		tokens = append(tokens, tok)
	}
	slices.Sort(tokens)

	meta := shrimptypes.PartMeta{
		ID:            id,
		NodeID:        l.nodeID,
		Level:         level + 1,
		MinTimestamp:  minTS,
		MaxTimestamp:  maxTS,
		Count:         count,
		Addr:          l.addr,
		Tokens:        tokens,
		Compression:   shrimpblock.CompressionZstd,
		FormatVersion: 1,
		BlockCount:    len(blockHeaders),
	}

	if err := WriteMeta(metaPath, meta); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write meta: %w", err)
	}

	slog.DebugContext(ctx, "compacting parts", "old_ids", oldIDs, "new_id", id, "level", level, "new_level", level+1, "count", count)
	if _, err := l.reg.AppendLog(ctx, OpMerge, meta, oldIDs); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(metaPath)
		return fmt.Errorf("append log: %w", err)
	}

	oldSet := make(map[string]bool, len(levelParts))
	for _, p := range levelParts {
		oldSet[p.ID] = true
	}
	l.mu.Lock()
	next := make([]shrimptypes.PartMeta, 0, len(l.parts))
	has := false
	for _, p := range l.parts {
		if !oldSet[p.ID] {
			if p.ID == meta.ID {
				has = true
			}
			next = append(next, p)
		}
	}
	if !has {
		next = append(next, meta)
	}
	l.parts = next
	l.mu.Unlock()

	idxEntries := make([]shrimptypes.IndexEntry, len(tokens))
	for i, tok := range tokens {
		idxEntries[i] = shrimptypes.IndexEntry{Token: tok, DataID: meta.ID}
	}
	if err := l.idxEngine.Write(idxEntries); err != nil {
		slog.WarnContext(ctx, "failed to write index entries on compaction", "id", meta.ID, "error", err)
	} else {
		if err := l.idxEngine.MarkCovered([]string{meta.ID}); err != nil {
			slog.WarnContext(ctx, "failed to mark covered on compaction", "id", meta.ID, "error", err)
		}
	}

	slog.InfoContext(ctx, "compacted parts", "level", level, "count", len(levelParts), "id", id, "new_level", level+1, "entry_count", count)
	return nil
}
