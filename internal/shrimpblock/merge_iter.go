package shrimpblock

import (
	"container/heap"
	"fmt"
	"iter"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

type blockCursor struct {
	pf    *PartFileV2
	part  int
	block int
	idx   int
	bb    *BinBlock
	ts    int64
}

type mergeHeap []*blockCursor

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	a := h[i]
	b := h[j]
	if a.ts != b.ts {
		return a.ts < b.ts
	}
	if a.part != b.part {
		return a.part < b.part
	}
	if a.block != b.block {
		return a.block < b.block
	}
	return a.idx < b.idx
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(*blockCursor)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func nextCursor(pf *PartFileV2, part, block int) (*blockCursor, error) {
	bb, err := ReadBinBlock(pf, block)
	if err != nil {
		return nil, err
	}
	if len(bb.TS) == 0 {
		return nil, nil
	}
	return &blockCursor{pf: pf, part: part, block: block, idx: 0, bb: bb, ts: bb.TS[0]}, nil
}

func advanceCursor(c *blockCursor) (*blockCursor, error) {
	c.idx++
	if c.idx < len(c.bb.TS) {
		c.ts = c.bb.TS[c.idx]
		return c, nil
	}
	c.block++
	if c.block >= len(c.pf.Headers) {
		return nil, nil
	}
	return nextCursor(c.pf, c.part, c.block)
}

// MergeParts returns a stream of entries from all parts in global timestamp order.
func MergeParts(parts []*PartFileV2) iter.Seq2[shrimptypes.Entry, error] {
	return func(yield func(shrimptypes.Entry, error) bool) {
		h := mergeHeap{}
		for i, pf := range parts {
			if pf == nil || len(pf.Headers) == 0 {
				continue
			}
			c, err := nextCursor(pf, i, 0)
			if err != nil {
				yield(shrimptypes.Entry{}, fmt.Errorf("load part %d block 0: %w", i, err))
				return
			}
			if c != nil {
				h = append(h, c)
			}
		}
		heap.Init(&h)
		for h.Len() > 0 {
			c := h[0]
			entry := shrimptypes.Entry{Timestamp: c.ts, Data: bytesString(c.bb.DataBytes(c.idx))}
			if !yield(entry, nil) {
				return
			}
			next, err := advanceCursor(c)
			if err != nil {
				yield(shrimptypes.Entry{}, fmt.Errorf("advance part %d block %d: %w", c.part, c.block, err))
				return
			}
			if next != nil {
				h[0] = next
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
		}
	}
}
