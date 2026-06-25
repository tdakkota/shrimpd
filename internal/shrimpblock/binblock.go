package shrimpblock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// BinBlock is a view over a decompressed binary block buffer.
type BinBlock struct {
	TS      []int64
	Offsets []uint32
	Blob    []byte
}

// EncodeBinBlock appends binary-encoded entries to dst and returns the result.
// Format: [ts: count*8 LE int64] [offsets: (count+1)*4 LE uint32] [blob: variable].
func EncodeBinBlock(entries []shrimptypes.Entry, dst []byte) []byte {
	count := len(entries)

	totalDataSize := 0
	for i := range entries {
		totalDataSize += len(entries[i].Data)
	}
	totalSize := count*8 + (count+1)*4 + totalDataSize

	if cap(dst) < totalSize {
		dst = make([]byte, 0, totalSize)
	}
	dst = dst[:totalSize]

	for i, e := range entries {
		binary.LittleEndian.PutUint64(dst[i*8:], uint64(e.Timestamp))
	}

	off := 0
	offsetsStart := count * 8
	for i, e := range entries {
		binary.LittleEndian.PutUint32(dst[offsetsStart+i*4:], uint32(off))
		off += len(e.Data)
	}
	binary.LittleEndian.PutUint32(dst[offsetsStart+count*4:], uint32(off))

	blobStart := offsetsStart + (count+1)*4
	pos := 0
	for _, e := range entries {
		copy(dst[blobStart+pos:], e.Data)
		pos += len(e.Data)
	}

	return dst
}

// DecodeBinBlock validates buf and returns a BinBlock view over it.
func DecodeBinBlock(buf []byte, count int) (BinBlock, error) {
	offsetsEnd := count*8 + (count+1)*4
	if len(buf) < offsetsEnd {
		return BinBlock{}, fmt.Errorf("binblock: buffer too small: %d < %d", len(buf), offsetsEnd)
	}

	ts := tsFromBuf(buf, count)
	offsets := offsetsFromBuf(buf, count)

	if count > 0 && offsets[0] != 0 {
		return BinBlock{}, fmt.Errorf("binblock: first offset is not zero: %d", offsets[0])
	}

	blobLen := int(offsets[count])
	blobEnd := offsetsEnd + blobLen
	if len(buf) < blobEnd {
		return BinBlock{}, fmt.Errorf("binblock: buffer too short for blob: %d < %d", len(buf), blobEnd)
	}

	for i := range count {
		if offsets[i+1] < offsets[i] {
			return BinBlock{}, fmt.Errorf("binblock: non-monotonic offsets at %d: %d < %d", i, offsets[i+1], offsets[i])
		}
	}

	return BinBlock{
		TS:      ts,
		Offsets: offsets,
		Blob:    buf[offsetsEnd:blobEnd],
	}, nil
}

// DataBytes returns the i-th entry's data as a byte subslice (zero-alloc).
func (bb BinBlock) DataBytes(i int) []byte {
	return bb.Blob[bb.Offsets[i]:bb.Offsets[i+1]]
}

// Data returns the i-th entry's data as a string (allocates on match).
func (bb BinBlock) Data(i int) string {
	return string(bb.DataBytes(i))
}

// HeapCost returns the approximate heap size of this decoded block for cache sizing.
func (bb BinBlock) HeapCost() uint32 {
	return uint32(len(bb.Blob) + len(bb.TS)*8 + len(bb.Offsets)*4)
}

// rowOf returns the row index whose [Offsets[k], Offsets[k+1]) contains byte offset pos.
// Uses binary search over Offsets.
func (bb BinBlock) rowOf(pos int) int {
	// Offsets has len = count+1; search for the largest i where Offsets[i] <= pos < Offsets[i+1]
	k, _ := slices.BinarySearch(bb.Offsets, uint32(pos))
	if k > 0 && int(bb.Offsets[k]) > pos {
		k--
	}
	if k < 0 {
		k = 0
	}
	if k >= len(bb.TS) {
		k = len(bb.TS) - 1
	}
	return k
}

// Iterate calls fn for each row whose timestamp is in [from,to] and whose data
// contains the (pre-lowercased) term. fn receives a []byte subslice; caller
// allocates string only on a hit.
func (bb BinBlock) Iterate(from, to int64, term string, fn func(ts int64, data []byte) error) error {
	for i := range bb.TS {
		ts := bb.TS[i]
		if ts < from || ts > to {
			continue
		}
		b := bb.DataBytes(i)
		if term != "" && !shrimptypes.FoldContains(b, term) {
			continue
		}
		if err := fn(ts, b); err != nil {
			return err
		}
	}
	return nil
}

// IterateMatcher calls fn for each row that passes timestamp range and m.
// Uses SIMD bytes.Index over the whole Blob for OpLineEq literals when present.
func (bb BinBlock) IterateMatcher(from, to int64, m shrimpfilter.Matcher, fn func(ts int64, data []byte) error) error {
	// Collect case-sensitive OpLineEq literals for SIMD scan.
	var eqLits [][]byte
	for _, lf := range m.Line {
		if lf.Op == shrimpfilter.OpLineEq {
			eqLits = append(eqLits, []byte(lf.Value))
		}
	}

	if len(eqLits) > 0 {
		// Pick longest literal as most selective.
		lit := eqLits[0]
		for _, e := range eqLits[1:] {
			if len(e) > len(lit) {
				lit = e
			}
		}
		candidates := make([]int, 0, 16)
		for pos := 0; pos < len(bb.Blob); {
			i := bytes.Index(bb.Blob[pos:], lit)
			if i < 0 {
				break
			}
			at := pos + i
			k := bb.rowOf(at)
			end := int(bb.Offsets[k+1])
			if at+len(lit) <= end {
				candidates = append(candidates, k)
				pos = end
			} else {
				pos = at + 1
			}
		}
		for _, k := range candidates {
			ts := bb.TS[k]
			if ts < from || ts > to {
				continue
			}
			b := bb.DataBytes(k)
			if !m.MatchLineBytes(b) {
				continue
			}
			if len(m.Labels) > 0 {
				s := string(b)
				labels := shrimpfilter.ExtractLabels(s)
				if !m.MatchLabels(labels) {
					continue
				}
			}
			if err := fn(ts, b); err != nil {
				return err
			}
		}
		return nil
	}

	// Fallback: per-row loop.
	for i := range bb.TS {
		ts := bb.TS[i]
		if ts < from || ts > to {
			continue
		}
		b := bb.DataBytes(i)
		if !m.MatchLineBytes(b) {
			continue
		}
		if len(m.Labels) > 0 {
			s := string(b)
			labels := shrimpfilter.ExtractLabels(s)
			if !m.MatchLabels(labels) {
				continue
			}
		}
		if err := fn(ts, b); err != nil {
			return err
		}
	}
	return nil
}
