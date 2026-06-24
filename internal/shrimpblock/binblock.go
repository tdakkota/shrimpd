package shrimpblock

import (
	"encoding/binary"
	"fmt"

	"github.com/tdakkota/shrimpd/internal/shrimptypes"
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
