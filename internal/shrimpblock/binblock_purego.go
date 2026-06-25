//go:build purego

package shrimpblock

import "encoding/binary"

func bytesString(b []byte) string {
	return string(b)
}

func tsFromBuf(buf []byte, count int) []int64 {
	ts := make([]int64, count)
	for i := range count {
		ts[i] = int64(binary.LittleEndian.Uint64(buf[i*8:]))
	}
	return ts
}

func offsetsFromBuf(buf []byte, count int) []uint32 {
	offsets := make([]uint32, count+1)
	base := count * 8
	for i := range count + 1 {
		offsets[i] = binary.LittleEndian.Uint32(buf[base+i*4:])
	}
	return offsets
}
