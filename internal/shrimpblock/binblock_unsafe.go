//go:build !purego

package shrimpblock

import "unsafe"

func bytesString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b)) // #nosec G103 -- callers keep the backing block immutable while the string is used.
}

func tsFromBuf(buf []byte, count int) []int64 {
	if count == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&buf[0])), count) // #nosec G103
}

func offsetsFromBuf(buf []byte, count int) []uint32 {
	base := count * 8
	if count == 0 {
		return unsafe.Slice((*uint32)(unsafe.Pointer(&buf[0])), 1) // #nosec G103
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(&buf[base])), count+1) // #nosec G103
}
