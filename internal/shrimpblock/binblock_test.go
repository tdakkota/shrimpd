package shrimpblock

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestEncodeDecodeBinBlock_RoundTrip(t *testing.T) {
	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: "hello"},
		{Timestamp: 2, Data: "world"},
		{Timestamp: 3, Data: "foo bar baz"},
	}

	buf := EncodeBinBlock(entries, nil)
	bb, err := DecodeBinBlock(buf, len(entries))
	require.NoError(t, err)
	require.Equal(t, len(entries), len(bb.TS))
	require.Equal(t, len(entries)+1, len(bb.Offsets))

	for i, e := range entries {
		require.Equal(t, e.Timestamp, bb.TS[i])
		require.Equal(t, e.Data, string(bb.DataBytes(i)))
		require.Equal(t, e.Data, bb.Data(i))
	}
}

func TestEncodeDecodeBinBlock_Empty(t *testing.T) {
	buf := EncodeBinBlock(nil, nil)
	// count=0: just the one offset entry
	require.Equal(t, 4, len(buf))
	bb, err := DecodeBinBlock(buf, 0)
	require.NoError(t, err)
	require.Len(t, bb.TS, 0)
	require.Len(t, bb.Offsets, 1)
	require.Equal(t, uint32(0), bb.Offsets[0])
}

func TestEncodeDecodeBinBlock_SingleRow(t *testing.T) {
	entries := []shrimptypes.Entry{{Timestamp: 42, Data: "single"}}
	buf := EncodeBinBlock(entries, nil)
	bb, err := DecodeBinBlock(buf, 1)
	require.NoError(t, err)
	require.Equal(t, int64(42), bb.TS[0])
	require.Equal(t, "single", string(bb.DataBytes(0)))
}

func TestDecodeBinBlock_CorruptOffsetNonZeroStart(t *testing.T) {
	entries := []shrimptypes.Entry{{Timestamp: 1, Data: "x"}}
	buf := EncodeBinBlock(entries, nil)
	// Corrupt the first offset (at offset count*8 = 8) to be non-zero
	buf[8] = 0x01
	_, err := DecodeBinBlock(buf, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "first offset is not zero")
}

func TestDecodeBinBlock_CorruptNonMonotonic(t *testing.T) {
	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: "abc"},
		{Timestamp: 2, Data: "def"},
	}
	buf := EncodeBinBlock(entries, nil)
	// offsets: [0, 3, 6]. Corrupt offsets[2] to be < offsets[1].
	// offsets start at byte count*8 = 16, offsets[2] is at 16+2*4=24
	offsetsStart := 2 * 8
	binary.LittleEndian.PutUint32(buf[offsetsStart+2*4:], 1) // set offsets[2] = 1
	_, err := DecodeBinBlock(buf, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-monotonic offsets")
}

func TestDecodeBinBlock_BufferTooSmall(t *testing.T) {
	_, err := DecodeBinBlock(nil, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "buffer too small")
}

func TestDecodeBinBlock_LargeString(t *testing.T) {
	big := string(make([]byte, 64*1024))
	entries := []shrimptypes.Entry{{Timestamp: 0, Data: big}}
	buf := EncodeBinBlock(entries, nil)
	bb, err := DecodeBinBlock(buf, 1)
	require.NoError(t, err)
	require.Equal(t, big, bb.Data(0))
}

func TestEncodeBinBlock_DstReuse(t *testing.T) {
	dst := make([]byte, 0, 1024)
	entries := []shrimptypes.Entry{{Timestamp: 10, Data: "reuse"}}
	buf := EncodeBinBlock(entries, dst)
	// When enough capacity is available, buf reuses dst's backing store.
	// With cap=1024 and totalSize=21, the pre-allocation should match.
	require.GreaterOrEqual(t, cap(buf), 1024)
	bb, err := DecodeBinBlock(buf, 1)
	require.NoError(t, err)
	require.Equal(t, "reuse", string(bb.DataBytes(0)))
}

func TestDecodeBinBlock_MaxOffset(t *testing.T) {
	// Verify that a block with blob approaching MaxUint32 can be decoded.
	// Use a modest data size but set the final offset to MaxUint32.
	count := 1
	buf := make([]byte, count*8+(count+1)*4)
	binary.LittleEndian.PutUint32(buf[count*8:], 0)
	binary.LittleEndian.PutUint32(buf[count*8+4:], math.MaxUint32)
	_, err := DecodeBinBlock(buf, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "buffer too short for blob")
}

func TestDecodeBinBlock_MonotonicSingle(t *testing.T) {
	entries := []shrimptypes.Entry{{Timestamp: 100, Data: "a"}, {Timestamp: 200, Data: ""}, {Timestamp: 300, Data: "bcd"}}
	buf := EncodeBinBlock(entries, nil)
	bb, err := DecodeBinBlock(buf, 3)
	require.NoError(t, err)
	require.Equal(t, "a", string(bb.DataBytes(0)))
	require.Equal(t, "", string(bb.DataBytes(1)))
	require.Equal(t, "bcd", string(bb.DataBytes(2)))
}

func TestDecodeBinBlock_AllOffsetsZeroPanic(t *testing.T) {
	buf := make([]byte, 1*8+(1+1)*4)
	binary.LittleEndian.PutUint32(buf[8:], 0)
	binary.LittleEndian.PutUint32(buf[12:], 0)
	bb, err := DecodeBinBlock(buf, 1)
	require.NoError(t, err)
	require.Empty(t, bb.DataBytes(0))
}
