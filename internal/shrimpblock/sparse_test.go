package shrimpblock

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestBuildSparseFromPart(t *testing.T) {
	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: "a"},
		{Timestamp: 2, Data: "b"},
		{Timestamp: 3, Data: "c"},
		{Timestamp: 4, Data: "d"},
	}
	path := t.TempDir() + "/p.json"
	_, err := WritePartV2(path, entries)
	require.NoError(t, err)
	pf, err := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pf.Close()

	sp := BuildSparseFromPart(pf, 2)
	require.Len(t, sp, 2)
	require.Equal(t, int64(1), sp[0].Timestamp)
	require.Equal(t, 0, sp[0].Idx)
	require.Equal(t, int64(3), sp[1].Timestamp)
	require.Equal(t, 2, sp[1].Idx)
}

func TestBuildSparse(t *testing.T) {
	for _, tt := range []struct {
		numEntries int
		by         int
	}{
		{0, 1},
		{100, 1},
		{100, 32},
		{100, 50},
		{100, 60},
		{100, 100},
		{100, 101},
	} {
		ents := make([]shrimptypes.Entry, tt.numEntries)
		for i := range ents {
			ents[i].Timestamp = int64(i)
		}
		sp := BuildSparse(ents, tt.by)

		var expectedLen int
		if tt.numEntries > 0 {
			expectedLen = (tt.numEntries-1)/tt.by + 1
		}
		require.Len(t, sp, expectedLen, "expected %d sparse entries for %d entries with every=%d", expectedLen, tt.numEntries, tt.by)

		for i, s := range sp {
			expectedIdx := i * tt.by
			require.Equal(t, expectedIdx, s.Idx, "sparse entry %d idx (by=%d)", i, tt.by)
			require.Equal(t, int64(expectedIdx), s.Timestamp, "sparse entry %d ts (by=%d)", i, tt.by)
		}
	}
}

func TestSparseRange(t *testing.T) {
	sp := []shrimptypes.SparseEntry{{Timestamp: 0, Idx: 0}, {Timestamp: 10, Idx: 5}, {Timestamp: 20, Idx: 10}}
	lo, hi := SparseRange(sp, 5, 15)
	require.Equal(t, 0, lo)
	require.Equal(t, 10, hi)
}

func TestSparseRangeBounds(t *testing.T) {
	ents := make([]shrimptypes.Entry, 100)
	for i := range ents {
		ents[i] = shrimptypes.Entry{Timestamp: int64(i), Data: fmt.Sprintf("entry-%d", i)}
	}

	sparse := BuildSparse(ents, 32)
	require.Len(t, sparse, 4)
	require.Equal(t, 0, sparse[0].Idx)
	require.Equal(t, int64(0), sparse[0].Timestamp)
	require.Equal(t, 32, sparse[1].Idx)
	require.Equal(t, int64(32), sparse[1].Timestamp)
	require.Equal(t, 64, sparse[2].Idx)
	require.Equal(t, int64(64), sparse[2].Timestamp)
	require.Equal(t, 96, sparse[3].Idx)
	require.Equal(t, int64(96), sparse[3].Timestamp)

	lo, hi := SparseRange(sparse, 10, 50)
	require.Equal(t, 0, lo)
	require.Equal(t, 64, hi)

	lo, hi = SparseRange(sparse, 35, 65)
	require.Equal(t, 32, lo)
	require.Equal(t, 96, hi, "conservative upper bound")

	lo, hi = SparseRange(sparse, 70, 90)
	require.Equal(t, 64, lo)
	require.Equal(t, 96, hi)

	lo, hi = SparseRange([]shrimptypes.SparseEntry{}, 0, 100)
	require.Equal(t, 0, lo)
	require.Equal(t, 1<<31-1, hi)
}
