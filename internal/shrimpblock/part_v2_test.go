package shrimpblock

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func TestStreamRowBlockMatcher_AllocsOnReject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"body":"hello world"}`},
		{Timestamp: 2, Data: `{"body":"error panic"}`},
		{Timestamp: 3, Data: `{"body":"debug info"}`},
	}
	_, err := WritePartV2(path, entries)
	require.NoError(t, err)

	meta := shrimptypes.PartMeta{FormatVersion: 1}
	pf, err := OpenPartV2(path, meta)
	require.NoError(t, err)
	require.NotNil(t, pf)
	defer pf.Close()

	m, err := shrimpfilter.CompileMatcher([]shrimpfilter.LineFilter{
		{Op: shrimpfilter.OpLineRe, Value: "panic"},
	}, nil)
	require.NoError(t, err)

	// Warm up
	var got int
	_ = StreamRowBlockMatcher(pf, 0, 0, 1<<62, m, func(e shrimptypes.Entry) error {
		got++
		return nil
	})

	// Measure allocs for a matcher that rejects most lines.
	// Per-block decode allocates (zstd + compressed/decompressed buffers + BinBlock),
	// but rejected rows must not allocate Go strings for data or labels.
	// With binblock decode (unsafe.Slice path) the per-block overhead is minimal.
	allocs := testing.AllocsPerRun(100, func() {
		_ = StreamRowBlockMatcher(pf, 0, 0, 1<<62, m, func(e shrimptypes.Entry) error { return nil })
	})
	if allocs > 15 {
		t.Fatalf("expected <=15 allocs per call for rejected-heavy matcher (block decode), got %v", allocs)
	}

	_ = got // silence
}

func TestStreamRowBlockMatcher_MatchesLabels(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p2.json")
	// Data must be OTLP-flattened JSON so ExtractLabels can parse labels.
	// resource.service.name -> service_name ; severity_text -> level
	entries := []shrimptypes.Entry{
		{Timestamp: 10, Data: `{"severity_text":"INFO","body":"ok","resource":{"service.name":"svc-a"}}`},
		{Timestamp: 11, Data: `{"severity_text":"ERROR","body":"boom","resource":{"service.name":"svc-b"}}`},
	}
	_, err := WritePartV2(path, entries)
	require.NoError(t, err)

	meta := shrimptypes.PartMeta{FormatVersion: 1}
	pf, err := OpenPartV2(path, meta)
	require.NoError(t, err)
	defer pf.Close()

	// Match level=ERROR and service_name contains "b" via label eq.
	m, err := shrimpfilter.CompileMatcher(nil, []shrimpfilter.LabelFilter{
		{Label: "level", Op: shrimpfilter.OpLabelEq, Value: "ERROR"},
		{Label: "service_name", Op: shrimpfilter.OpLabelEq, Value: "svc-b"},
	})
	require.NoError(t, err)

	var out []shrimptypes.Entry
	require.NoError(t, StreamRowBlockMatcher(pf, 0, 0, 1<<62, m, func(e shrimptypes.Entry) error {
		out = append(out, e)
		return nil
	}))
	require.Len(t, out, 1)
	require.Equal(t, int64(11), out[0].Timestamp)
}

func TestStreamRowBlockMatcher_EmptyMatchesAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p3.json")
	entries := []shrimptypes.Entry{{Timestamp: 1, Data: "x"}, {Timestamp: 2, Data: "y"}}
	_, err := WritePartV2(path, entries)
	require.NoError(t, err)

	pf, _ := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	defer pf.Close()

	var got int
	require.NoError(t, StreamRowBlockMatcher(pf, 0, 0, 1<<62, shrimpfilter.Matcher{}, func(e shrimptypes.Entry) error {
		got++
		return nil
	}))
	require.Equal(t, 2, got)
}

func TestVerifyPartV2RejectsCorruptBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p4.json")
	entries := []shrimptypes.Entry{{Timestamp: 1, Data: "x"}, {Timestamp: 2, Data: "y"}}
	_, err := WritePartV2(path, entries)
	require.NoError(t, err)

	pf, err := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pf.Close()

	require.NoError(t, VerifyPartV2(pf))

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x00}, pf.Headers[0].Offset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.Error(t, VerifyPartV2(pf))
}

func TestMergeParts(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "p1.json"),
		filepath.Join(dir, "p2.json"),
		filepath.Join(dir, "p3.json"),
	}
	inputs := [][]shrimptypes.Entry{
		{{Timestamp: 1, Data: "a1"}, {Timestamp: 4, Data: "a4"}},
		{{Timestamp: 2, Data: "b2"}, {Timestamp: 5, Data: "b5"}},
		{{Timestamp: 3, Data: "c3"}, {Timestamp: 6, Data: "c6"}},
	}
	parts := make([]*PartFileV2, 0, len(paths))
	for i, path := range paths {
		_, err := WritePartV2(path, inputs[i])
		require.NoError(t, err)
		pf, err := OpenPartV2(path, shrimptypes.PartMeta{FormatVersion: 1})
		require.NoError(t, err)
		parts = append(parts, pf)
	}

	var got []int64
	for e, err := range MergeParts(parts) {
		require.NoError(t, err)
		got = append(got, e.Timestamp)
	}
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, got)
}

func TestWritePartV2FromIter(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.json")
	pathB := filepath.Join(dir, "b.json")
	entries := []shrimptypes.Entry{
		{Timestamp: 1, Data: `{"body":"hello world"}`},
		{Timestamp: 2, Data: `{"body":"error panic"}`},
		{Timestamp: 3, Data: `{"body":"debug info"}`},
		{Timestamp: 4, Data: `{"body":"service name"}`},
	}

	_, err := WritePartV2(pathA, entries)
	require.NoError(t, err)
	_, err = WritePartV2Seq(pathB, func(yield func(shrimptypes.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
	}, 2, nil)
	require.NoError(t, err)

	pfA, err := OpenPartV2(pathA, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pfA.Close()
	pfB, err := OpenPartV2(pathB, shrimptypes.PartMeta{FormatVersion: 1})
	require.NoError(t, err)
	defer pfB.Close()

	gotA := make([]shrimptypes.Entry, 0, len(entries))
	for i := range pfA.Headers {
		bb, err := ReadBinBlock(pfA, i)
		require.NoError(t, err)
		for j := range bb.TS {
			gotA = append(gotA, shrimptypes.Entry{Timestamp: bb.TS[j], Data: string(bb.DataBytes(j))})
		}
	}
	gotB := make([]shrimptypes.Entry, 0, len(entries))
	for i := range pfB.Headers {
		bb, err := ReadBinBlock(pfB, i)
		require.NoError(t, err)
		for j := range bb.TS {
			gotB = append(gotB, shrimptypes.Entry{Timestamp: bb.TS[j], Data: string(bb.DataBytes(j))})
		}
	}
	require.True(t, slices.Equal(gotA, gotB))
	require.Equal(t, entries, gotB)
}
