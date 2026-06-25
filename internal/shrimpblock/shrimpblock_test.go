package shrimpblock

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/blevesearch/vellum"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

func openFSTForTest(t *testing.T, path string, start, end []byte) (*vellum.FSTIterator, error) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f, err := vellum.Load(data)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = f.Close() })
	return f.Iterator(start, end)
}

func TestWriteBlockZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := shrimptypes.Block{
		SourceReplica: "node1",
		Data: []shrimptypes.Entry{
			{Timestamp: 1, Data: "hello"},
			{Timestamp: 2, Data: "world"},
		},
	}
	path := filepath.Join(dir, "parts", "test.json")
	require.NoError(t, WriteBlock(path, block, CompressionZstd))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, data[:4], "zstd frame magic on disk")

	got, err := ReadBlock(path)
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestWriteBlockPlainRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := shrimptypes.Block{Data: []shrimptypes.Entry{{Timestamp: 7, Data: "plain"}}}
	path := filepath.Join(dir, "parts", "plain.json")
	require.NoError(t, WriteBlock(path, block, ""))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte('{'), data[0], "plain JSON starts with '{'")

	got, err := ReadBlock(path)
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestReadLocalPartLegacyPlain(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	plain := []byte(`{"data":[{"timestamp":1,"data":"foo"}]}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "parts", "legacy.json"), plain, 0o644))

	got, err := ReadBlock(filepath.Join(dir, "parts", "legacy.json"))
	require.NoError(t, err)
	require.Equal(t, []shrimptypes.Entry{{Timestamp: 1, Data: "foo"}}, got.Data)
}

func TestBuildIndexFSTRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "index"), 0o750))
	entries := []shrimptypes.IndexEntry{
		{Token: "lbl:a=b", DataID: "p1"},
		{Token: "lbl:a=b", DataID: "p2"},
		{Token: "lbl:x=y", DataID: "p1"},
	}
	path := filepath.Join(dir, "index", "test.fst")
	dataIDs, err := BuildIndexFST(path, entries)
	require.NoError(t, err)
	require.Equal(t, []string{"p1", "p2"}, dataIDs)

	// prefix scan: all ordinals for token "lbl:a=b", resolve via dataIDs table
	start := compositeKey("lbl:a=b", 0)
	end := []byte("lbl:a=b\x01")
	itr, err := openFSTForTest(t, path, start, end)
	require.NoError(t, err)
	var got []string
	for {
		k, _ := itr.Current()
		if k == nil {
			break
		}
		sep := bytes.IndexByte(k, '\x00')
		require.True(t, sep >= 0)
		ord := binary.BigEndian.Uint16(k[sep+1:])
		if int(ord) < len(dataIDs) {
			got = append(got, dataIDs[ord])
		}
		if err := itr.Next(); err != nil {
			break
		}
	}
	_ = itr.Close()
	require.Equal(t, []string{"p1", "p2"}, got)
}
