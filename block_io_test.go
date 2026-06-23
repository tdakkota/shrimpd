package shrimpd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteBlockZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := Block{
		SourceReplica: "node1",
		Data: []Entry{
			{Timestamp: 1, Data: "hello"},
			{Timestamp: 2, Data: "world"},
		},
	}
	path := filepath.Join(dir, "parts", "test.json")
	require.NoError(t, writeBlock(path, block, compressionZstd))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, data[:4], "zstd frame magic on disk")

	l := &LSM{dataDir: dir}
	got, err := l.readLocalPart("test")
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestWriteBlockPlainRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	block := Block{Data: []Entry{{Timestamp: 7, Data: "plain"}}}
	path := filepath.Join(dir, "parts", "plain.json")
	require.NoError(t, writeBlock(path, block, ""))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, byte('{'), data[0], "plain JSON starts with '{'")

	l := &LSM{dataDir: dir}
	got, err := l.readLocalPart("plain")
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestReadLocalPartLegacyPlain(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o750))
	plain := []byte(`{"data":[{"timestamp":1,"data":"foo"}]}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "parts", "legacy.json"), plain, 0o644))

	l := &LSM{dataDir: dir}
	got, err := l.readLocalPart("legacy")
	require.NoError(t, err)
	require.Equal(t, []Entry{{Timestamp: 1, Data: "foo"}}, got.Data)
}

func TestWriteIndexBlockZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "index"), 0o750))
	block := IndexBlock{Entries: []IndexEntry{
		{Token: "hello", DataID: "p1"},
		{Token: "world", DataID: "p1"},
	}}
	path := filepath.Join(dir, "index", "test.json")
	require.NoError(t, writeIndexBlock(path, block, compressionZstd))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, data[:4], "zstd frame magic on disk")

	got, err := readIndexBlock(path)
	require.NoError(t, err)
	require.Equal(t, block, got)
}

func TestReadIndexBlockLegacyPlain(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "index"), 0o750))
	plain := []byte(`{"entries":[{"token":"hello","data_id":"p1"}]}`)
	path := filepath.Join(dir, "index", "legacy.json")
	require.NoError(t, os.WriteFile(path, plain, 0o644))

	got, err := readIndexBlock(path)
	require.NoError(t, err)
	require.Equal(t, IndexBlock{Entries: []IndexEntry{{Token: "hello", DataID: "p1"}}}, got)
}
