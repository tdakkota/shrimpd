package shrimpd

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectAlgo(t *testing.T) {
	cases := []struct {
		name string
		head []byte
		want string
	}{
		{"zstd magic", []byte{0x28, 0xb5, 0x2f, 0xfd}, compressionZstd},
		{"zstd magic with trailing", []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x01}, compressionZstd},
		{"gzip magic", []byte{0x1f, 0x8b, 0x08, 0x00}, "gzip"},
		{"gzip magic exact", []byte{0x1f, 0x8b}, "gzip"},
		{"json object", []byte{'{', '}', 0x00, 0x00}, ""},
		{"json array", []byte{'[', ']', 0x00, 0x00}, ""},
		{"empty", []byte{}, ""},
		{"short zstd prefix", []byte{0x28}, ""},
		{"short gzip prefix", []byte{0x1f}, ""},
		{"plain text", []byte("hello world"), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, detectAlgo(c.head))
		})
	}
}

func TestNewCompressingWriterPassthrough(t *testing.T) {
	var buf bytes.Buffer
	cw, err := newCompressingWriter(&buf, "")
	require.NoError(t, err)
	payload := []byte(`{"hello":"world"}`)
	_, err = cw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.Equal(t, payload, buf.Bytes())
}

func TestNewCompressingWriterUnknownAlgo(t *testing.T) {
	_, err := newCompressingWriter(&bytes.Buffer{}, "brotli")
	require.Error(t, err)
}

func TestZstdRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte(`{"data":[{"timestamp":1,"data":"hello world"}]}`), 200)
	var buf bytes.Buffer
	cw, err := newCompressingWriter(&buf, compressionZstd)
	require.NoError(t, err)
	_, err = cw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, cw.Close())

	require.Less(t, buf.Len(), len(payload), "zstd should compress the repetitive payload")
	require.Equal(t, []byte{0x28, 0xb5, 0x2f, 0xfd}, buf.Bytes()[:4], "zstd frame magic")

	rc, algo, err := openBlockReader(&buf)
	require.NoError(t, err)
	require.Equal(t, compressionZstd, algo)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestOpenBlockReaderPlainJSON(t *testing.T) {
	payload := []byte(`{"data":[{"timestamp":1,"data":"foo"}]}`)
	rc, algo, err := openBlockReader(bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, "", algo)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestOpenBlockReaderGzip(t *testing.T) {
	payload := []byte(`{"entries":[{"token":"hello","data_id":"p1"}]}`)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(payload)
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	rc, algo, err := openBlockReader(&buf)
	require.NoError(t, err)
	require.Equal(t, "gzip", algo)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestEncoderPoolConcurrent(t *testing.T) {
	payload := bytes.Repeat([]byte("compress me compress me compress me"), 500)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			var buf bytes.Buffer
			cw, err := newCompressingWriter(&buf, compressionZstd)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := cw.Write(payload); err != nil {
				t.Error(err)
				_ = cw.Close()
				return
			}
			if err := cw.Close(); err != nil {
				t.Error(err)
				return
			}
			rc, _, err := openBlockReader(&buf)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(got, payload) {
				t.Error("payload mismatch after round-trip")
			}
		})
	}
	wg.Wait()
}
