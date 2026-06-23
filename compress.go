package shrimpd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const compressionZstd = "zstd"

var algoMagic = []struct {
	algo  string
	magic []byte
}{
	{compressionZstd, []byte{0x28, 0xb5, 0x2f, 0xfd}},
	{"gzip", []byte{0x1f, 0x8b}},
}

var encoderPool = sync.Pool{
	New: func() any {
		e, err := zstd.NewWriter(nil)
		if err != nil {
			panic(fmt.Sprintf("zstd new writer: %v", err))
		}
		return e
	},
}

var decoderPool = sync.Pool{
	New: func() any {
		d, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("zstd new reader: %v", err))
		}
		return d
	},
}

type zstdCompressWriter struct {
	enc  *zstd.Encoder
	done bool
}

func (z *zstdCompressWriter) Write(p []byte) (int, error) {
	return z.enc.Write(p)
}

func (z *zstdCompressWriter) Close() error {
	if z.done {
		return nil
	}
	z.done = true
	err := z.enc.Close()
	encoderPool.Put(z.enc)
	return err
}

type zstdDecompressReader struct {
	dec  *zstd.Decoder
	done bool
}

func (z *zstdDecompressReader) Read(p []byte) (int, error) {
	return z.dec.Read(p)
}

func (z *zstdDecompressReader) Close() error {
	if z.done {
		return nil
	}
	z.done = true
	_ = z.dec.Reset(nil)
	decoderPool.Put(z.dec)
	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func detectAlgo(head []byte) string {
	for _, m := range algoMagic {
		if len(head) >= len(m.magic) && bytes.Equal(head[:len(m.magic)], m.magic) {
			return m.algo
		}
	}
	return ""
}

func newCompressingWriter(w io.Writer, algo string) (io.WriteCloser, error) {
	switch algo {
	case compressionZstd:
		enc := encoderPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		return &zstdCompressWriter{enc: enc}, nil
	case "":
		return nopWriteCloser{w}, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %q", algo)
	}
}

func newZstdDecompressReader(r io.Reader) (io.ReadCloser, error) {
	dec := decoderPool.Get().(*zstd.Decoder)
	if err := dec.Reset(r); err != nil {
		decoderPool.Put(dec)
		return nil, fmt.Errorf("zstd reset: %w", err)
	}
	return &zstdDecompressReader{dec: dec}, nil
}

func openBlockReader(r io.Reader) (io.ReadCloser, string, error) {
	br := bufio.NewReaderSize(r, 512)
	head, err := br.Peek(4)
	if err != nil && err != io.EOF {
		return nil, "", fmt.Errorf("peek block header: %w", err)
	}
	switch detectAlgo(head) {
	case compressionZstd:
		dec := decoderPool.Get().(*zstd.Decoder)
		if err := dec.Reset(br); err != nil {
			decoderPool.Put(dec)
			return nil, "", fmt.Errorf("zstd reset: %w", err)
		}
		return &zstdDecompressReader{dec: dec}, compressionZstd, nil
	case "gzip":
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, "", fmt.Errorf("gzip new reader: %w", err)
		}
		return gz, "gzip", nil
	default:
		return io.NopCloser(br), "", nil
	}
}
