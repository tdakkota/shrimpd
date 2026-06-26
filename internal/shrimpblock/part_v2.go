package shrimpblock

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/oteldb/shrimpd/internal/fsyncutil"
	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimptypes"

	"github.com/klauspost/compress/zstd"
)

const (
	MagicShrimp = "SHMP"
	v2Version   = 0x02

	v2HeaderSize  = 16   // 4 + 1 + 3 + 8
	v2BlockDirRow = 1096 // per-block directory entry size
	v2BlockRows   = 512  // default rows per block

	// DefaultV2BlockRows is the default streaming block size for callers outside
	// this package.
	DefaultV2BlockRows = v2BlockRows
)

// PartFileV2 holds an open file descriptor and its block directory in memory.
type PartFileV2 struct {
	Meta    shrimptypes.PartMeta
	Headers []shrimptypes.BlockHeader
	Version byte
	fd      *os.File
}

// WritePartV2 splits entries into n-row blocks, builds bloom per block,
// compresses each block independently, and writes the header + directory + data.
// Returns the written headers.
func WritePartV2(path string, entries []shrimptypes.Entry) ([]shrimptypes.BlockHeader, error) {
	return WritePartV2Seq(path, entriesSeq(entries), v2BlockRows, nil)
}

func entriesSeq(entries []shrimptypes.Entry) iter.Seq2[shrimptypes.Entry, error] {
	return func(yield func(shrimptypes.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
	}
}

// WritePartV2Seq streams entries into a V2 part without materializing all rows.
func WritePartV2Seq(path string, it iter.Seq2[shrimptypes.Entry, error], blockSize int, cb func([]shrimptypes.Entry) error) ([]shrimptypes.BlockHeader, error) {
	return writePartV2Seq(path, it, blockSize, cb)
}

func writePartV2Seq(path string, it iter.Seq2[shrimptypes.Entry, error], blockSize int, cb func([]shrimptypes.Entry) error) ([]shrimptypes.BlockHeader, error) {
	if blockSize <= 0 {
		blockSize = v2BlockRows
	}

	payloadTmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-v2-payload-")
	if err != nil {
		return nil, fmt.Errorf("create payload temp: %w", err)
	}
	payloadName := payloadTmp.Name()
	defer func() { _ = payloadTmp.Close(); _ = os.Remove(payloadName) }()

	enc := encoderPool.Get().(*zstd.Encoder)
	defer encoderPool.Put(enc)

	var (
		buf     = make([]shrimptypes.Entry, 0, blockSize)
		headers []shrimptypes.BlockHeader
		offset  int64
	)

	flushBlock := func(block []shrimptypes.Entry) error {
		if len(block) == 0 {
			return nil
		}
		if cb != nil {
			if err := cb(block); err != nil {
				return err
			}
		}
		binBuf := EncodeBinBlock(block, nil)
		enc.Reset(payloadTmp)
		if _, err := enc.Write(binBuf); err != nil {
			return fmt.Errorf("compress block: %w", err)
		}
		if err := enc.Close(); err != nil {
			return fmt.Errorf("close zstd: %w", err)
		}
		end, err := payloadTmp.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// Build a label-only bloom filter for this block.
		// Text-token bloom is omitted: a fixed 8192-bit filter with k=4 saturates
		// at ~10 k distinct tokens (FP ≈ 0.97) and prunes nothing on real log data.
		// Label cardinality (lbl:k=v) is far lower so the bloom stays effective.
		// Text-term pruning is handled at the part level via PartMeta.Tokens.
		var bloom shrimptypes.BloomFilter
		for _, e := range block {
			labels := shrimpfilter.ExtractLabels(e.Data)
			for k, v := range labels {
				BloomAddLabel(&bloom, k, v)
			}
		}
		headers = append(headers, shrimptypes.BlockHeader{
			Offset:       offset,
			CompressedSz: end - offset,
			Count:        int32(len(block)),
			MinTimestamp: block[0].Timestamp,
			MaxTimestamp: block[len(block)-1].Timestamp,
			Bloom:        bloom,
		})
		offset = end
		return nil
	}

	for e, err := range it {
		if err != nil {
			return nil, err
		}
		buf = append(buf, e)
		if len(buf) == blockSize {
			if err := flushBlock(buf); err != nil {
				return nil, err
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := flushBlock(buf); err != nil {
			return nil, err
		}
	}

	finalTmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-v2-")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	finalName := finalTmp.Name()
	defer func() { _ = finalTmp.Close(); _ = os.Remove(finalName) }()

	headerBuf := make([]byte, v2HeaderSize)
	copy(headerBuf[0:4], MagicShrimp)
	headerBuf[4] = v2Version
	binary.LittleEndian.PutUint64(headerBuf[8:16], uint64(len(headers)))
	if _, err := finalTmp.Write(headerBuf); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	dirOffset := int64(v2HeaderSize)
	dirSize := int64(len(headers)) * v2BlockDirRow
	if _, err := finalTmp.Write(make([]byte, dirSize)); err != nil {
		return nil, fmt.Errorf("write dir placeholder: %w", err)
	}

	if _, err := payloadTmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.Copy(finalTmp, payloadTmp); err != nil {
		return nil, err
	}

	dirBuf := make([]byte, dirSize)
	baseOffset := dirOffset + dirSize
	for bi, h := range headers {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		binary.LittleEndian.PutUint64(row[0:8], uint64(baseOffset+h.Offset))
		binary.LittleEndian.PutUint64(row[8:16], uint64(h.CompressedSz))
		binary.LittleEndian.PutUint32(row[16:20], uint32(h.Count))
		binary.LittleEndian.PutUint64(row[20:28], uint64(h.MinTimestamp))
		binary.LittleEndian.PutUint64(row[28:36], uint64(h.MaxTimestamp))
		copy(row[36:1060], h.Bloom[:])
	}
	if _, err := finalTmp.WriteAt(dirBuf, dirOffset); err != nil {
		return nil, fmt.Errorf("write dir: %w", err)
	}
	if err := finalTmp.Sync(); err != nil {
		return nil, err
	}
	if err := finalTmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(finalName, path); err != nil {
		return nil, err
	}
	if err := fsyncutil.SyncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}

	return headers, nil
}

// OpenPartV2 reads the magic, block directory, and returns a PartFileV2.
// Returns an error if magic is missing or the file cannot be read.
func OpenPartV2(path string, meta shrimptypes.PartMeta) (*PartFileV2, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal part path
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	br := bufio.NewReaderSize(f, 512)
	head, err := br.Peek(4)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if string(head) != MagicShrimp {
		_ = f.Close()
		return nil, fmt.Errorf("bad magic in part %s: got %q, want %q", path, string(head), MagicShrimp)
	}

	// Read header
	var hdrBuf [v2HeaderSize]byte
	if _, err := io.ReadFull(br, hdrBuf[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read v2 header: %w", err)
	}

	version := hdrBuf[4]
	if version != v2Version {
		_ = f.Close()
		return nil, fmt.Errorf("unsupported v2 version: got 0x%02x, want 0x%02x", version, v2Version)
	}

	blockCount := int(binary.LittleEndian.Uint64(hdrBuf[8:16]))

	// Read block directory
	dirSize := blockCount * v2BlockDirRow
	dirBuf := make([]byte, dirSize)
	if _, err := io.ReadFull(br, dirBuf); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read block dir: %w", err)
	}

	headers := make([]shrimptypes.BlockHeader, blockCount)
	for bi := range blockCount {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		headers[bi] = shrimptypes.BlockHeader{
			Offset:       int64(binary.LittleEndian.Uint64(row[0:8])),
			CompressedSz: int64(binary.LittleEndian.Uint64(row[8:16])),
			Count:        int32(binary.LittleEndian.Uint32(row[16:20])),
			MinTimestamp: int64(binary.LittleEndian.Uint64(row[20:28])),
			MaxTimestamp: int64(binary.LittleEndian.Uint64(row[28:36])),
		}
		copy(headers[bi].Bloom[:], row[36:1060])
	}

	return &PartFileV2{
		Meta:    meta,
		Headers: headers,
		Version: version,
		fd:      f,
	}, nil
}

// Close closes the underlying file descriptor.
func (pf *PartFileV2) Close() error {
	return pf.fd.Close()
}

// ReadBinBlock pread-fetches exactly hdr.CompressedSz bytes at hdr.Offset,
// decompresses, and returns a BinBlock view over the decoded buffer.
func ReadBinBlock(pf *PartFileV2, idx int) (*BinBlock, error) {
	if idx < 0 || idx >= len(pf.Headers) {
		return nil, fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return nil, fmt.Errorf("read block %d: %w", idx, err)
	}

	dec := decoderPool.Get().(*zstd.Decoder)
	defer func() {
		_ = dec.Reset(nil)
		decoderPool.Put(dec)
	}()
	if err := dec.ResetWithOptions(nil, zstd.WithDecoderConcurrency(1)); err != nil {
		return nil, fmt.Errorf("reset zstd decoder: %w", err)
	}

	decoded, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", idx, err)
	}

	bb, err := DecodeBinBlock(decoded, int(hdr.Count))
	if err != nil {
		return nil, fmt.Errorf("decode block %d: %w", idx, err)
	}
	return &bb, nil
}

// VerifyPartV2 fully decodes every block in the part.
func VerifyPartV2(pf *PartFileV2) error {
	for i := range pf.Headers {
		if _, err := ReadBinBlock(pf, i); err != nil {
			return fmt.Errorf("verify block %d: %w", i, err)
		}
	}
	return nil
}

// StreamRowBlock decompresses block idx and calls fn for each entry that passes
// the timestamp range and term filter. Delegates to ReadBinBlock + BinBlock.Iterate.
func StreamRowBlock(pf *PartFileV2, idx int, from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	bb, err := ReadBinBlock(pf, idx)
	if err != nil {
		return err
	}
	return bb.Iterate(from, to, term, func(ts int64, data []byte) error {
		return fn(shrimptypes.Entry{Timestamp: ts, Data: string(data)})
	})
}

// StreamRowBlockMatcher is like StreamRowBlock but applies a shrimpfilter.Matcher
// as a post-filter. Delegates to ReadBinBlock + BinBlock.IterateMatcher.
func StreamRowBlockMatcher(pf *PartFileV2, idx int, from, to int64, m shrimpfilter.Matcher, fn func(shrimptypes.Entry) error) error {
	bb, err := ReadBinBlock(pf, idx)
	if err != nil {
		return err
	}
	return bb.IterateMatcher(from, to, m, func(ts int64, data []byte) error {
		return fn(shrimptypes.Entry{Timestamp: ts, Data: string(data)})
	})
}
