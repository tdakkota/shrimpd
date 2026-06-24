package shrimpblock

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/tdakkota/shrimpd/internal/shrimpfilter"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"

	"github.com/klauspost/compress/zstd"
)

const (
	MagicShrimp = "SHMP"
	v2Version   = 0x02

	v2HeaderSize  = 16   // 4 + 1 + 3 + 8
	v2BlockDirRow = 1096 // per-block directory entry size
	v2BlockRows   = 512  // default rows per block
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
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-v2-")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	name := tmp.Name()

	blockCount := (len(entries) + v2BlockRows - 1) / v2BlockRows
	if blockCount == 0 {
		blockCount = 1
	}
	blocks := make([][]shrimptypes.Entry, 0, blockCount)
	for i := 0; i < len(entries); i += v2BlockRows {
		end := min(i+v2BlockRows, len(entries))
		blocks = append(blocks, entries[i:end])
	}

	headers := make([]shrimptypes.BlockHeader, len(blocks))

	// Write magic header placeholder
	headerBuf := make([]byte, v2HeaderSize)
	copy(headerBuf[0:4], MagicShrimp)
	headerBuf[4] = v2Version
	// reserved: bytes 5-7 are zero
	binary.LittleEndian.PutUint64(headerBuf[8:16], uint64(len(blocks)))

	if _, err := tmp.Write(headerBuf); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write header: %w", err)
	}

	// Write block directory placeholder
	dirOffset := int64(v2HeaderSize)
	dirSize := int64(len(blocks)) * v2BlockDirRow
	if _, err := tmp.Write(make([]byte, dirSize)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write dir placeholder: %w", err)
	}

	// Write each block
	payloadOffset := dirOffset + dirSize
	enc := encoderPool.Get().(*zstd.Encoder)
	defer encoderPool.Put(enc)

	var binBuf []byte

	for bi, block := range blocks {
		binBuf = EncodeBinBlock(block, binBuf[:0])

		enc.Reset(tmp)
		if _, err := enc.Write(binBuf); err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, fmt.Errorf("compress block: %w", err)
		}
		if err := enc.Close(); err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, fmt.Errorf("close zstd: %w", err)
		}
		compressedEnd, _ := tmp.Seek(0, io.SeekCurrent)
		compressedSz := compressedEnd - payloadOffset

		// Build bloom filter from block entries
		var bloom shrimptypes.BloomFilter
		for _, e := range block {
			for tok := range Tokenize(e.Data) {
				BloomAdd(&bloom, tok)
			}
			labels := shrimpfilter.ExtractLabels(e.Data)
			for k, v := range labels {
				BloomAddLabel(&bloom, k, v)
			}
		}

		headers[bi] = shrimptypes.BlockHeader{
			Offset:       payloadOffset,
			CompressedSz: compressedSz,
			Count:        int32(len(block)),
			MinTimestamp: block[0].Timestamp,
			MaxTimestamp: block[len(block)-1].Timestamp,
			Bloom:        bloom,
		}

		payloadOffset = compressedEnd
	}

	// Rewrite block directory
	dirBuf := make([]byte, dirSize)
	for bi, h := range headers {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		binary.LittleEndian.PutUint64(row[0:8], uint64(h.Offset))
		binary.LittleEndian.PutUint64(row[8:16], uint64(h.CompressedSz))
		binary.LittleEndian.PutUint32(row[16:20], uint32(h.Count))
		binary.LittleEndian.PutUint64(row[20:28], uint64(h.MinTimestamp))
		binary.LittleEndian.PutUint64(row[28:36], uint64(h.MaxTimestamp))
		copy(row[36:1060], h.Bloom[:])
	}

	if _, err := tmp.WriteAt(dirBuf, dirOffset); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("write dir: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	if err := os.Rename(name, path); err != nil {
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

// ReadRowBlock pread-fetches exactly hdr.CompressedSz bytes at hdr.Offset,
// decompresses, decodes binary block into RowBlock.
func ReadRowBlock(pf *PartFileV2, idx int) (*shrimptypes.RowBlock, error) {
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

	decoded, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", idx, err)
	}

	bb, err := DecodeBinBlock(decoded, int(hdr.Count))
	if err != nil {
		return nil, fmt.Errorf("decode block %d: %w", idx, err)
	}

	data := make([]string, hdr.Count)
	for i := range bb.TS {
		data[i] = bb.Data(i)
	}

	return &shrimptypes.RowBlock{
		Timestamps: bb.TS,
		Data:       data,
		Cost:       uint32(len(decoded)),
	}, nil
}

// VerifyPartV2 fully decodes every block in the part.
func VerifyPartV2(pf *PartFileV2) error {
	for i := range pf.Headers {
		if _, err := ReadRowBlock(pf, i); err != nil {
			return fmt.Errorf("verify block %d: %w", i, err)
		}
	}
	return nil
}

// StreamRowBlock decompresses block idx and calls fn for each entry that passes
// the timestamp range and term filter. No RowBlock is built and no result slice
// is accumulated: only one string is allocated per matching entry.
//
// The decoded buffer is interpreted as a BinBlock, providing zero-alloc DataBytes
// access for filter matching. Strings are only materialized for survivors.
func StreamRowBlock(pf *PartFileV2, idx int, from, to int64, term string, fn func(shrimptypes.Entry) error) error {
	if idx < 0 || idx >= len(pf.Headers) {
		return fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return fmt.Errorf("read block %d: %w", idx, err)
	}

	dec := decoderPool.Get().(*zstd.Decoder)
	decoded, err := dec.DecodeAll(compressed, nil)
	_ = dec.Reset(nil)
	decoderPool.Put(dec)
	if err != nil {
		return fmt.Errorf("decompress block %d: %w", idx, err)
	}

	bb, err := DecodeBinBlock(decoded, int(hdr.Count))
	if err != nil {
		return fmt.Errorf("decode block %d: %w", idx, err)
	}

	for i := range bb.TS {
		ts := bb.TS[i]
		if ts < from || ts > to {
			continue
		}
		sb := bb.DataBytes(i)
		if term != "" && !shrimptypes.FoldContains(sb, term) {
			continue
		}
		if err := fn(shrimptypes.Entry{Timestamp: ts, Data: string(sb)}); err != nil {
			return err
		}
	}
	return nil
}

// StreamRowBlockMatcher is like StreamRowBlock but applies a shrimpfilter.Matcher
// as a post-filter. Line filters run on DataBytes subslice; only survivors allocate
// via string(sb) and then run label extraction + MatchLabels.
func StreamRowBlockMatcher(pf *PartFileV2, idx int, from, to int64, m shrimpfilter.Matcher, fn func(shrimptypes.Entry) error) error {
	if idx < 0 || idx >= len(pf.Headers) {
		return fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return fmt.Errorf("read block %d: %w", idx, err)
	}

	dec := decoderPool.Get().(*zstd.Decoder)
	decoded, err := dec.DecodeAll(compressed, nil)
	_ = dec.Reset(nil)
	decoderPool.Put(dec)
	if err != nil {
		return fmt.Errorf("decompress block %d: %w", idx, err)
	}

	bb, err := DecodeBinBlock(decoded, int(hdr.Count))
	if err != nil {
		return fmt.Errorf("decode block %d: %w", idx, err)
	}

	for i := range bb.TS {
		ts := bb.TS[i]
		if ts < from || ts > to {
			continue
		}
		sb := bb.DataBytes(i)
		if !m.MatchLineBytes(sb) {
			continue
		}
		s := string(sb)
		if !m.Empty() && len(m.Labels) > 0 {
			labels := shrimpfilter.ExtractLabels(s)
			if !m.MatchLabels(labels) {
				continue
			}
		}
		if err := fn(shrimptypes.Entry{Timestamp: ts, Data: s}); err != nil {
			return err
		}
	}
	return nil
}

// V2ToBlock converts a V2 part file to a legacy Block for backward-compatible
// remote serving. It reads and merges all blocks.
func V2ToBlock(pf *PartFileV2) (shrimptypes.Block, error) {
	var entries []shrimptypes.Entry
	for i := range pf.Headers {
		rb, err := ReadRowBlock(pf, i)
		if err != nil {
			return shrimptypes.Block{}, err
		}
		for j := range rb.Timestamps {
			entries = append(entries, shrimptypes.Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]})
		}
	}
	slices.SortFunc(entries, func(a, b shrimptypes.Entry) int {
		return int(a.Timestamp - b.Timestamp)
	})
	return shrimptypes.Block{
		SourceReplica: pf.Meta.NodeID,
		CreatedAt:     time.Now().UnixNano(),
		Data:          entries,
	}, nil
}
