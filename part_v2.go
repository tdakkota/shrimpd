package shrimpd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	magicShrimp = "SHMP"
	v2Version   = 0x01

	v2HeaderSize  = 16 // 4 + 1 + 3 + 8
	v2BlockDirRow = 1096 // per-block directory entry size
	v2BlockRows   = 512  // default rows per block
)

// PartFileV2 holds an open file descriptor and its block directory in memory.
type PartFileV2 struct {
	Meta    PartMeta
	Headers []BlockHeader
	fd      *os.File
}

// writePartV2 splits entries into n-row blocks, builds bloom per block,
// compresses each block independently, and writes the header + directory + data.
// Returns the written headers.
func writePartV2(path string, entries []Entry) ([]BlockHeader, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-v2-")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	name := tmp.Name()

	blockCount := (len(entries) + v2BlockRows - 1) / v2BlockRows
	if blockCount == 0 {
		blockCount = 1
	}
	blocks := make([][]Entry, 0, blockCount)
	for i := 0; i < len(entries); i += v2BlockRows {
		end := i + v2BlockRows
		if end > len(entries) {
			end = len(entries)
		}
		blocks = append(blocks, entries[i:end])
	}

	headers := make([]BlockHeader, len(blocks))

	// Write magic header placeholder
	headerBuf := make([]byte, v2HeaderSize)
	copy(headerBuf[0:4], magicShrimp)
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
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("zstd writer: %w", err)
	}

	for bi, block := range blocks {
		// Build columnar JSON: {"ts":[...],"d":[...]}
		ts := make([]int64, len(block))
		d := make([]string, len(block))
		for i, e := range block {
			ts[i] = e.Timestamp
			d[i] = e.Data
		}
		colJSON, err := json.Marshal(struct {
			Ts []int64  `json:"ts"`
			D  []string `json:"d"`
		}{Ts: ts, D: d})
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, fmt.Errorf("marshal block: %w", err)
		}

		enc.Reset(tmp)
		if _, err := enc.Write(colJSON); err != nil {
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
		var bloom [1024]byte
		for _, e := range block {
			for tok := range tokenize(e.Data) {
				bloomAdd(&bloom, tok)
			}
		}

		headers[bi] = BlockHeader{
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

// openPartV2 reads the magic, block directory, and returns a PartFileV2.
// If the file does not have the V2 magic, returns nil, nil.
func openPartV2(path string, meta PartMeta) (*PartFileV2, error) {
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
	if string(head) != magicShrimp {
		_ = f.Close()
		return nil, nil // legacy JSON file
	}

	// Read header
	var hdrBuf [v2HeaderSize]byte
	if _, err := io.ReadFull(br, hdrBuf[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read v2 header: %w", err)
	}

	blockCount := int(binary.LittleEndian.Uint64(hdrBuf[8:16]))

	// Read block directory
	dirSize := blockCount * v2BlockDirRow
	dirBuf := make([]byte, dirSize)
	if _, err := io.ReadFull(br, dirBuf); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read block dir: %w", err)
	}

	headers := make([]BlockHeader, blockCount)
	for bi := range blockCount {
		row := dirBuf[bi*v2BlockDirRow : (bi+1)*v2BlockDirRow]
		headers[bi] = BlockHeader{
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
		fd:      f,
	}, nil
}

// Close closes the underlying file descriptor.
func (pf *PartFileV2) Close() error {
	return pf.fd.Close()
}

// readRowBlock pread-fetches exactly hdr.CompressedSz bytes at hdr.Offset,
// decompresses, decodes columnar JSON into RowBlock.
func readRowBlock(pf *PartFileV2, idx int) (*RowBlock, error) {
	if idx < 0 || idx >= len(pf.Headers) {
		return nil, fmt.Errorf("block index %d out of range (0-%d)", idx, len(pf.Headers)-1)
	}
	hdr := pf.Headers[idx]

	compressed := make([]byte, hdr.CompressedSz)
	if _, err := pf.fd.ReadAt(compressed, hdr.Offset); err != nil {
		return nil, fmt.Errorf("read block %d: %w", idx, err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()

	decoded, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block %d: %w", idx, err)
	}

	var col struct {
		Ts []int64  `json:"ts"`
		D  []string `json:"d"`
	}
	if err := json.Unmarshal(decoded, &col); err != nil {
		return nil, fmt.Errorf("unmarshal block %d: %w", idx, err)
	}

	return &RowBlock{
		Timestamps: col.Ts,
		Data:       col.D,
	}, nil
}

// v2ToBlock converts a V2 part file to a legacy Block for backward-compatible
// remote serving. It reads and merges all blocks.
func v2ToBlock(pf *PartFileV2) (Block, error) {
	var entries []Entry
	for i := range pf.Headers {
		rb, err := readRowBlock(pf, i)
		if err != nil {
			return Block{}, err
		}
		for j := range rb.Timestamps {
			entries = append(entries, Entry{Timestamp: rb.Timestamps[j], Data: rb.Data[j]})
		}
	}
	slices.SortFunc(entries, func(a, b Entry) int {
		return int(a.Timestamp - b.Timestamp)
	})
	return Block{
		SourceReplica: pf.Meta.NodeID,
		CreatedAt:     time.Now().UnixNano(),
		Data:          entries,
	}, nil
}
