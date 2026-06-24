// Package shrimpwal implements a segmented write-ahead log for the shrimpd project.
package shrimpwal

import (
	"github.com/go-faster/jx"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

type (
	Entry      = shrimptypes.Entry
	IndexEntry = shrimptypes.IndexEntry
)

// WAL is the per-node, segmented write-ahead log for pre-flush durability.
//
// Records are appended (NDJSON, one Entry per line) to the active segment and
// fsynced before Append returns. A flush proceeds as:
//
//	seq, _ := wal.Seal()                  // close active segment, open a fresh one
//	... write part file, publish to etcd  // slow; NO wal lock held
//	wal.Discard(seq)                      // delete the now-redundant sealed segments
//
// Because Seal redirects subsequent writes to a brand-new segment, the heavy
// flush work (disk + etcd) runs without blocking concurrent Append. Recover
// replays every segment, so a crash between Seal and Discard simply replays the
// sealed entries on the next startup. The invariant the caller must preserve is:
// the live in-memory set equals the union of every not-yet-discarded segment.
type WAL struct {
	sl *segLog
}

// OpenWAL opens (or creates) the segmented write-ahead log rooted at path.
// "<dir>/wal.jsonl" yields segments "<dir>/wal-NNNNNN.jsonl".
func OpenWAL(path string) (*WAL, error) {
	sl, err := openSegLog(path)
	if err != nil {
		return nil, err
	}
	return &WAL{sl: sl}, nil
}

// Append writes entries to the active segment and fsyncs before returning.
// All entries are encoded into a single pooled buffer and written in one syscall.
func (w *WAL) Append(entries []Entry) error {
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	for _, e := range entries {
		jw.ObjStart()
		jw.RawStr(`"timestamp":`)
		jw.Int64(e.Timestamp)
		jw.RawStr(`,"data":`)
		jw.Str(e.Data)
		jw.ObjEnd()
		jw.Buf = append(jw.Buf, '\n')
	}
	return w.sl.append(jw.Buf)
}

// Seal closes the active segment and opens a fresh one, returning the sealed
// sequence number for a later Discard.
func (w *WAL) Seal() (uint64, error) { return w.sl.seal() }

// Discard removes sealed segments with sequence number <= uptoSeq.
func (w *WAL) Discard(uptoSeq uint64) error { return w.sl.discard(uptoSeq) }

// Recover reads all entries from every segment, oldest first. Called once on
// startup. Skips corrupt lines silently — they indicate a mid-write crash.
func (w *WAL) Recover() ([]Entry, error) {
	var entries []Entry
	err := w.sl.forEachLine(func(line []byte) {
		var e Entry
		d := jx.DecodeBytes(line)
		if derr := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
			switch string(key) {
			case "timestamp":
				v, err := d.Int64()
				if err != nil {
					return err
				}
				e.Timestamp = v
			case "data":
				v, err := d.Str()
				if err != nil {
					return err
				}
				e.Data = v
			default:
				return d.Skip()
			}
			return nil
		}); derr == nil {
			entries = append(entries, e)
		}
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Close closes the active segment file.
func (w *WAL) Close() error { return w.sl.close() }
