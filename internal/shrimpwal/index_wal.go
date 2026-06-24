package shrimpwal

import (
	"github.com/go-faster/jx"
)

// IndexWAL is the segmented write-ahead log for pre-flush index entries.
// It shares the seal/discard segment machinery with WAL; see WAL for the flush
// protocol and durability invariant.
type IndexWAL struct {
	sl *segLog
}

// OpenIndexWAL opens (or creates) the segmented index write-ahead log at path.
// "<dir>/index-wal.jsonl" yields segments "<dir>/index-wal-NNNNNN.jsonl".
func OpenIndexWAL(path string) (*IndexWAL, error) {
	sl, err := openSegLog(path)
	if err != nil {
		return nil, err
	}
	return &IndexWAL{sl: sl}, nil
}

// Enqueue encodes entries and adds them to the pending group-commit batch
// without waiting for the fsync. Callers must Wait on the returned Commit.
func (w *IndexWAL) Enqueue(entries []IndexEntry) *Commit {
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	for _, e := range entries {
		jw.ObjStart()
		jw.RawStr(`"token":`)
		jw.Str(e.Token)
		jw.RawStr(`,"data_id":`)
		jw.Str(e.DataID)
		jw.ObjEnd()
		jw.Buf = append(jw.Buf, '\n')
	}
	return &Commit{sl: w.sl, c: w.sl.enqueue(jw.Buf)}
}

// Append writes entries to the active segment and fsyncs before returning.
// Concurrent Appends batch their fsyncs via group commit.
func (w *IndexWAL) Append(entries []IndexEntry) error {
	return w.Enqueue(entries).Wait()
}

// Seal closes the active segment and opens a fresh one, returning the sealed
// sequence number for a later Discard.
func (w *IndexWAL) Seal() (uint64, error) { return w.sl.seal() }

// Discard removes sealed segments with sequence number <= uptoSeq.
func (w *IndexWAL) Discard(uptoSeq uint64) error { return w.sl.discard(uptoSeq) }

// Recover reads all entries from every segment, oldest first. Called once on
// startup. Skips corrupt lines silently.
func (w *IndexWAL) Recover() ([]IndexEntry, error) {
	var entries []IndexEntry
	err := w.sl.forEachLine(func(line []byte) {
		var e IndexEntry
		d := jx.DecodeBytes(line)
		if derr := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
			switch string(key) {
			case "token":
				v, err := d.Str()
				if err != nil {
					return err
				}
				e.Token = v
			case "data_id":
				v, err := d.Str()
				if err != nil {
					return err
				}
				e.DataID = v
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
func (w *IndexWAL) Close() error { return w.sl.close() }
