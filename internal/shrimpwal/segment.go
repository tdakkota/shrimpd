package shrimpwal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
)

// segLog is a generic segmented append-only log shared by WAL and IndexWAL.
//
// It manages segment files named "<prefix>-NNNNNN.jsonl" under a directory and
// provides crash-safe seal/discard semantics; it does not interpret line
// contents. The typed wrappers encode/decode each line.
//
// Flush protocol:
//
//	seq, _ := s.seal()   // close active segment, open a fresh one
//	... persist data elsewhere (slow); no seg lock held ...
//	s.discard(seq)       // delete the now-redundant sealed segments
//
// recover replays every segment, so a crash between seal and discard simply
// replays the sealed lines. The caller's invariant: the live in-memory set
// equals the union of all not-yet-discarded segments.
type segLog struct {
	mu        sync.Mutex
	dir       string
	prefix    string
	active    *os.File
	activeSeq uint64
}

// openSegLog opens (or creates) a segmented log rooted at path. The directory
// and base name of path determine where segments live and how they are named:
// "<dir>/wal.jsonl" yields segments "<dir>/wal-NNNNNN.jsonl". A pre-existing
// single-file log at path (legacy, pre-segments) is migrated into segment 0.
func openSegLog(path string) (*segLog, error) {
	dir := filepath.Dir(path)
	prefix := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)) // wal.jsonl -> wal
	if prefix == "" {
		return nil, fmt.Errorf("shrimpwal: cannot derive segment prefix from %q", path)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	s := &segLog{dir: dir, prefix: prefix}

	// Migrate a legacy single-file log into segment 0 so its entries are still
	// recovered and eventually discarded after the next flush.
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		legacy := s.segPath(0)
		if _, statErr := os.Stat(legacy); os.IsNotExist(statErr) {
			if err := os.Rename(path, legacy); err != nil {
				return nil, fmt.Errorf("migrate legacy log: %w", err)
			}
			if err := fsyncutil.SyncDir(dir); err != nil {
				return nil, err
			}
		}
	}

	seqs, err := s.listSegments()
	if err != nil {
		return nil, err
	}
	// Reuse the highest existing segment as the active one (it may hold unflushed
	// records from a previous run); otherwise start at segment 1.
	seq := uint64(1)
	if len(seqs) > 0 {
		seq = seqs[len(seqs)-1]
	}

	// #nosec G304 -- the daemon intentionally opens its configured local data path.
	f, err := os.OpenFile(s.segPath(seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := fsyncutil.SyncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	s.active = f
	s.activeSeq = seq
	return s, nil
}

func (s *segLog) segPath(seq uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%06d.jsonl", s.prefix, seq))
}

// listSegments returns the sequence numbers of all segments on disk, ascending.
func (s *segLog) listSegments() ([]uint64, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	pfx := s.prefix + "-"
	var seqs []uint64
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, pfx) || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, pfx), ".jsonl")
		seq, err := strconv.ParseUint(mid, 10, 64)
		if err != nil {
			continue // not one of ours (e.g. a different prefix sharing the dir)
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// append writes buf to the active segment and fsyncs before returning.
func (s *segLog) append(buf []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.active.Write(buf); err != nil {
		return err
	}
	return s.active.Sync()
}

// seal fsyncs and closes the active segment, then opens a fresh one. It returns
// the sequence number of the sealed segment; pass it to discard once the
// corresponding data is durable elsewhere.
func (s *segLog) seal() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sealed := s.activeSeq
	if err := s.active.Sync(); err != nil {
		return 0, err
	}
	if err := s.active.Close(); err != nil {
		return 0, err
	}

	next := sealed + 1
	// #nosec G304 -- configured local data path.
	f, err := os.OpenFile(s.segPath(next), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		// Best effort: reopen the sealed segment so the log stays usable.
		// #nosec G304 -- configured local data path.
		if reopened, rerr := os.OpenFile(s.segPath(sealed), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600); rerr == nil {
			s.active = reopened
		}
		return 0, err
	}
	if err := fsyncutil.SyncDir(s.dir); err != nil {
		_ = f.Close()
		return 0, err
	}
	s.active = f
	s.activeSeq = next
	return sealed, nil
}

// discard removes every sealed segment with sequence number <= uptoSeq. The
// active segment is never removed. Safe to call repeatedly: missing files are
// ignored, so a crash between seal and discard converges on retry.
func (s *segLog) discard(uptoSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seqs, err := s.listSegments()
	if err != nil {
		return err
	}
	removed := false
	for _, seq := range seqs {
		if seq > uptoSeq || seq == s.activeSeq {
			continue
		}
		if err := os.Remove(s.segPath(seq)); err != nil && !os.IsNotExist(err) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	return fsyncutil.SyncDir(s.dir)
}

// forEachLine reads every non-empty line from every segment, oldest first, and
// passes it to fn. Corrupt/partial trailing lines are the caller's concern: fn
// decides whether to skip them.
func (s *segLog) forEachLine(fn func(line []byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seqs, err := s.listSegments()
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		// #nosec G304 -- configured local data path.
		f, err := os.Open(s.segPath(seq))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 4<<20), 4<<20) // 4 MiB max line (large log entries)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			fn(line)
		}
		if scErr := sc.Err(); scErr != nil {
			_ = f.Close()
			return scErr
		}
		_ = f.Close()
	}
	return nil
}

// close closes the active segment file.
func (s *segLog) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active.Close()
}
