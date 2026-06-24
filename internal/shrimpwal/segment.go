package shrimpwal

import (
	"bufio"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
)

// syncFile fsyncs a segment file. It is a package var so tests can inject
// latency to exercise group-commit batching deterministically.
var syncFile = (*os.File).Sync

// segLog is a generic segmented append-only log shared by WAL and IndexWAL.
//
// It manages segment files named "<prefix>-NNNNNN.jsonl" under a directory and
// provides crash-safe seal/discard semantics; it does not interpret line
// contents. The typed wrappers encode/decode each line.
//
// # Group commit
//
// Appends are batched: enqueue copies the encoded bytes into an in-memory
// pending buffer (cheap, under mu) and returns a commit handle. The first
// waiter to call commitWait becomes the batch "leader", writes the whole
// pending buffer to the active segment, and fsyncs once for everyone; other
// waiters in that batch return as soon as the leader finishes. While that single
// fsync is in flight, concurrent enqueues accumulate into the next batch, so the
// batch size self-tunes to the fsync latency — no timer and no fixed latency
// floor. This is the standard leader/follower group commit.
//
// # Flush protocol
//
//	seq, _ := s.seal()   // flush pending, close active segment, open a fresh one
//	... persist data elsewhere (slow) ...
//	s.discard(seq)       // delete the now-redundant sealed segments
//
// recover replays every segment, so a crash between seal and discard simply
// replays the sealed lines. The caller's invariant: the live in-memory set
// equals the union of all not-yet-discarded segments.
type segLog struct {
	// ioMu serializes file I/O (the leader's write+fsync, seal, discard, close),
	// so at most one goroutine touches the segment files at a time.
	ioMu sync.Mutex
	// mu protects the in-memory batch state and the active-segment fields.
	mu        sync.Mutex
	dir       string
	prefix    string
	active    *os.File
	activeSeq uint64

	pending []byte  // encoded bytes not yet written+fsynced
	cur     *commit // batch the pending bytes belong to; nil when pending is empty

	syncs atomic.Uint64 // count of fsyncs performed; for tests/observability
}

// commit is a group-commit batch result. done/err are written by the leader and
// read by waiters, both under ioMu.
type commit struct {
	done bool
	err  error
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
// Callers that mutate the segment set (seal, discard) hold ioMu.
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

// enqueue copies buf into the pending batch and returns its commit handle.
// The bytes are durable only after the handle's commitWait returns nil.
func (s *segLog) enqueue(buf []byte) *commit {
	s.mu.Lock()
	if s.cur == nil {
		s.cur = &commit{}
	}
	s.pending = append(s.pending, buf...) // copies buf; the caller may reuse it
	c := s.cur
	s.mu.Unlock()
	return c
}

// commitWait blocks until the batch containing c has been written and fsynced.
// The first waiter for the open batch becomes the leader and does the I/O for
// every member; later members observe c.done and return immediately.
func (s *segLog) commitWait(c *commit) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	if c.done {
		return c.err
	}
	// We hold ioMu and c is not yet flushed, so s.cur is still c: become leader.
	if err := s.flushPending(); err != nil {
		return err
	}
	return c.err
}

// flushPending writes the current pending buffer to the active segment and
// fsyncs once, resolving the batch. Must be called with ioMu held. The pending
// snapshot is taken under mu; the write+fsync runs without mu so concurrent
// enqueues accumulate into the next batch.
func (s *segLog) flushPending() error {
	s.mu.Lock()
	if s.cur == nil {
		s.mu.Unlock()
		return nil
	}
	buf := s.pending
	batch := s.cur
	s.pending = nil
	s.cur = nil
	f := s.active
	s.mu.Unlock()

	_, werr := f.Write(buf)
	var serr error
	if werr == nil {
		serr = s.fsync(f)
	}
	err := cmp.Or(werr, serr)
	batch.err = err
	batch.done = true
	return err
}

// fsync syncs f and counts it.
func (s *segLog) fsync(f *os.File) error {
	s.syncs.Add(1)
	return syncFile(f)
}

// seal flushes any pending bytes to the active segment, closes it, and opens a
// fresh one. It returns the sequence number of the sealed segment; pass it to
// discard once the corresponding data is durable elsewhere.
//
// seal holds mu across the flush+swap so a concurrent enqueue cannot attach
// bytes to a segment that is being retired.
func (s *segLog) seal() (uint64, error) {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush pending to the current active segment before retiring it.
	if s.cur != nil {
		_, werr := s.active.Write(s.pending)
		var serr error
		if werr == nil {
			serr = s.fsync(s.active)
		}
		err := cmp.Or(werr, serr)
		s.cur.err = err
		s.cur.done = true
		s.pending = nil
		s.cur = nil
		if err != nil {
			return 0, err
		}
	}

	sealed := s.activeSeq
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
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	s.mu.Lock()
	activeSeq := s.activeSeq
	s.mu.Unlock()

	seqs, err := s.listSegments()
	if err != nil {
		return err
	}
	removed := false
	for _, seq := range seqs {
		if seq > uptoSeq || seq == activeSeq {
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
// passes it to fn. Intended for startup recovery. Corrupt/partial trailing lines
// are the caller's concern: fn decides whether to skip them.
func (s *segLog) forEachLine(fn func(line []byte)) error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()

	// Flush anything buffered (a no-op at startup, where recover is used).
	if err := s.flushPending(); err != nil {
		return err
	}

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

// close flushes any pending bytes and closes the active segment file.
func (s *segLog) close() error {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	flushErr := s.flushPending()
	s.mu.Lock()
	defer s.mu.Unlock()
	closeErr := s.active.Close()
	return cmp.Or(flushErr, closeErr)
}
