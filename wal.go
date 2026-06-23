package shrimpd

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// WAL is the per-node write-ahead log for pre-flush durability.
// Format: one JSON-encoded Entry per line (NDJSON). Truncated after a successful
// part flush; replayed on startup to rebuild the memtable after a crash.
type WAL struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

// OpenWAL opens the local write-ahead log at path.
func OpenWAL(path string) (*WAL, error) {
	// #nosec G304 -- the daemon intentionally opens its configured local data path.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, enc: json.NewEncoder(f)}, nil
}

// Append writes entries to the WAL and fsyncs before returning.
func (w *WAL) Append(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if err := w.enc.Encode(e); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

// Recover reads all entries from the WAL file. Called once on startup.
// Skips corrupt lines silently — they indicate a mid-write crash.
func (w *WAL) Recover() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries []Entry
	sc := bufio.NewScanner(w.f)
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			entries = append(entries, e)
		}
	}
	return entries, sc.Err()
}

// Rotate truncates the WAL after a successful flush. The entries are now
// durably committed in a part file and registered in etcd.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, 0)
	w.enc = json.NewEncoder(w.f)
	return err
}

// Close closes the WAL file.
func (w *WAL) Close() error { return w.f.Close() }

// IndexWAL is the write-ahead log for pre-flush index entries.
type IndexWAL struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

// OpenIndexWAL opens the local index write-ahead log at path.
func OpenIndexWAL(path string) (*IndexWAL, error) {
	// #nosec G304 -- configured local path
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &IndexWAL{f: f, enc: json.NewEncoder(f)}, nil
}

// Append writes entries to the IndexWAL and fsyncs before returning.
func (w *IndexWAL) Append(entries []IndexEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if err := w.enc.Encode(e); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

// Recover reads all entries from the IndexWAL file. Called once on startup.
func (w *IndexWAL) Recover() ([]IndexEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var entries []IndexEntry
	sc := bufio.NewScanner(w.f)
	for sc.Scan() {
		var e IndexEntry
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			entries = append(entries, e)
		}
	}
	return entries, sc.Err()
}

// Rotate truncates the IndexWAL after a successful flush.
func (w *IndexWAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, 0)
	w.enc = json.NewEncoder(w.f)
	return err
}

// Close closes the IndexWAL file.
func (w *IndexWAL) Close() error { return w.f.Close() }
