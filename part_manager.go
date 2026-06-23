package shrimpd

import (
	"path/filepath"
	"sync"
)

// PartManager owns open PartFileV2 instances for the lifetime of the active set.
type PartManager struct {
	dataDir string
	mu      sync.RWMutex
	fds     map[string]*PartFileV2
}

// NewPartManager creates a new PartManager.
func NewPartManager(dataDir string) *PartManager {
	return &PartManager{
		dataDir: dataDir,
		fds:     make(map[string]*PartFileV2),
	}
}

// Get returns a PartFileV2 for the given part ID. If already open, returns
// the cached instance. Otherwise tries to open the V2 file; if the file is
// legacy (no V2 magic), returns nil, nil.
func (pm *PartManager) Get(id string, meta PartMeta) (*PartFileV2, error) {
	pm.mu.RLock()
	pf, ok := pm.fds[id]
	pm.mu.RUnlock()
	if ok {
		return pf, nil
	}

	if meta.FormatVersion != 1 {
		return nil, nil
	}

	path := filepath.Join(pm.dataDir, "parts", id+".json")
	pf, err := openPartV2(path, meta)
	if err != nil {
		return nil, err
	}
	if pf == nil {
		return nil, nil
	}

	pm.mu.Lock()
	if existing, ok := pm.fds[id]; ok {
		pm.mu.Unlock()
		_ = pf.Close()
		return existing, nil
	}
	pm.fds[id] = pf
	pm.mu.Unlock()

	return pf, nil
}

// Release closes and removes the PartFileV2 for the given part ID.
func (pm *PartManager) Release(id string) {
	pm.mu.Lock()
	pf, ok := pm.fds[id]
	if ok {
		delete(pm.fds, id)
	}
	pm.mu.Unlock()
	if ok {
		_ = pf.Close()
	}
}

// Close releases all open PartFileV2 instances.
func (pm *PartManager) Close() {
	pm.mu.Lock()
	fds := pm.fds
	pm.fds = make(map[string]*PartFileV2)
	pm.mu.Unlock()
	for _, pf := range fds {
		_ = pf.Close()
	}
}
