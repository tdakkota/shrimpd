package shrimplication

import (
	"path/filepath"
	"sync"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// PartManager owns open [shrimpblock.PartFileV2] instances for the lifetime of the active set.
type PartManager struct {
	dataDir string
	mu      sync.RWMutex
	fds     map[string]*shrimpblock.PartFileV2
}

// NewPartManager creates a new PartManager.
func NewPartManager(dataDir string) *PartManager {
	return &PartManager{
		dataDir: dataDir,
		fds:     make(map[string]*shrimpblock.PartFileV2),
	}
}

// Get returns a shrimpblock.PartFileV2 for the given part ID. If already open, returns
// the cached instance. Otherwise tries to open the V2 file; if the file is
// legacy (no V2 magic), returns nil, nil.
func (pm *PartManager) Get(id string, meta shrimptypes.PartMeta) (*shrimpblock.PartFileV2, error) {
	pm.mu.RLock()
	pf, ok := pm.fds[id]
	pm.mu.RUnlock()
	if ok {
		return pf, nil
	}

	path := filepath.Join(pm.dataDir, "parts", id+".json")
	pf, err := shrimpblock.OpenPartV2(path, meta)
	if err != nil {
		return nil, err
	}
	if pf == nil {
		return nil, nil
	}
	if err := shrimpblock.VerifyPartV2(pf); err != nil {
		_ = pf.Close()
		return nil, err
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

// Release closes and removes the shrimpblock.PartFileV2 for the given part ID.
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

// Close releases all open shrimpblock.PartFileV2 instances.
func (pm *PartManager) Close() {
	pm.mu.Lock()
	fds := pm.fds
	pm.fds = make(map[string]*shrimpblock.PartFileV2)
	pm.mu.Unlock()
	for _, pf := range fds {
		_ = pf.Close()
	}
}
