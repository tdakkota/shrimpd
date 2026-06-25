package shrimplication

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/tdakkota/shrimpd/internal/fsyncutil"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

func (l *LSM) partPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".json")
}

func (l *LSM) partMetaPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".meta")
}

func (l *LSM) sidecarPath(id string) string {
	return filepath.Join(l.dataDir, "parts", id+".sparse.json")
}

// ReadMeta reads [shrimptypes.PartMeta] from a .meta file on disk.
func ReadMeta(path string) (shrimptypes.PartMeta, error) {
	f, err := os.Open(path) // #nosec G304 -- trusted internal part path
	if err != nil {
		return shrimptypes.PartMeta{}, err
	}
	defer func() { _ = f.Close() }()
	var meta shrimptypes.PartMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return shrimptypes.PartMeta{}, err
	}
	return meta, nil
}

// WriteMeta writes meta to path atomically via a temp-file + rename.
func WriteMeta(path string, meta shrimptypes.PartMeta) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-meta-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(meta); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return fsyncutil.SyncDir(filepath.Dir(path))
}
