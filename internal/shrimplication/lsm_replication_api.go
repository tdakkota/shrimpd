package shrimplication

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/tdakkota/shrimpd/internal/shrimpblock"
	"github.com/tdakkota/shrimpd/internal/shrimptypes"
)

// AllParts returns the copy of current memory parts list.
func (l *LSM) AllParts(_ context.Context) ([]shrimptypes.PartMeta, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	copied := make([]shrimptypes.PartMeta, len(l.parts))
	copy(copied, l.parts)
	return copied, nil
}

// ServeLocalPart streams the part file to w, used by /part/{id}.
// If the part is zstd-compressed on disk and the client advertises Accept-Encoding: zstd,
// the compressed bytes are streamed verbatim with Content-Encoding: zstd; otherwise the
// part is decompressed on the fly so legacy peers and humans get plain JSON.
func (l *LSM) ServeLocalPart(r *http.Request, w http.ResponseWriter) error {
	id := r.PathValue("id")
	f, err := os.Open(l.partPath(id)) // #nosec G304 -- trusted internal part path
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if pf, err := shrimpblock.OpenPartV2(f.Name(), shrimptypes.PartMeta{}); err != nil {
		return err
	} else if pf != nil {
		if err := shrimpblock.VerifyPartV2(pf); err != nil {
			_ = pf.Close()
			return err
		}
		_ = pf.Close()
	}

	br := bufio.NewReaderSize(f, 512)
	head, err := br.Peek(4)
	if err != nil && err != io.EOF {
		return err
	}
	onDisk := shrimpblock.DetectAlgo(head)
	acceptZstd := strings.Contains(r.Header.Get("Accept-Encoding"), shrimpblock.CompressionZstd)

	if onDisk == shrimpblock.CompressionZstd && acceptZstd {
		w.Header().Set("Content-Encoding", shrimpblock.CompressionZstd)
		_, copyErr := io.Copy(w, br)
		return copyErr
	}

	if onDisk == shrimpblock.CompressionZstd {
		dec, _, err := shrimpblock.OpenBlockReader(br)
		if err != nil {
			return err
		}
		defer func() { _ = dec.Close() }()
		_, copyErr := io.Copy(w, dec)
		return copyErr
	}

	_, copyErr := io.Copy(w, br)
	return copyErr
}

// fetchRemotePart downloads a part, trying meta.Addr first then each addr in
// extraCandidates until one succeeds. This allows recovery when the origin node
// has merged/GC'd the part — any live peer that already replicated it will serve it.
//
// It returns the raw bytes (to be written verbatim) and the decoded Block (for indexing).
func fetchRemotePart(ctx context.Context, meta shrimptypes.PartMeta, extraCandidates []string, client *http.Client) (raw []byte, block shrimptypes.Block, err error) {
	candidates := make([]string, 0, 1+len(extraCandidates))
	candidates = append(candidates, meta.Addr)
	for _, c := range extraCandidates {
		if c != meta.Addr {
			candidates = append(candidates, c)
		}
	}

	var errs []error
	for _, addr := range candidates {
		raw, block, err = tryFetchPartFrom(ctx, addr, meta, client)
		if err == nil {
			return raw, block, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
	}
	return nil, shrimptypes.Block{}, errors.Join(errs...)
}

// tryFetchPartFrom fetches a single part from the given addr.
func tryFetchPartFrom(ctx context.Context, addr string, meta shrimptypes.PartMeta, client *http.Client) (raw []byte, block shrimptypes.Block, err error) {
	u := (&url.URL{Scheme: "http", Host: addr}).JoinPath("part", meta.ID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, shrimptypes.Block{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, shrimptypes.Block{}, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, shrimptypes.Block{}, fmt.Errorf("remote %s from %s: HTTP %d", meta.ID, addr, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, shrimptypes.Block{}, fmt.Errorf("read body: %w", err)
	}
	_ = resp.Body.Close()

	// The httpcompression middleware on the server may transparently apply
	// HTTP-level compression (zstd) that Go's http.Client does not
	// auto-decompress. Detect and strip it before format detection so the
	// V2 magic check and JSON decoder see the native part format.
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		dec, _, decErr := shrimpblock.OpenBlockReader(bytes.NewReader(body))
		if decErr == nil {
			decompressed, readErr := io.ReadAll(dec)
			_ = dec.Close()
			if readErr == nil {
				body = decompressed
			}
		}
	}

	// V2 binary format: write raw bytes verbatim so PartManager can open them.
	if len(body) >= 4 && string(body[:4]) == shrimpblock.MagicShrimp {
		tmpDir, _ := os.MkdirTemp("", "shrimpd-fetch-*")
		tmpPath := filepath.Join(tmpDir, meta.ID+".json")
		if err := os.WriteFile(tmpPath, body, 0o600); err != nil {
			_ = os.RemoveAll(tmpDir)
			return nil, shrimptypes.Block{}, fmt.Errorf("write tmp v2: %w", err)
		}
		pf, err := shrimpblock.OpenPartV2(tmpPath, meta)
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			return nil, shrimptypes.Block{}, fmt.Errorf("open v2: %w", err)
		}
		if pf == nil {
			_ = os.RemoveAll(tmpDir)
			return nil, shrimptypes.Block{}, fmt.Errorf("invalid v2 magic: %s", meta.ID)
		}
		b, err := shrimpblock.V2ToBlock(pf)
		_ = pf.Close()
		_ = os.RemoveAll(tmpDir)
		if err != nil {
			return nil, shrimptypes.Block{}, fmt.Errorf("v2 to block: %w", err)
		}
		return body, b, nil
	}

	r, _, err := shrimpblock.OpenBlockReader(bytes.NewReader(body))
	if err != nil {
		return nil, shrimptypes.Block{}, err
	}
	var b shrimptypes.Block
	decodeErr := json.NewDecoder(r).Decode(&b)
	rCloseErr := r.Close()
	if decodeErr != nil {
		return nil, shrimptypes.Block{}, decodeErr
	}
	if rCloseErr != nil {
		return nil, shrimptypes.Block{}, rCloseErr
	}
	return body, b, nil
}
