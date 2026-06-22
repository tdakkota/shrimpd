package shrimpd

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// Server serves the daemon HTTP API for ingesting, reading, and sharing parts.
type Server struct {
	lsm *LSM
	srv *http.Server
}

// NewServer creates a daemon HTTP server bound to addr.
func NewServer(addr string, lsm *LSM) *Server {
	mux := http.NewServeMux()
	s := &Server{lsm: lsm}

	// POST /ingest          body: {"data":[{"timestamp":1,"data":"foo"}]}
	// GET  /read?from=&to=  timestamp range, inclusive; omit either for open bound
	// GET  /query?from=&to= same as /read, kept as a debug-friendly alias
	// GET  /part/{id}       raw part JSON (served to peer nodes)
	// GET  /parts           global part list from etcd (debugging)
	// POST /compact         forces immediate compaction of L0 parts
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("POST /ingest/otlp", s.handleIngestOTLP)
	mux.HandleFunc("POST /v1/logs", s.handleIngestOTLP)
	mux.HandleFunc("GET /read", s.handleQuery)
	mux.HandleFunc("GET /query", s.handleQuery)
	mux.HandleFunc("GET /part/{id}", s.handlePart)
	mux.HandleFunc("GET /parts", s.handleParts)
	mux.HandleFunc("POST /compact", s.handleCompact)

	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Run listens and serves until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "http server listening", "addr", s.srv.Addr)
	go func() {
		<-ctx.Done()
		if err := s.srv.Shutdown(context.Background()); err != nil {
			slog.Warn("http server shutdown failed", "error", err)
		}
	}()
	if err := s.srv.Serve(ln); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var block Block
	if err := json.NewDecoder(r.Body).Decode(&block); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(block.Data) == 0 {
		http.Error(w, "empty block", http.StatusBadRequest)
		return
	}
	if err := s.lsm.Write(r.Context(), block.Data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type otlpScopeJSON struct {
	Name       string         `json:"name,omitempty"`
	Version    string         `json:"version,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type otlpLogRecordJSON struct {
	Timestamp         uint64         `json:"timestamp"`
	ObservedTimestamp uint64         `json:"observed_timestamp,omitempty"`
	SeverityNumber    int32          `json:"severity_number,omitempty"`
	SeverityText      string         `json:"severity_text,omitempty"`
	Body              any            `json:"body,omitempty"`
	Attributes        map[string]any `json:"attributes,omitempty"`
	TraceID           string         `json:"trace_id,omitempty"`
	SpanID            string         `json:"span_id,omitempty"`
	Flags             uint32         `json:"flags,omitempty"`
	Resource          map[string]any `json:"resource,omitempty"`
	Scope             *otlpScopeJSON `json:"scope,omitempty"`
}

func (s *Server) handleIngestOTLP(w http.ResponseWriter, r *http.Request) {
	const maxBodySize = 32 << 20 // 32 MiB
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var logsData plog.Logs
	var unmarshalErr error
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/x-protobuf") {
		unmarshaler := &plog.ProtoUnmarshaler{}
		logsData, unmarshalErr = unmarshaler.UnmarshalLogs(bodyBytes)
	} else {
		// Default to JSON
		unmarshaler := &plog.JSONUnmarshaler{}
		logsData, unmarshalErr = unmarshaler.UnmarshalLogs(bodyBytes)
	}
	if unmarshalErr != nil {
		http.Error(w, "failed to unmarshal logs: "+unmarshalErr.Error(), http.StatusBadRequest)
		return
	}

	var entries []Entry
	now := time.Now().UnixNano()

	rls := logsData.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scope := sl.Scope()
			records := sl.LogRecords()
			for k := 0; k < records.Len(); k++ {
				record := records.At(k)
				ts := record.Timestamp()
				if ts == 0 {
					ts = record.ObservedTimestamp()
				}
				if ts == 0 {
					ts = pcommon.Timestamp(now)
				}

				rMap := resource.Attributes().AsRaw()
				sObj := &otlpScopeJSON{
					Name:       scope.Name(),
					Version:    scope.Version(),
					Attributes: scope.Attributes().AsRaw(),
				}

				bodyVal := record.Body().AsRaw()
				attrMap := record.Attributes().AsRaw()

				var traceIDHex string
				if !record.TraceID().IsEmpty() {
					traceIDHex = record.TraceID().String()
				}
				var spanIDHex string
				if !record.SpanID().IsEmpty() {
					spanIDHex = record.SpanID().String()
				}

				entryJSON := otlpLogRecordJSON{
					Timestamp:         uint64(record.Timestamp()),
					ObservedTimestamp: uint64(record.ObservedTimestamp()),
					SeverityNumber:    int32(record.SeverityNumber()),
					SeverityText:      record.SeverityText(),
					Body:              bodyVal,
					Attributes:        attrMap,
					TraceID:           traceIDHex,
					SpanID:            spanIDHex,
					Flags:             uint32(record.Flags()),
					Resource:          rMap,
					Scope:             sObj,
				}

			dataBytes, err := json.Marshal(entryJSON)
			if err != nil {
				slog.WarnContext(r.Context(), "skip OTLP record: marshal failed", "error", err)
				continue
			}

				entries = append(entries, Entry{
					Timestamp: int64(ts),
					Data:      string(dataBytes),
				})
			}
		}
	}

	if len(entries) == 0 {
		s.writeOTLPResponse(w, contentType)
		return
	}

	if err := s.lsm.Write(r.Context(), entries); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeOTLPResponse(w, contentType)
}

func (s *Server) writeOTLPResponse(w http.ResponseWriter, contentType string) {
	if strings.Contains(contentType, "application/x-protobuf") {
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		// Do not write body bytes to match prior behavior and test expectations.
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := plogotlp.NewExportResponse()
		b, _ := resp.MarshalJSON()
		_, _ = w.Write(b)
	}
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := parseIntParam(q.Get("from"), 0)
	to := parseIntParam(q.Get("to"), 1<<62)
	term := q.Get("term")

	entries, err := s.lsm.Query(r.Context(), from, to, term)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Block{Data: entries}); err != nil {
		slog.WarnContext(r.Context(), "encode query response", "error", err)
	}
}

func (s *Server) handlePart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Set Content-Type before writing; http.Error will override it on failure
	// (safe because os.Open failure occurs before any bytes are written to w).
	w.Header().Set("Content-Type", "application/json")
	if err := s.lsm.ServeLocalPart(id, w); err != nil {
		http.Error(w, "part not found: "+id, http.StatusNotFound)
	}
}

func (s *Server) handleParts(w http.ResponseWriter, r *http.Request) {
	parts, err := s.lsm.AllParts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(parts); err != nil {
		slog.WarnContext(r.Context(), "encode parts response", "error", err)
	}
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.compact(r.Context(), true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseIntParam(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}
