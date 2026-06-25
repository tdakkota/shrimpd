// Package shrimpapi implements the HTTP API for the shrimpd daemon.
package shrimpapi

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/CAFxX/httpcompression"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/oteldb/shrimpd/internal/shrimpfilter"
	"github.com/oteldb/shrimpd/internal/shrimplication"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

// Server serves the daemon HTTP API for ingesting, reading, and sharing parts.
type Server struct {
	lsm *shrimplication.LSM
	srv *http.Server
}

// NewServer creates a daemon HTTP server bound to addr.
func NewServer(addr string, lsm *shrimplication.LSM) *Server {
	mux := http.NewServeMux()
	s := &Server{lsm: lsm}

	// POST /ingest          body: {"data":[{"timestamp":1,"data":"foo"}]}
	// GET  /read?from=&to=  timestamp range, inclusive; omit either for open bound
	// GET  /query?from=&to= same as /read, kept as a debug-friendly alias
	// GET  /part/{id}       raw part JSON (served to peer nodes)
	// GET  /parts           global part list from etcd (debugging)
	// POST /flush           forces immediate flush of memtable and index memtable
	// POST /compact         forces immediate compaction of data and index parts
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("POST /ingest/otlp", s.handleIngestOTLP)
	mux.HandleFunc("POST /v1/logs", s.handleIngestOTLP)
	queryCompression, err := httpcompression.DefaultAdapter(httpcompression.MinSize(1024))
	if err != nil {
		panic(err)
	}
	queryHandler := queryCompression(http.HandlerFunc(s.handleQuery))
	mux.Handle("GET /read", queryHandler)
	mux.Handle("GET /query", queryHandler)
	mux.HandleFunc("GET /part/{id}", s.handlePart)
	mux.HandleFunc("GET /parts", s.handleParts)
	mux.HandleFunc("POST /flush", s.handleFlush)
	mux.HandleFunc("POST /compact", s.handleCompact)

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.InfoContext(r.Context(), "incoming request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		mux.ServeHTTP(w, r)
	})

	s.srv = &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
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

// ingestStreamBatch controls how many entries are decoded and written to WAL at
// a time. Keeping this small bounds peak memory regardless of request body size.
const ingestStreamBatch = 1000

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	const (
		maxIngestBody = 256 << 20 // 256 MiB hard ceiling
		jxBufSize     = 4096      // jx internal read buffer; covers any single token
	)

	d := jx.Decode(http.MaxBytesReader(w, r.Body, maxIngestBody), jxBufSize)

	var (
		batch    []shrimptypes.Entry
		total    int
		writeErr error
	)

	flush := func() {
		if writeErr != nil || len(batch) == 0 {
			return
		}
		if err := s.lsm.Write(r.Context(), batch); err != nil {
			writeErr = err
			return
		}
		total += len(batch)
		batch = batch[:0]
	}

	decodeErr := decodeIngestEntries(d, func(e shrimptypes.Entry) error {
		if writeErr != nil {
			return writeErr
		}
		batch = append(batch, e)
		if len(batch) >= ingestStreamBatch {
			flush()
		}
		return writeErr
	})

	if decodeErr != nil {
		http.Error(w, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	if writeErr != nil {
		http.Error(w, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	flush() // flush remaining tail
	if writeErr != nil {
		http.Error(w, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	if total == 0 {
		http.Error(w, "empty block", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleIngestOTLP(w http.ResponseWriter, r *http.Request) {
	const maxBodySize = 32 << 20 // 32 MiB
	bodyReader := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		bodyReader, err = gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer func() { _ = bodyReader.Close() }()
	}

	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, bodyReader, maxBodySize))
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
		slog.WarnContext(r.Context(), "failed to unmarshal OTLP logs", "error", unmarshalErr, "content_type", contentType)
		http.Error(w, "failed to unmarshal logs: "+unmarshalErr.Error(), http.StatusBadRequest)
		return
	}

	var entries []shrimptypes.Entry
	now := time.Now().UnixNano()
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

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

				if err := writeOTLPLogRecordJSON(jw, record, resource.Attributes().AsRaw(), scope); err != nil {
					slog.WarnContext(r.Context(), "skip OTLP record: encode failed", "error", err)
					continue
				}

				entries = append(entries, shrimptypes.Entry{
					Timestamp: int64(ts),
					Data:      string(jw.Buf),
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

	var m shrimpfilter.Matcher
	if qstr := q.Get("q"); qstr != "" {
		var err error
		m, err = decodeMatcherQuery(qstr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	// Stream response: never accumulate a []Entry result slice.
	// Peak memory = O(one decoded block) regardless of match count.
	bw := bufio.NewWriterSize(w, 64<<10)
	_, _ = bw.WriteString(`{"data":[`)

	first := true
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	var err error
	emit := func(e shrimptypes.Entry) error {
		jw.Reset()
		if !first {
			jw.Comma()
		}
		first = false
		writeEntryJSON(jw, e)
		_, werr := bw.Write(jw.Buf)
		return werr
	}

	var stats *shrimptypes.QueryStats
	if q.Get("q") != "" {
		stats, err = s.lsm.QueryMatcherWithStats(r.Context(), from, to, m, emit)
		if err != nil {
			slog.WarnContext(r.Context(), "query matcher", "error", err)
		}
	} else {
		stats, err = s.lsm.QueryStreamWithStats(r.Context(), from, to, term, emit)
		if err != nil {
			slog.WarnContext(r.Context(), "query stream", "error", err)
		}
	}

	_, _ = bw.WriteString(`],"stats":`)
	jw.Reset()
	writeQueryStatsJSON(jw, stats)
	_, _ = bw.Write(jw.Buf)
	_, _ = bw.WriteString(`}`)
	if ferr := bw.Flush(); ferr != nil {
		slog.WarnContext(r.Context(), "flush query response", "error", ferr)
	}
}

func writeEntryJSON(jw *jx.Writer, e shrimptypes.Entry) {
	jw.ObjStart()
	jw.FieldStart("timestamp")
	jw.Int64(e.Timestamp)
	jw.Comma()
	jw.FieldStart("data")
	jw.Str(e.Data)
	jw.ObjEnd()
}

func decodeIngestEntries(d *jx.Decoder, emit func(shrimptypes.Entry) error) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		if string(key) != "data" {
			return d.Skip()
		}
		return d.Arr(func(d *jx.Decoder) error {
			e, err := decodeEntryJSON(d)
			if err != nil {
				return err
			}
			return emit(e)
		})
	})
}

func decodeEntryJSON(d *jx.Decoder) (shrimptypes.Entry, error) {
	var e shrimptypes.Entry
	err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
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
	})
	return e, err
}

func writeOTLPLogRecordJSON(
	jw *jx.Writer,
	record plog.LogRecord,
	resource map[string]any,
	scope pcommon.InstrumentationScope,
) error {
	bodyVal := record.Body().AsRaw()
	attrMap := record.Attributes().AsRaw()
	sMap := scope.Attributes().AsRaw()

	var traceIDHex string
	if !record.TraceID().IsEmpty() {
		traceIDHex = record.TraceID().String()
	}
	var spanIDHex string
	if !record.SpanID().IsEmpty() {
		spanIDHex = record.SpanID().String()
	}

	jw.Reset()
	jw.ObjStart()
	firstField := true
	field := func(name string) {
		if !firstField {
			jw.Comma()
		}
		firstField = false
		jw.FieldStart(name)
	}

	field("timestamp")
	jw.UInt64(uint64(record.Timestamp()))

	if observed := uint64(record.ObservedTimestamp()); observed != 0 {
		field("observed_timestamp")
		jw.UInt64(observed)
	}
	if severity := int32(record.SeverityNumber()); severity != 0 {
		field("severity_number")
		jw.Int32(severity)
	}
	if severityText := record.SeverityText(); severityText != "" {
		field("severity_text")
		jw.Str(severityText)
	}
	if bodyVal != nil {
		field("body")
		if err := writeJSONAny(jw, bodyVal); err != nil {
			return fmt.Errorf("encode body: %w", err)
		}
	}
	if len(attrMap) != 0 {
		field("attributes")
		if err := writeJSONAny(jw, attrMap); err != nil {
			return fmt.Errorf("encode attributes: %w", err)
		}
	}
	if traceIDHex != "" {
		field("trace_id")
		jw.Str(traceIDHex)
	}
	if spanIDHex != "" {
		field("span_id")
		jw.Str(spanIDHex)
	}
	if flags := uint32(record.Flags()); flags != 0 {
		field("flags")
		jw.UInt32(flags)
	}
	if len(resource) != 0 {
		field("resource")
		if err := writeJSONAny(jw, resource); err != nil {
			return fmt.Errorf("encode resource: %w", err)
		}
	}

	field("scope")
	jw.ObjStart()
	scopeFirst := true
	scopeField := func(name string) {
		if !scopeFirst {
			jw.Comma()
		}
		scopeFirst = false
		jw.FieldStart(name)
	}
	if name := scope.Name(); name != "" {
		scopeField("name")
		jw.Str(name)
	}
	if version := scope.Version(); version != "" {
		scopeField("version")
		jw.Str(version)
	}
	if len(sMap) != 0 {
		scopeField("attributes")
		if err := writeJSONAny(jw, sMap); err != nil {
			return fmt.Errorf("encode scope attributes: %w", err)
		}
	}
	jw.ObjEnd()
	jw.ObjEnd()

	if !jx.Valid(jw.Buf) {
		return fmt.Errorf("invalid JSON produced")
	}

	return nil
}

func decodeMatcherQuery(qstr string) (shrimpfilter.Matcher, error) {
	var qf struct {
		Line []struct {
			Op string `json:"op"`
			V  string `json:"v"`
		} `json:"line"`
		Labels []struct {
			L  string `json:"l"`
			Op string `json:"op"`
			V  string `json:"v"`
		} `json:"labels"`
	}
	if err := json.Unmarshal([]byte(qstr), &qf); err != nil {
		return shrimpfilter.Matcher{}, fmt.Errorf("bad q: %w", err)
	}

	lines := make([]shrimpfilter.LineFilter, 0, len(qf.Line))
	for _, lf := range qf.Line {
		op, ok := parseLineOp(lf.Op)
		if !ok {
			return shrimpfilter.Matcher{}, fmt.Errorf("bad line op: %s", lf.Op)
		}
		lines = append(lines, shrimpfilter.LineFilter{Op: op, Value: lf.V})
	}

	labels := make([]shrimpfilter.LabelFilter, 0, len(qf.Labels))
	for _, lf := range qf.Labels {
		op, ok := parseLabelOp(lf.Op)
		if !ok {
			return shrimpfilter.Matcher{}, fmt.Errorf("bad label op: %s", lf.Op)
		}
		labels = append(labels, shrimpfilter.LabelFilter{Label: lf.L, Op: op, Value: lf.V})
	}

	m, err := shrimpfilter.CompileMatcher(lines, labels)
	if err != nil {
		return shrimpfilter.Matcher{}, fmt.Errorf("bad matcher: %w", err)
	}

	return m, nil
}

func writeJSONAny(jw *jx.Writer, v any) error {
	switch x := v.(type) {
	case nil:
		jw.Null()
		return nil
	case bool:
		jw.Bool(x)
		return nil
	case string:
		jw.Str(x)
		return nil
	case []byte:
		jw.Base64(x)
		return nil
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return fmt.Errorf("unsupported float32 value")
		}
		jw.Float32(x)
		return nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return fmt.Errorf("unsupported float64 value")
		}
		jw.Float64(x)
		return nil
	case int:
		jw.Int(x)
		return nil
	case int8:
		jw.Int8(x)
		return nil
	case int16:
		jw.Int16(x)
		return nil
	case int32:
		jw.Int32(x)
		return nil
	case int64:
		jw.Int64(x)
		return nil
	case uint:
		jw.UInt(x)
		return nil
	case uint8:
		jw.UInt8(x)
		return nil
	case uint16:
		jw.UInt16(x)
		return nil
	case uint32:
		jw.UInt32(x)
		return nil
	case uint64:
		jw.UInt64(x)
		return nil
	case map[string]any:
		jw.ObjStart()
		first := true
		for k, v := range x {
			if !first {
				jw.Comma()
			}
			first = false
			jw.FieldStart(k)
			if err := writeJSONAny(jw, v); err != nil {
				return err
			}
		}
		jw.ObjEnd()
		return nil
	case []any:
		jw.ArrStart()
		for i, v := range x {
			if i != 0 {
				jw.Comma()
			}
			if err := writeJSONAny(jw, v); err != nil {
				return err
			}
		}
		jw.ArrEnd()
		return nil
	default:
		return fmt.Errorf("unsupported JSON type %T", x)
	}
}

func writeQueryStatsJSON(jw *jx.Writer, stats *shrimptypes.QueryStats) {
	if stats == nil {
		jw.Null()
		return
	}

	jw.ObjStart()
	jw.FieldStart("parts_total")
	jw.Int(stats.PartsTotal)
	jw.Comma()
	jw.FieldStart("parts_pruned_by_ts")
	jw.Int(stats.PartsPrunedByTS)
	jw.Comma()
	jw.FieldStart("parts_pruned_by_index")
	jw.Int(stats.PartsPrunedByIndex)
	jw.Comma()
	jw.FieldStart("parts_scanned")
	jw.Int(stats.PartsScanned)
	jw.Comma()
	jw.FieldStart("blocks_total")
	jw.Int(stats.BlocksTotal)
	jw.Comma()
	jw.FieldStart("blocks_pruned_by_ts")
	jw.Int(stats.BlocksPrunedByTS)
	jw.Comma()
	jw.FieldStart("blocks_pruned_by_index")
	jw.Int(stats.BlocksPrunedByIndex)
	jw.Comma()
	jw.FieldStart("blocks_scanned")
	jw.Int(stats.BlocksScanned)
	jw.Comma()
	jw.FieldStart("entries_scanned")
	jw.Int(stats.EntriesScanned)
	jw.Comma()
	jw.FieldStart("entries_matched")
	jw.Int(stats.EntriesMatched)
	jw.Comma()
	jw.FieldStart("used_index")
	jw.Bool(stats.UsedIndex)
	jw.Comma()
	jw.FieldStart("duration_ms")
	jw.Int64(stats.DurationMs)
	jw.ObjEnd()
}

func parseLineOp(s string) (shrimpfilter.LineOp, bool) {
	switch s {
	case "|=", "eq":
		return shrimpfilter.OpLineEq, true
	case "!=", "ne":
		return shrimpfilter.OpLineNotEq, true
	case "|~", "re":
		return shrimpfilter.OpLineRe, true
	case "!~", "nre":
		return shrimpfilter.OpLineNotRe, true
	}
	return 0, false
}

func parseLabelOp(s string) (shrimpfilter.LabelOp, bool) {
	switch s {
	case "eq":
		return shrimpfilter.OpLabelEq, true
	case "ne":
		return shrimpfilter.OpLabelNotEq, true
	case "re":
		return shrimpfilter.OpLabelRe, true
	case "nre":
		return shrimpfilter.OpLabelNotRe, true
	}
	return 0, false
}

func (s *Server) handlePart(w http.ResponseWriter, r *http.Request) {
	// Set Content-Type before writing; http.Error will override it on failure
	// (safe because os.Open failure occurs before any bytes are written to w).
	w.Header().Set("Content-Type", "application/json")
	if err := s.lsm.ServeLocalPart(r, w); err != nil {
		status := http.StatusInternalServerError
		msg := "failed to serve part: " + r.PathValue("id")
		if os.IsNotExist(err) {
			status = http.StatusNotFound
			msg = "part not found: " + r.PathValue("id")
		}
		http.Error(w, msg, status)
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

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.Flush(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if err := s.lsm.Compact(r.Context()); err != nil {
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
