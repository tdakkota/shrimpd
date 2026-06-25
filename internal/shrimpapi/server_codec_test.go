package shrimpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/shrimpd/internal/shrimplication"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
	"github.com/oteldb/shrimpd/internal/shrimpwal"
)

func TestDecodeEntryJSON(t *testing.T) {
	d := jx.DecodeStr(`{"timestamp":42,"data":"hello","ignored":true}`)

	e, err := decodeEntryJSON(d)
	require.NoError(t, err)
	require.Equal(t, shrimptypes.Entry{Timestamp: 42, Data: "hello"}, e)
}

func TestDecodeIngestEntries(t *testing.T) {
	d := jx.DecodeStr(`{"data":[{"timestamp":1,"data":"a"},{"timestamp":2,"data":"b"}],"x":1}`)

	var got []shrimptypes.Entry
	err := decodeIngestEntries(d, func(e shrimptypes.Entry) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []shrimptypes.Entry{{Timestamp: 1, Data: "a"}, {Timestamp: 2, Data: "b"}}, got)
}

func TestDecodeIngestEntries_EmitError(t *testing.T) {
	d := jx.DecodeStr(`{"data":[{"timestamp":1,"data":"a"}]}`)
	expected := errors.New("stop")

	err := decodeIngestEntries(d, func(shrimptypes.Entry) error {
		return expected
	})
	require.ErrorIs(t, err, expected)
}

func TestDecodeMatcherQuery(t *testing.T) {
	m, err := decodeMatcherQuery(`{"line":[{"op":"eq","v":"panic"}],"labels":[{"l":"level","op":"eq","v":"ERROR"}]}`)
	require.NoError(t, err)
	require.False(t, m.Empty())
	require.True(t, m.MatchLine("panic happened"))
	require.True(t, m.MatchLabels(map[string]string{"level": "ERROR"}))
}

func TestDecodeMatcherQuery_BadInput(t *testing.T) {
	_, err := decodeMatcherQuery(`{bad`)
	require.ErrorContains(t, err, "bad q")

	_, err = decodeMatcherQuery(`{"line":[{"op":"wat","v":"panic"}]}`)
	require.ErrorContains(t, err, "bad line op")

	_, err = decodeMatcherQuery(`{"line":[{"op":"re","v":"[bad"}]}`)
	require.ErrorContains(t, err, "bad matcher")
}

func TestWriteEntryJSON(t *testing.T) {
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	writeEntryJSON(jw, shrimptypes.Entry{Timestamp: 7, Data: "hello"})
	require.True(t, jx.Valid(jw.Buf))
	require.JSONEq(t, `{"timestamp":7,"data":"hello"}`, string(jw.Buf))
}

func TestWriteQueryStatsJSON(t *testing.T) {
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	writeQueryStatsJSON(jw, &shrimptypes.QueryStats{
		PartsTotal:          1,
		PartsPrunedByTS:     2,
		PartsPrunedByIndex:  3,
		PartsScanned:        4,
		BlocksTotal:         5,
		BlocksPrunedByTS:    6,
		BlocksPrunedByIndex: 7,
		BlocksScanned:       8,
		EntriesScanned:      9,
		EntriesMatched:      10,
		UsedIndex:           true,
		DurationMs:          11,
	})

	require.True(t, jx.Valid(jw.Buf))
	require.JSONEq(t, `{"parts_total":1,"parts_pruned_by_ts":2,"parts_pruned_by_index":3,"parts_scanned":4,"blocks_total":5,"blocks_pruned_by_ts":6,"blocks_pruned_by_index":7,"blocks_scanned":8,"entries_scanned":9,"entries_matched":10,"used_index":true,"duration_ms":11}`, string(jw.Buf))

	jw.Reset()
	writeQueryStatsJSON(jw, nil)
	require.Equal(t, "null", string(jw.Buf))
}

func TestWriteJSONAny(t *testing.T) {
	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	value := map[string]any{
		"s": "x",
		"n": int64(12),
		"b": true,
		"a": []any{"v", float64(1.5)},
	}

	err := writeJSONAny(jw, value)
	require.NoError(t, err)
	require.True(t, jx.Valid(jw.Buf))

	var got map[string]any
	require.NoError(t, json.Unmarshal(jw.Buf, &got))
	require.Equal(t, "x", got["s"])
	require.Equal(t, float64(12), got["n"])
	require.Equal(t, true, got["b"])

	jw.Reset()
	require.Error(t, writeJSONAny(jw, struct{}{}))

	jw.Reset()
	require.Error(t, writeJSONAny(jw, math.NaN()))
}

func TestWriteOTLPLogRecordJSON(t *testing.T) {
	record := plog.NewLogRecord()
	record.SetTimestamp(pcommon.Timestamp(123))
	record.SetObservedTimestamp(pcommon.Timestamp(124))
	record.SetSeverityNumber(plog.SeverityNumber(9))
	record.SetSeverityText("ERROR")
	record.Body().SetStr("panic")
	record.Attributes().PutStr("k", "v")
	record.SetFlags(plog.LogRecordFlags(1))

	var traceID pcommon.TraceID
	copy(traceID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	record.SetTraceID(traceID)

	var spanID pcommon.SpanID
	copy(spanID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8})
	record.SetSpanID(spanID)

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("logger")
	scope.SetVersion("1.2.3")
	scope.Attributes().PutStr("scope_key", "scope_val")

	resource := map[string]any{"service.name": "shrimpd"}

	jw := jx.GetWriter()
	defer jx.PutWriter(jw)

	err := writeOTLPLogRecordJSON(jw, record, resource, scope)
	require.NoError(t, err)
	require.True(t, jx.Valid(jw.Buf))

	var got map[string]any
	require.NoError(t, json.Unmarshal(jw.Buf, &got))
	require.Equal(t, float64(123), got["timestamp"])
	require.Equal(t, float64(124), got["observed_timestamp"])
	require.Equal(t, "ERROR", got["severity_text"])
	require.Equal(t, "panic", got["body"])
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", got["trace_id"])
	require.Equal(t, "0102030405060708", got["span_id"])

	scopeObj, ok := got["scope"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "logger", scopeObj["name"])
	require.Equal(t, "1.2.3", scopeObj["version"])
}

func TestHandleQuery_Gzip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))
	wal, err := shrimpwal.OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(t, err)
	defer wal.Close()

	lsm, err := shrimplication.NewLSM("n1", "127.0.0.1:0", dir, wal, shrimplication.NewRegistry(nil, "n1"))
	require.NoError(t, err)
	defer lsm.Close()

	require.NoError(t, lsm.Write(context.Background(), []shrimptypes.Entry{
		{Timestamp: 1, Data: strings.Repeat("hello ", 400)},
		{Timestamp: 2, Data: "world"},
	}))

	srv := NewServer("127.0.0.1:0", lsm)
	httpSrv := httptest.NewServer(srv.srv.Handler)
	defer httpSrv.Close()

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	req, err := http.NewRequest(http.MethodGet, httpSrv.URL+"/query?term=hello", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	zr, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	defer zr.Close()

	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Contains(t, got, "data")
	require.Equal(t, []any{map[string]any{"timestamp": float64(1), "data": strings.Repeat("hello ", 400)}}, got["data"])
}

func BenchmarkDecodeIngestEntries(b *testing.B) {
	const n = 100
	var input bytes.Buffer
	input.WriteString(`{"data":[`)
	for i := range n {
		if i != 0 {
			input.WriteString(",")
		}
		fmt.Fprintf(&input, `{"timestamp":%d,"data":"line-%d"}`, i, i)
	}
	input.WriteString(`]}`)
	buf := input.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		d := jx.DecodeBytes(buf)
		err := decodeIngestEntries(d, func(shrimptypes.Entry) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteOTLPLogRecordJSON(b *testing.B) {
	record := plog.NewLogRecord()
	record.SetTimestamp(pcommon.Timestamp(123))
	record.SetObservedTimestamp(pcommon.Timestamp(124))
	record.SetSeverityNumber(plog.SeverityNumber(9))
	record.SetSeverityText("ERROR")
	record.Body().SetStr("panic")
	record.Attributes().PutStr("k", "v")
	record.SetFlags(plog.LogRecordFlags(1))

	var traceID pcommon.TraceID
	copy(traceID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	record.SetTraceID(traceID)

	var spanID pcommon.SpanID
	copy(spanID[:], []byte{1, 2, 3, 4, 5, 6, 7, 8})
	record.SetSpanID(spanID)

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("logger")
	scope.SetVersion("1.2.3")
	scope.Attributes().PutStr("scope_key", "scope_val")

	resource := map[string]any{"service.name": "shrimpd"}

	jw := jx.GetWriter()
	b.Cleanup(func() { jx.PutWriter(jw) })

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := writeOTLPLogRecordJSON(jw, record, resource, scope); err != nil {
			b.Fatal(err)
		}
	}
}
