package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type entry struct {
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
}

type ingestReq struct {
	Data []entry `json:"data"`
}

func run() error {
	var (
		dsn       = flag.String("ch-dsn", "localhost:9000", "ClickHouse DSN")
		user      = flag.String("ch-user", "default", "ClickHouse user")
		pass      = flag.String("ch-pass", "", "ClickHouse password")
		db        = flag.String("ch-db", "default", "ClickHouse database")
		shrimpd   = flag.String("shrimpd", "http://localhost:8080", "shrimpd ingest URL")
		batchSize = flag.Int("batch", 1000, "batch size")
		from      = flag.String("from", "", "start time filter (RFC3339)")
		since     = flag.Duration("since", 0, "a Go duration to filter logs from now (e.g. 1h, 30m)")
		limit     = flag.Int("limit", 0, "limit number of logs to fetch (0 for no limit)")
	)
	flag.Parse()

	ctx := context.Background()
	conn, err := ch.Dial(ctx, ch.Options{
		Address:  *dsn,
		User:     *user,
		Password: *pass,
		Database: *db,
	})
	if err != nil {
		return fmt.Errorf("connect to ClickHouse: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	var query string
	switch {
	case *from != "" && *since != 0:
		log.Fatal("only one of -from or -since can be specified")
	case *from == "" && *since == 0:
		query = "SELECT * FROM logs"
	case *from != "":
		t, err := time.Parse(time.RFC3339, *from)
		if err != nil {
			log.Fatalf("invalid -from: %v", err)
		}
		query = fmt.Sprintf("SELECT * FROM logs WHERE timestamp >= parseDateTime64BestEffort('%s')",
			t.Format("2006-01-02 15:04:05.000000000"))
	case *since != 0:
		t := time.Now().Add(-*since)
		query = fmt.Sprintf("SELECT * FROM logs WHERE timestamp >= parseDateTime64BestEffort('%s')",
			t.Format("2006-01-02 15:04:05.000000000"))
	}
	if *limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *limit)
	}

	var (
		colServiceInstanceID = proto.NewLowCardinality(new(proto.ColStr))
		colServiceName       = proto.NewLowCardinality(new(proto.ColStr))
		colServiceNamespace  = proto.NewLowCardinality(new(proto.ColStr))
		colTimestamp         = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		colSeverityNumber    proto.ColUInt8
		colSeverityText      = proto.NewLowCardinality(new(proto.ColStr))
		colTraceID           = proto.ColFixedStr{Size: 16}
		colSpanID            = proto.ColFixedStr{Size: 8}
		colTraceFlags        proto.ColUInt8
		colBody              proto.ColStr
		colAttribute         proto.ColStr
		colResource          = proto.NewLowCardinality(new(proto.ColStr))
		colScopeName         = proto.NewLowCardinality(new(proto.ColStr))
		colScopeVersion      = proto.NewLowCardinality(new(proto.ColStr))
		colScope             = proto.NewLowCardinality(new(proto.ColStr))
	)

	var (
		buf    []entry
		total  int
		nBatch int
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	)

	sendBatch := func() error {
		body, err := json.Marshal(ingestReq{Data: buf})
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, *shrimpd+"/ingest", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		// Read the response body to ensure the connection can be reused

		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			return err
		}

		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("unexpected status: %s", resp.Status)
		}
		total += len(buf)
		nBatch++
		fmt.Fprintf(os.Stderr, "sent %d records (%d batches)\n", total, nBatch)
		return nil
	}

	if err := conn.Do(ctx, ch.Query{
		Body: query,
		Result: proto.Results{
			{Name: "service_instance_id", Data: colServiceInstanceID},
			{Name: "service_name", Data: colServiceName},
			{Name: "service_namespace", Data: colServiceNamespace},
			{Name: "timestamp", Data: colTimestamp},
			{Name: "severity_number", Data: &colSeverityNumber},
			{Name: "severity_text", Data: colSeverityText},
			{Name: "trace_id", Data: &colTraceID},
			{Name: "span_id", Data: &colSpanID},
			{Name: "trace_flags", Data: &colTraceFlags},
			{Name: "body", Data: &colBody},
			{Name: "attribute", Data: &colAttribute},
			{Name: "resource", Data: colResource},
			{Name: "scope_name", Data: colScopeName},
			{Name: "scope_version", Data: colScopeVersion},
			{Name: "scope", Data: colScope},
		},
		OnResult: func(_ context.Context, b proto.Block) error {
			for i := 0; i < b.Rows; i++ {
				row := map[string]any{
					"service_instance_id": colServiceInstanceID.Row(i),
					"service_name":        colServiceName.Row(i),
					"service_namespace":   colServiceNamespace.Row(i),
					"severity_number":     colSeverityNumber.Row(i),
					"severity_text":       colSeverityText.Row(i),
					"trace_id":            fmt.Sprintf("%x", colTraceID.Row(i)),
					"span_id":             fmt.Sprintf("%x", colSpanID.Row(i)),
					"trace_flags":         colTraceFlags.Row(i),
					"body":                colBody.Row(i),
					"attribute":           colAttribute.Row(i),
					"resource":            colResource.Row(i),
					"scope_name":          colScopeName.Row(i),
					"scope_version":       colScopeVersion.Row(i),
					"scope":               colScope.Row(i),
				}
				data, err := json.Marshal(row)
				if err != nil {
					return err
				}

				buf = append(buf, entry{
					Timestamp: colTimestamp.Row(i).UnixNano(),
					Data:      string(data),
				})
				if len(buf) >= *batchSize {
					if err := sendBatch(); err != nil {
						return err
					}
					buf = buf[:0]
				}
			}
			return nil
		},
	}); err != nil {
		return fmt.Errorf("load data: %w", err)
	}

	if len(buf) > 0 {
		if err := sendBatch(); err != nil {
			return fmt.Errorf("send tail batch: %w", err)
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
