package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oteldb/shrimpd/internal/shrimptypes"
)

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

func parseTime(s string, defaultVal int64) (int64, error) {
	if s == "" {
		return defaultVal, nil
	}
	// Try parsing as duration first
	d, err := time.ParseDuration(s)
	if err == nil {
		if d > 0 {
			return time.Now().Add(-d).UnixNano(), nil
		}
		return time.Now().Add(d).UnixNano(), nil
	}
	// Try parsing as integer nanoseconds
	val, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return val, nil
	}
	return 0, fmt.Errorf("invalid time/duration format: %q", s)
}

func formatEntry(e shrimptypes.Entry) string {
	ts := time.Unix(0, e.Timestamp).Format("2006-01-02 15:04:05.000000000")

	// Check if it's OTLP log JSON
	var record otlpLogRecordJSON
	if err := json.Unmarshal([]byte(e.Data), &record); err == nil && (record.Body != nil || record.SeverityText != "") {
		severity := "INFO"
		if record.SeverityText != "" {
			severity = record.SeverityText
		}

		bodyStr := ""
		if record.Body != nil {
			bodyStr = fmt.Sprintf("%v", record.Body)
		}

		var attrs []string
		if len(record.Attributes) > 0 {
			attrs = append(attrs, fmt.Sprintf("attrs: %v", record.Attributes))
		}
		if len(record.Resource) > 0 {
			attrs = append(attrs, fmt.Sprintf("resource: %v", record.Resource))
		}
		if record.TraceID != "" {
			attrs = append(attrs, fmt.Sprintf("trace_id: %s", record.TraceID))
		}
		if record.SpanID != "" {
			attrs = append(attrs, fmt.Sprintf("span_id: %s", record.SpanID))
		}

		extra := ""
		if len(attrs) > 0 {
			extra = " (" + strings.Join(attrs, ", ") + ")"
		}

		return fmt.Sprintf("%s [%s] %s%s", ts, severity, bodyStr, extra)
	}

	// Fallback to raw text
	return fmt.Sprintf("%s %s", ts, e.Data)
}

func main() {
	serverAddr := flag.String("server", "http://localhost:8080", "shrimpd HTTP address")
	flag.StringVar(serverAddr, "s", "http://localhost:8080", "shrimpd HTTP address (shorthand)")

	limit := flag.Int("n", 100, "maximum number of recent log lines to display (0 for unlimited)")
	flag.IntVar(limit, "limit", 100, "maximum number of recent log lines to display (shorthand)")

	fromStr := flag.String("from", "", "query starting timestamp (duration e.g. 5m, or Unix nanoseconds)")
	toStr := flag.String("to", "", "query ending timestamp (duration e.g. 1m, or Unix nanoseconds)")
	termFlag := flag.String("term", "", "filter term (can also be specified as positional arguments)")
	qFlag := flag.String("q", "", "matcher filter (JSON, same format as GET /query q param)")
	parseFlag := flag.Bool("parse", false, "enables entry parsing")
	statsFlag := flag.Bool("stats", false, "prints query execution stats to stderr")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: shrimply [options] [term]\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// Handle term from positional arguments if not specified in flags
	term := *termFlag
	if term == "" && flag.NArg() > 0 {
		term = strings.Join(flag.Args(), " ")
	}

	from, err := parseTime(*fromStr, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: from time: %v\n", err)
		os.Exit(1)
	}

	to, err := parseTime(*toStr, 1<<62)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: to time: %v\n", err)
		os.Exit(1)
	}

	// Ensure the server URL is well-formed
	srv := *serverAddr
	if !strings.HasPrefix(srv, "http://") && !strings.HasPrefix(srv, "https://") {
		srv = "http://" + srv
	}

	u, err := url.Parse(srv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid server URL: %v\n", err)
		os.Exit(1)
	}

	// Resolve the relative path /query
	u.Path = "/query"

	q := u.Query()
	if term != "" {
		q.Set("term", term)
	}
	if *qFlag != "" {
		q.Set("q", *qFlag)
	}
	if from != 0 {
		q.Set("from", strconv.FormatInt(from, 10))
	}
	if to != 1<<62 {
		q.Set("to", strconv.FormatInt(to, 10))
	}
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error: server returned status %s: %s\n", resp.Status, string(body))
		os.Exit(1)
	}

	var block shrimptypes.Block
	if err := json.NewDecoder(resp.Body).Decode(&block); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to decode response: %v\n", err)
		os.Exit(1)
	}

	entries := block.Data
	if *limit > 0 && len(entries) > *limit {
		entries = entries[len(entries)-*limit:]
	}

	for _, entry := range entries {
		s := strings.TrimSpace(entry.Data)
		if *parseFlag {
			s = formatEntry(entry)
		}
		fmt.Println(s)
	}

	if *statsFlag && block.Stats != nil {
		stats := block.Stats
		fmt.Fprintf(os.Stderr, "stats: took=%dms parts(total=%d pruned_ts=%d pruned_index=%d scanned=%d) blocks(total=%d pruned_ts=%d pruned_index=%d scanned=%d) entries(scanned=%d matched=%d) used_index=%t\n",
			stats.DurationMs,
			stats.PartsTotal,
			stats.PartsPrunedByTS,
			stats.PartsPrunedByIndex,
			stats.PartsScanned,
			stats.BlocksTotal,
			stats.BlocksPrunedByTS,
			stats.BlocksPrunedByIndex,
			stats.BlocksScanned,
			stats.EntriesScanned,
			stats.EntriesMatched,
			stats.UsedIndex,
		)
	}
}
