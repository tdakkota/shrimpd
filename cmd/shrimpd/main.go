package main

// Playground for LSM + etcd-based distributed log storage.
// Each node owns its parts on local disk; etcd is the global metadata plane.
//
// Quick start:
//
//	docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest
//
//	# node 1
//	go run . -id=node1 -addr=localhost:8080 -data=./data1
//
//	# node 2
//	go run . -id=node2 -addr=localhost:8081 -data=./data2
//
// Ingest:
//
//	curl -sX POST localhost:8080/ingest \
//	  -H 'Content-Type: application/json' \
//	  -d '{"data":[{"timestamp":1,"data":"foo"},{"timestamp":3,"data":"baz"}]}'
//
//	curl -sX POST localhost:8081/ingest \
//	  -H 'Content-Type: application/json' \
//	  -d '{"data":[{"timestamp":2,"data":"bar"}]}'
//
// Query across both nodes (from either):
//
//	curl -s 'localhost:8080/read?from=1&to=3' | jq .
//
// Inspect global parts (from etcd):
//
//	curl -s localhost:8080/parts | jq .

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	httpprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimepprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	slogmulti "github.com/samber/slog-multi"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"
	"golang.org/x/sync/errgroup"

	"github.com/tdakkota/shrimpd/internal/shrimpapi"
	"github.com/tdakkota/shrimpd/internal/shrimplication"
	shrimpwal "github.com/tdakkota/shrimpd/internal/shrimpwal"
)

// bytesFlag is a custom flag type that allows human-readable byte sizes (e.g., "10MB") to be parsed into uint64 values.
type bytesFlag uint64

func BytesFlag(name, value, usage string) *bytesFlag {
	b := bytesFlag(0)
	if err := b.Set(value); err != nil {
		panic(fmt.Sprintf("invalid default value for %s: %v", name, err))
	}
	flag.Var(&b, name, usage)
	return &b
}

// String implements flag.Value interface, returning the value as a string.
func (b *bytesFlag) String() string {
	return humanize.Bytes(uint64(*b))
}

// Set implements flag.Value interface, allowing human-readable byte sizes like "10MB".
func (b *bytesFlag) Set(s string) error {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return err
	}
	*b = bytesFlag(v)
	return nil
}

func main() {
	var (
		nodeID  = flag.String("id", "node1", "unique node identifier")
		addr    = flag.String("addr", "localhost:8080", "HTTP listen + advertise address (host:port)")
		dataDir = flag.String("data", "./data", "directory for parts and WAL")
		etcdEps = flag.String("etcd", "localhost:2379", "etcd endpoints, comma-separated")

		// Profiling flags
		pprofAddr          = flag.String("pprof", "", "expose net/http/pprof on this address (e.g. :6060); empty = disabled")
		cpuProfile         = flag.String("cpuprofile", "", "write CPU profile to file on exit")
		memProfileDir      = flag.String("memprofile-dir", "", "directory to write heap profiles; empty = disabled")
		memProfileInterval = flag.Duration("memprofile-interval", 30*time.Second, "how often to poll heap usage for threshold check")
		memProfileThresh   = BytesFlag("memprofile-threshold", "0", "auto-dump heap profile when HeapInuse exceeds this many bytes (0 = disabled)")
		memLimit           = BytesFlag("memlimit", "0", "soft memory limit in bytes passed to runtime/debug.SetMemoryLimit (0 = runtime default); also respects GOMEMLIMIT env var")
	)
	flag.Parse()

	// Apply soft memory limit before anything allocates.
	if *memLimit > 0 {
		prev := debug.SetMemoryLimit(int64(*memLimit))
		slog.Info("set memory limit", "bytes", *memLimit, "previous", prev)
	}

	// CPU profiling.
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile) // #nosec G304
		if err != nil {
			slog.Error("create cpu profile", "error", err)
			os.Exit(1)
		}
		if err := runtimepprof.StartCPUProfile(f); err != nil {
			slog.Error("start cpu profile", "error", err)
			os.Exit(1)
		}
		defer func() {
			runtimepprof.StopCPUProfile()
			_ = f.Close()
		}()
	}

	consoleHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	var handler slog.Handler = consoleHandler

	shutdown, otelHandler, err := initOTEL(context.Background(), *nodeID)
	if err != nil {
		slog.Warn("failed to initialize OpenTelemetry logging, falling back to console-only", "error", err)
	} else if otelHandler != nil {
		handler = slogmulti.Fanout(consoleHandler, otelHandler)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdown(shutdownCtx); err != nil {
				slog.Warn("failed to shutdown OpenTelemetry LoggerProvider", "error", err)
			}
		}()
	}

	slog.SetDefault(slog.New(handler))

	if err := os.MkdirAll(*dataDir+"/parts", 0o750); err != nil {
		slog.Error("create data directory", "error", err)
		os.Exit(1)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*etcdEps, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		slog.Error("connect etcd", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := cli.Close(); err != nil {
			slog.Warn("close etcd client", "error", err)
		}
	}()

	wal, err := shrimpwal.OpenWAL(*dataDir + "/wal.jsonl")
	if err != nil {
		slog.Error("open wal", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			slog.Warn("close wal", "error", err)
		}
	}()
	reg := shrimplication.NewRegistry(cli, *nodeID)

	lsm, err := shrimplication.NewLSM(*nodeID, *addr, *dataDir, wal, reg)
	if err != nil {
		slog.Error("create lsm", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// pprof HTTP server (separate from the main API server).
	if *pprofAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /debug/pprof/", httpprof.Index)
			mux.HandleFunc("GET /debug/pprof/cmdline", httpprof.Cmdline)
			mux.HandleFunc("GET /debug/pprof/profile", httpprof.Profile)
			mux.HandleFunc("GET /debug/pprof/symbol", httpprof.Symbol)
			mux.HandleFunc("GET /debug/pprof/trace", httpprof.Trace)
			slog.Info("pprof listening", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, mux); err != nil { // #nosec G114
				slog.Error("pprof server", "error", err)
			}
		}()
	}

	// Background heap monitor: auto-dump when threshold crossed, and on SIGUSR1.
	if *memProfileDir != "" {
		if err := os.MkdirAll(*memProfileDir, 0o750); err != nil {
			slog.Error("create memprofile dir", "error", err)
			os.Exit(1)
		}
		go heapMonitor(ctx, *memProfileDir, uint64(*memProfileThresh), *memProfileInterval)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return lsm.Run(ctx) })
	eg.Go(func() error { return shrimpapi.NewServer(*addr, lsm).Run(ctx) })

	if err := eg.Wait(); err != nil && err != context.Canceled {
		slog.Error("exit", "error", err)
	}
}

// heapMonitor polls heap stats and writes a heap profile when HeapInuse exceeds
// threshold, and on SIGUSR1. Profiles land in dir as heap-<timestamp>.pprof.
// A zero threshold disables the automatic dump; SIGUSR1 always works.
func heapMonitor(ctx context.Context, dir string, threshold uint64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// docker kill -s USR1 <container>  →  manual heap dump
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	defer signal.Stop(sigCh)

	var lastDump time.Time
	const minDumpInterval = 30 * time.Second

	dump := func(reason string) {
		if time.Since(lastDump) < minDumpInterval {
			return
		}
		path := filepath.Join(dir, fmt.Sprintf("heap-%d.pprof", time.Now().UnixNano()))
		f, err := os.Create(path) // #nosec G304
		if err != nil {
			slog.Error("create heap profile", "error", err)
			return
		}
		runtime.GC() // get up-to-date stats
		if err := runtimepprof.WriteHeapProfile(f); err != nil {
			slog.Error("write heap profile", "error", err)
		} else {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			slog.Info("wrote heap profile",
				"reason", reason,
				"path", path,
				"heap_inuse_mb", ms.HeapInuse>>20,
				"heap_alloc_mb", ms.HeapAlloc>>20,
			)
		}
		_ = f.Close()
		lastDump = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			dump("SIGUSR1")
		case <-ticker.C:
			if threshold <= 0 {
				continue
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse >= threshold {
				dump(fmt.Sprintf("threshold(%dMiB)", threshold>>20))
			}
		}
	}
}

func initOTEL(ctx context.Context, nodeID string) (func(context.Context) error, slog.Handler, error) {
	if os.Getenv("OTEL_LOGS_EXPORTER") == "none" {
		return func(_ context.Context) error { return nil }, nil, nil
	}

	exporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	processor := log.NewBatchProcessor(exporter, log.WithExportInterval(1*time.Second))
	provider := log.NewLoggerProvider(
		log.WithProcessor(processor),
	)

	otelHandler := otelslog.NewHandler(nodeID, otelslog.WithLoggerProvider(provider))

	shutdown := func(shutdownCtx context.Context) error {
		return provider.Shutdown(shutdownCtx)
	}

	return shutdown, otelHandler, nil
}
