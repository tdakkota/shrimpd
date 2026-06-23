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
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tdakkota/shrimpd"

	slogmulti "github.com/samber/slog-multi"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		nodeID  = flag.String("id", "node1", "unique node identifier")
		addr    = flag.String("addr", "localhost:8080", "HTTP listen + advertise address (host:port)")
		dataDir = flag.String("data", "./data", "directory for parts and WAL")
		etcdEps = flag.String("etcd", "localhost:2379", "etcd endpoints, comma-separated")
	)
	flag.Parse()

	consoleHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	var handler slog.Handler = consoleHandler

	shutdown, otelHandler, err := initOTEL(context.Background(), *nodeID)
	if err != nil {
		slog.Warn("failed to initialize OpenTelemetry logging, falling back to console-only", "error", err)
	} else {
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

	wal, err := shrimpd.OpenWAL(*dataDir + "/wal.jsonl")
	if err != nil {
		slog.Error("open wal", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			slog.Warn("close wal", "error", err)
		}
	}()

	reg := shrimpd.NewRegistry(cli, *nodeID)

	lsm, err := shrimpd.NewLSM(*nodeID, *addr, *dataDir, wal, reg)
	if err != nil {
		slog.Error("create lsm", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return lsm.Run(ctx) })
	eg.Go(func() error { return shrimpd.NewServer(*addr, lsm).Run(ctx) })

	if err := eg.Wait(); err != nil && err != context.Canceled {
		slog.Error("exit", "error", err)
	}
}

func initOTEL(ctx context.Context, nodeID string) (func(context.Context) error, slog.Handler, error) {
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
