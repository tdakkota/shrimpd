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

	clientv3 "go.etcd.io/etcd/client/v3"
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

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

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
