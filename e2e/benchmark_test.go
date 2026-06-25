package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/shrimpd/internal/shrimplication"
	"github.com/oteldb/shrimpd/internal/shrimptypes"
	"github.com/oteldb/shrimpd/internal/shrimpwal"
)

func setupLSM(b *testing.B, endpoint string) *shrimplication.LSM {
	b.Helper()
	dir := b.TempDir()
	require.NoError(b, os.MkdirAll(filepath.Join(dir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(b, err)

	wal, err := shrimpwal.OpenWAL(filepath.Join(dir, "wal.jsonl"))
	require.NoError(b, err)

	addr := freeLocalAddr(b)

	lsm, err := shrimplication.NewLSM("bench-node", addr, dir, wal, shrimplication.NewRegistry(cli, "bench-node"))
	require.NoError(b, err)

	b.Cleanup(func() {
		_ = wal.Close()
		_ = lsm.Close()
		_ = cli.Close()
	})

	return lsm
}

func generateCorpus(startTS int64, count int) []shrimptypes.Entry {
	entries := make([]shrimptypes.Entry, count)
	for i := range count {
		level := "INFO"
		var suffix string
		if i == count/2 {
			level = "ERROR"
			suffix = " - special-uuid-42"
		} else {
			if i%10 == 0 {
				level = "WARN"
			}
			suffix = fmt.Sprintf(" - randomtoken-%d", i)
		}
		entries[i] = shrimptypes.Entry{
			Timestamp: startTS + int64(i),
			Data:      fmt.Sprintf("%s: standard operation status update %d%s", level, i, suffix),
		}
	}
	return entries
}

func runQueries(b *testing.B, lsm *shrimplication.LSM, from, to int64) {
	b.Helper()
	ctx := context.Background()

	b.Run("Time_Range_Scan", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := lsm.Query(ctx, from, to, "")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			_ = res
		}
	})

	b.Run("High_Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := lsm.Query(ctx, from, to, "special-uuid-42")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			_ = res
		}
	})

	b.Run("Low_Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := lsm.Query(ctx, from, to, "info")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			_ = res
		}
	})

	b.Run("Term_Miss", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, err := lsm.Query(ctx, from, to, "nonexistent-term")
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			_ = res
		}
	})
}

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()
	etcdEndpoint := startEtcd(ctx, b)

	b.Run("MemTable", func(b *testing.B) {
		lsm := setupLSM(b, etcdEndpoint)
		const count = 90
		startTS := int64(1000)
		corpus := generateCorpus(startTS, count)
		err := lsm.Write(ctx, corpus)
		require.NoError(b, err)

		runQueries(b, lsm, startTS, startTS+count)
	})

	b.Run("L0_Parts", func(b *testing.B) {
		lsm := setupLSM(b, etcdEndpoint)
		const batchSize = 100
		const numBatches = 5
		startTS := int64(1000)
		for batchIdx := range numBatches {
			corpus := generateCorpus(startTS+int64(batchIdx*batchSize), batchSize)
			err := lsm.Write(ctx, corpus)
			require.NoError(b, err)
			err = lsm.Flush(ctx)
			require.NoError(b, err)
		}

		runQueries(b, lsm, startTS, startTS+int64(numBatches*batchSize))
	})

	b.Run("L1_Part", func(b *testing.B) {
		lsm := setupLSM(b, etcdEndpoint)
		const batchSize = 100
		const numBatches = 5
		startTS := int64(1000)
		for batchIdx := range numBatches {
			corpus := generateCorpus(startTS+int64(batchIdx*batchSize), batchSize)
			err := lsm.Write(ctx, corpus)
			require.NoError(b, err)
			err = lsm.Flush(ctx)
			require.NoError(b, err)
		}
		err := lsm.Compact(ctx)
		require.NoError(b, err)

		runQueries(b, lsm, startTS, startTS+int64(numBatches*batchSize))
	})
}
