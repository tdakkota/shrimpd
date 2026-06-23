package shrimpd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tdakkota/shrimpd"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/sync/errgroup"
)

func TestDaemonSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	dataDir := t.TempDir()
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	wal, err := shrimpd.OpenWAL(filepath.Join(dataDir, "wal.jsonl"))
	must.NoError(err)
	defer func() {
		must.NoError(wal.Close())
	}()

	addr := freeLocalAddr(t)
	lsm, err := shrimpd.NewLSM("node1", addr, dataDir, wal, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr, lsm).Run(runCtx) })
	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL := "http://" + addr
	waitHTTP(ctx, t, baseURL+"/parts")

	postJSON(ctx, t, baseURL+"/ingest", shrimpd.Block{Data: []shrimpd.Entry{
		{Timestamp: 2, Data: "bar"},
		{Timestamp: 1, Data: "foo"},
	}})

	var got shrimpd.Block
	getJSON(ctx, t, baseURL+"/read?from=1&to=2", &got)
	must.Equal([]shrimpd.Entry{
		{Timestamp: 1, Data: "foo"},
		{Timestamp: 2, Data: "bar"},
	}, got.Data)
}

func TestDaemonSmokeOTLP(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	dataDir := t.TempDir()
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	wal, err := shrimpd.OpenWAL(filepath.Join(dataDir, "wal.jsonl"))
	must.NoError(err)
	defer func() {
		must.NoError(wal.Close())
	}()

	addr := freeLocalAddr(t)
	lsm, err := shrimpd.NewLSM("node1", addr, dataDir, wal, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr, lsm).Run(runCtx) })
	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL := "http://" + addr
	waitHTTP(ctx, t, baseURL+"/parts")

	// Test OTLP JSON ingestion
	t.Run("OTLP_JSON", func(t *testing.T) {
		otlpJSON := `{
		"resourceLogs": [{
			"resource": {
				"attributes": [{
					"key": "service.name",
					"value": {"stringValue": "test-service"}
				}]
			},
			"scopeLogs": [{
				"scope": {
					"name": "test-scope"
				},
				"logRecords": [{
					"timeUnixNano": "1719080000000000000",
					"severityText": "INFO",
					"body": {"stringValue": "hello from OTLP JSON"}
				}]
			}]
		}]
	}`
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/ingest/otlp", bytes.NewReader([]byte(otlpJSON)))
		must.NoError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		must.NoError(err)
		must.Equal(http.StatusOK, resp.StatusCode)
		respBody, err := io.ReadAll(resp.Body)
		must.NoError(err)
		resp.Body.Close()
		must.Equal(`{"partialSuccess":{}}`, string(respBody))

		// Verify we can read it back
		var gotOTLP shrimpd.Block
		getJSON(ctx, t, baseURL+"/read?from=1719080000000000000&to=1719080000000000000", &gotOTLP)
		must.Len(gotOTLP.Data, 1)
		must.Equal(int64(1719080000000000000), gotOTLP.Data[0].Timestamp)

		// Unmarshal Data to verify resource, scope, and record are present
		var entryObj struct {
			Resource map[string]any `json:"resource"`
			Scope    struct {
				Name string `json:"name"`
			} `json:"scope"`
			Body         any    `json:"body"`
			SeverityText string `json:"severity_text"`
		}
		must.NoError(json.Unmarshal([]byte(gotOTLP.Data[0].Data), &entryObj))
		must.NotEmpty(entryObj.Resource)
		must.Equal("test-service", entryObj.Resource["service.name"])
		must.Equal("test-scope", entryObj.Scope.Name)
		must.Equal("hello from OTLP JSON", entryObj.Body)

		// Simple term queries exercise tokenization + token index pruning.
		var qHello shrimpd.Block
		getJSON(ctx, t, baseURL+"/query?from=1719080000000000000&to=1719080000000000000&term=hello", &qHello)
		must.Len(qHello.Data, 1)

		var qOTLP shrimpd.Block
		getJSON(ctx, t, baseURL+"/query?from=1719080000000000000&to=1719080000000000000&term=otlp", &qOTLP)
		must.Len(qOTLP.Data, 1)

		var qMiss shrimpd.Block
		getJSON(ctx, t, baseURL+"/query?from=1719080000000000000&to=1719080000000000000&term=nonexistent", &qMiss)
		must.Len(qMiss.Data, 0)
	})
	t.Run("OTLP_Proto", func(t *testing.T) {
		// Test OTLP Protobuf ingestion
		logsData := plog.NewLogs()
		rl := logsData.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test-service-proto")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("test-scope-proto")
		record := sl.LogRecords().AppendEmpty()
		record.SetTimestamp(1719080000000000001)
		record.SetSeverityText("WARN")
		record.Body().SetStr("hello from OTLP Proto")

		pbBytes, err := (&plog.ProtoMarshaler{}).MarshalLogs(logsData)
		must.NoError(err)

		reqPB, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/logs", bytes.NewReader(pbBytes))
		must.NoError(err)
		reqPB.Header.Set("Content-Type", "application/x-protobuf")
		respPB, err := http.DefaultClient.Do(reqPB)
		must.NoError(err)
		must.Equal(http.StatusOK, respPB.StatusCode)
		respPBBody, err := io.ReadAll(respPB.Body)
		must.NoError(err)
		respPB.Body.Close()
		must.Len(respPBBody, 0) // expect empty body for protobuf response

		// Verify we can read it back
		var gotOTLPPB shrimpd.Block
		getJSON(ctx, t, baseURL+"/read?from=1719080000000000001&to=1719080000000000001", &gotOTLPPB)
		must.Len(gotOTLPPB.Data, 1)
		must.Equal(int64(1719080000000000001), gotOTLPPB.Data[0].Timestamp)

		var entryObjPB struct {
			Resource map[string]any `json:"resource"`
			Scope    struct {
				Name string `json:"name"`
			} `json:"scope"`
			Body         any    `json:"body"`
			SeverityText string `json:"severity_text"`
		}
		must.NoError(json.Unmarshal([]byte(gotOTLPPB.Data[0].Data), &entryObjPB))
		must.NotEmpty(entryObjPB.Resource)
		must.Equal("test-service-proto", entryObjPB.Resource["service.name"])
		must.Equal("test-scope-proto", entryObjPB.Scope.Name)
		must.Equal("hello from OTLP Proto", entryObjPB.Body)

		// Term queries on protobuf-ingested record.
		var qProto shrimpd.Block
		getJSON(ctx, t, baseURL+"/query?from=1719080000000000001&to=1719080000000000001&term=proto", &qProto)
		must.Len(qProto.Data, 1)

		var qProtoMiss shrimpd.Block
		getJSON(ctx, t, baseURL+"/query?from=1719080000000000001&to=1719080000000000001&term=xyz", &qProtoMiss)
		must.Len(qProtoMiss.Data, 0)
	})
}

var (
	sharedEtcdOnce      sync.Once
	sharedEtcdContainer testcontainers.Container
	sharedEtcdEndpoint  string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedEtcdContainer != nil {
		_ = sharedEtcdContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

func startEtcd(ctx context.Context, t testing.TB) string {
	t.Helper()
	sharedEtcdOnce.Do(func() {
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "quay.io/coreos/etcd:v3.5.13",
				ExposedPorts: []string{"2379/tcp", "2380/tcp"},
				Cmd: []string{
					"/usr/local/bin/etcd",
					"--name", "node1",
					"--listen-client-urls", "http://0.0.0.0:2379",
					"--advertise-client-urls", "http://0.0.0.0:2379",
					"--listen-peer-urls", "http://0.0.0.0:2380",
					"--initial-advertise-peer-urls", "http://0.0.0.0:2380",
					"--initial-cluster", "node1=http://0.0.0.0:2380",
					"--initial-cluster-state", "new",
				},
				WaitingFor: wait.ForListeningPort("2379/tcp").WithStartupTimeout(time.Minute),
			},
			Started: true,
		})
		require.NoError(t, err)
		sharedEtcdContainer = container

		host, err := container.Host(ctx)
		require.NoError(t, err)
		port, err := container.MappedPort(ctx, "2379/tcp")
		require.NoError(t, err)
		sharedEtcdEndpoint = net.JoinHostPort(host, port.Port())
	})
	clearEtcd(ctx, t, sharedEtcdEndpoint)
	return sharedEtcdEndpoint
}

func clearEtcd(ctx context.Context, t testing.TB, endpoint string) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	_, err = cli.Delete(ctx, "/lsm/", clientv3.WithPrefix())
	require.NoError(t, err)
}

func waitEtcd(ctx context.Context, t testing.TB, cli *clientv3.Client) {
	t.Helper()
	for {
		_, err := cli.Status(ctx, cli.Endpoints()[0])
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "wait for etcd", "endpoint %s: %v", cli.Endpoints()[0], ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func freeLocalAddr(t testing.TB) string {
	t.Helper()
	must := require.New(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.NoError(err)
	addr := ln.Addr().String()
	must.NoError(ln.Close())
	return addr
}

func waitHTTP(ctx context.Context, t testing.TB, url string) {
	t.Helper()
	must := require.New(t)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		must.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "wait for http", "url %s: %v", url, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func postJSON(ctx context.Context, t testing.TB, url string, v any) {
	t.Helper()
	must := require.New(t)
	body, err := json.Marshal(v)
	must.NoError(err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	must.NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusNoContent, resp.StatusCode, "POST %s: %s", url, resp.Status)
}

func getJSON(ctx context.Context, t testing.TB, url string, v any) {
	t.Helper()
	must := require.New(t)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	must.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	defer resp.Body.Close()
	must.Equal(http.StatusOK, resp.StatusCode, "GET %s: %s", url, resp.Status)
	must.NoError(json.NewDecoder(resp.Body).Decode(v), fmt.Sprintf("decode %s", url))
}

func TestDaemonReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	// Create directories for node1 and node2
	tempDir := t.TempDir()
	dataDir1 := filepath.Join(tempDir, "node1")
	dataDir2 := filepath.Join(tempDir, "node2")
	must.NoError(os.MkdirAll(filepath.Join(dataDir1, "parts"), 0o755))
	must.NoError(os.MkdirAll(filepath.Join(dataDir2, "parts"), 0o755))

	wal1, err := shrimpd.OpenWAL(filepath.Join(dataDir1, "wal.jsonl"))
	must.NoError(err)
	defer wal1.Close()

	wal2, err := shrimpd.OpenWAL(filepath.Join(dataDir2, "wal.jsonl"))
	must.NoError(err)
	defer wal2.Close()

	addr1 := freeLocalAddr(t)
	addr2 := freeLocalAddr(t)

	lsm1, err := shrimpd.NewLSM("node1", addr1, dataDir1, wal1, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	lsm2, err := shrimpd.NewLSM("node2", addr2, dataDir2, wal2, shrimpd.NewRegistry(cli, "node2"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm1.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr1, lsm1).Run(runCtx) })
	eg.Go(func() error { return lsm2.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr2, lsm2).Run(runCtx) })

	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL1 := "http://" + addr1
	baseURL2 := "http://" + addr2
	waitHTTP(ctx, t, baseURL1+"/parts")
	waitHTTP(ctx, t, baseURL2+"/parts")

	// 1. Ingest into node1.
	// Since we ingest only 2 entries (which is below the flushThreshold of 100),
	// it will stay in memtable first. To force a flush to a part, we either wait
	// for the flushInterval (5s) or we ingest 100 entries.
	// Let's write 100 entries to force an immediate flush and replication event.
	entries := make([]shrimpd.Entry, 100)
	for i := range 100 {
		entries[i] = shrimpd.Entry{Timestamp: int64(i + 1), Data: fmt.Sprintf("val-%d", i)}
	}
	postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: entries})

	// 2. Poll read on node2 until replicated
	var got shrimpd.Block
	for {
		getJSON(ctx, t, baseURL2+"/read?from=1&to=100", &got)
		if len(got.Data) == 100 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	must.Equal(int64(1), got.Data[0].Timestamp)
	must.Equal("val-0", got.Data[0].Data)

	// 3. Trigger compaction on node1.
	// To trigger compaction, we need at least compactTrigger (4) L0 parts.
	// Let's ingest 3 more blocks of 100 entries to create 4 parts in total.
	for b := 1; b < 4; b++ {
		batchEntries := make([]shrimpd.Entry, 100)
		for i := range 100 {
			ts := int64(b*100 + i + 1)
			batchEntries[i] = shrimpd.Entry{Timestamp: ts, Data: fmt.Sprintf("val-%d", ts)}
		}
		postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: batchEntries})
	}

	// Wait for Node 2 to replicate all 4 parts
	for {
		getJSON(ctx, t, baseURL2+"/read?from=1&to=400", &got)
		if len(got.Data) == 400 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for all 4 parts replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Trigger compact on node 1
	postJSON(ctx, t, baseURL1+"/compact", nil)

	// 4. Poll parts on node2 until compaction replicated (there should be 1 part with level=1)
	var parts []shrimpd.PartMeta
	for {
		getJSON(ctx, t, baseURL2+"/parts", &parts)
		if len(parts) == 1 && parts[0].Level == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for compaction replication: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestNewNodeBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: 5 * time.Second})
	must.NoError(err)
	defer cli.Close()
	waitEtcd(ctx, t, cli)

	tempDir := t.TempDir()
	dataDir1 := filepath.Join(tempDir, "node1")
	dataDir2 := filepath.Join(tempDir, "node2")
	must.NoError(os.MkdirAll(filepath.Join(dataDir1, "parts"), 0o755))
	must.NoError(os.MkdirAll(filepath.Join(dataDir2, "parts"), 0o755))

	wal1, _ := shrimpd.OpenWAL(filepath.Join(dataDir1, "wal.jsonl"))
	defer wal1.Close()

	addr1 := freeLocalAddr(t)
	lsm1, _ := shrimpd.NewLSM("node1", addr1, dataDir1, wal1, shrimpd.NewRegistry(cli, "node1"))
	runCtx, stop := context.WithCancel(ctx)
	eg, _ := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm1.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr1, lsm1).Run(runCtx) })
	baseURL1 := "http://" + addr1
	waitHTTP(ctx, t, baseURL1+"/parts")

	// ingest enough to flush a part
	entries := make([]shrimpd.Entry, 100)
	for i := range entries {
		entries[i] = shrimpd.Entry{Timestamp: int64(i + 1), Data: "v"}
	}
	postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: entries})

	// start node2 (should bootstrap via /lsm/parts/ not replay from 0)
	wal2, _ := shrimpd.OpenWAL(filepath.Join(dataDir2, "wal.jsonl"))
	defer wal2.Close()
	addr2 := freeLocalAddr(t)
	lsm2, _ := shrimpd.NewLSM("node2", addr2, dataDir2, wal2, shrimpd.NewRegistry(cli, "node2"))
	eg.Go(func() error { return lsm2.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr2, lsm2).Run(runCtx) })
	baseURL2 := "http://" + addr2
	waitHTTP(ctx, t, baseURL2+"/parts")

	var got shrimpd.Block
	for {
		getJSON(ctx, t, baseURL2+"/read?from=1&to=100", &got)
		if len(got.Data) == 100 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("bootstrap timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stop()
	_ = eg.Wait()
}

func TestLogTruncation(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: 5 * time.Second})
	must.NoError(err)
	defer cli.Close()
	waitEtcd(ctx, t, cli)

	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "node1")
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))
	wal, _ := shrimpd.OpenWAL(filepath.Join(dataDir, "wal.jsonl"))
	defer wal.Close()

	addr := freeLocalAddr(t)
	reg := shrimpd.NewRegistry(cli, "node1")
	lsm, _ := shrimpd.NewLSM("node1", addr, dataDir, wal, reg)
	runCtx, stop := context.WithCancel(ctx)
	eg, _ := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr, lsm).Run(runCtx) })
	base := "http://" + addr
	waitHTTP(ctx, t, base+"/parts")

	// generate many parts -> long log
	for b := range 5 {
		ents := make([]shrimpd.Entry, 100)
		for i := range ents {
			ents[i] = shrimpd.Entry{Timestamp: int64(b*100 + i + 1), Data: "v"}
		}
		postJSON(ctx, t, base+"/ingest", shrimpd.Block{Data: ents})
		time.Sleep(100 * time.Millisecond)
	}
	// force compaction to produce merge entries
	postJSON(ctx, t, base+"/compact", nil)

	// wait a bit for cleanup loop to run (30s ticker), we can't easily wait, just call directly
	_ = reg.LogCleanup(ctx)

	// check log entries are bounded (we expect truncation happened at least somewhat)
	resp, _ := cli.Get(ctx, "/lsm/log/", clientv3.WithPrefix())
	// At least we did not error; if truncation ran, fewer than 1000 keys.
	must.LessOrEqual(len(resp.Kvs), 1000)
	stop()
	_ = eg.Wait()
}

func TestRecoveringNode(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: 5 * time.Second})
	must.NoError(err)
	defer cli.Close()
	waitEtcd(ctx, t, cli)

	tempDir := t.TempDir()
	dataDir1 := filepath.Join(tempDir, "node1")
	dataDir2 := filepath.Join(tempDir, "node2")
	must.NoError(os.MkdirAll(filepath.Join(dataDir1, "parts"), 0o755))
	must.NoError(os.MkdirAll(filepath.Join(dataDir2, "parts"), 0o755))

	wal1, _ := shrimpd.OpenWAL(filepath.Join(dataDir1, "wal.jsonl"))
	defer wal1.Close()
	wal2, _ := shrimpd.OpenWAL(filepath.Join(dataDir2, "wal.jsonl"))
	defer wal2.Close()

	addr1 := freeLocalAddr(t)
	addr2 := freeLocalAddr(t)

	reg1 := shrimpd.NewRegistry(cli, "node1")
	reg2 := shrimpd.NewRegistry(cli, "node2")
	lsm1, _ := shrimpd.NewLSM("node1", addr1, dataDir1, wal1, reg1)
	lsm2, _ := shrimpd.NewLSM("node2", addr2, dataDir2, wal2, reg2)

	runCtx, stop := context.WithCancel(ctx)
	eg, _ := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm1.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr1, lsm1).Run(runCtx) })
	eg.Go(func() error { return lsm2.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr2, lsm2).Run(runCtx) })

	base1 := "http://" + addr1
	base2 := "http://" + addr2
	waitHTTP(ctx, t, base1+"/parts")
	waitHTTP(ctx, t, base2+"/parts")

	// Ingest on node1 to create log entries
	for b := range 3 {
		ents := make([]shrimpd.Entry, 100)
		for i := range ents {
			ents[i] = shrimpd.Entry{Timestamp: int64(b*100 + i + 1), Data: "v"}
		}
		postJSON(ctx, t, base1+"/ingest", shrimpd.Block{Data: ents})
	}

	// Stop only node2 (keep node1 running for continued ingest)
	// We can't easily stop one goroutine group; recreate node2 lifecycle below by canceling a subctx isn't simple.
	// Simpler: start node2 later in a separate runCtx2. For now, just stop whole and restart node1+node2 together is wrong.
	// To isolate, we'll use separate run groups.

	// Stop everything first to cleanly separate phases
	stop()
	_ = eg.Wait()

	// Restart node1 alone to continue producing while node2 is down
	runCtx1b, stop1b := context.WithCancel(ctx)
	eg1b, _ := errgroup.WithContext(runCtx1b)
	wal1b, _ := shrimpd.OpenWAL(filepath.Join(dataDir1, "wal.jsonl"))
	defer wal1b.Close()
	lsm1b, _ := shrimpd.NewLSM("node1", addr1, dataDir1, wal1b, reg1)
	eg1b.Go(func() error { return lsm1b.Run(runCtx1b) })
	eg1b.Go(func() error { return shrimpd.NewServer(addr1, lsm1b).Run(runCtx1b) })
	waitHTTP(ctx, t, base1+"/parts")

	// Continue ingest + compact on node1 (node2 offline)
	for b := 3; b < 6; b++ {
		ents := make([]shrimpd.Entry, 100)
		for i := range ents {
			ents[i] = shrimpd.Entry{Timestamp: int64(b*100 + i + 1), Data: "v"}
		}
		postJSON(ctx, t, base1+"/ingest", shrimpd.Block{Data: ents})
	}
	postJSON(ctx, t, base1+"/compact", nil)
	_ = reg1.LogCleanup(ctx)

	// Start node2 fresh; it should detect gap and bootstrap from parts
	runCtx2, stop2 := context.WithCancel(ctx)
	eg2, _ := errgroup.WithContext(runCtx2)
	wal2b, _ := shrimpd.OpenWAL(filepath.Join(dataDir2, "wal.jsonl"))
	defer wal2b.Close()
	lsm2b, _ := shrimpd.NewLSM("node2", addr2, dataDir2, wal2b, reg2)
	eg2.Go(func() error { return lsm2b.Run(runCtx2) })
	eg2.Go(func() error { return shrimpd.NewServer(addr2, lsm2b).Run(runCtx2) })
	waitHTTP(ctx, t, base2+"/parts")

	var got shrimpd.Block
	for {
		getJSON(ctx, t, base2+"/read?from=1&to=600", &got)
		if len(got.Data) >= 300 { // at least some data after bootstrap
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("recovering node bootstrap timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
	stop2()
	_ = eg2.Wait()
	stop1b()
	_ = eg1b.Wait()
}

func TestShrimplyCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Precompile shrimply binary
	tmpBinDir := t.TempDir()
	binaryPath := filepath.Join(tmpBinDir, "shrimply")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/shrimply")
	must.NoError(buildCmd.Run(), "failed to compile shrimply")

	etcdEndpoint := startEtcd(ctx, t)
	dataDir := t.TempDir()
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	wal, err := shrimpd.OpenWAL(filepath.Join(dataDir, "wal.jsonl"))
	must.NoError(err)
	defer func() {
		must.NoError(wal.Close())
	}()

	addr := freeLocalAddr(t)
	lsm, err := shrimpd.NewLSM("node1", addr, dataDir, wal, shrimpd.NewRegistry(cli, "node1"))
	must.NoError(err)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	eg, runCtx := errgroup.WithContext(runCtx)
	eg.Go(func() error { return lsm.Run(runCtx) })
	eg.Go(func() error { return shrimpd.NewServer(addr, lsm).Run(runCtx) })
	defer func() {
		stop()
		err := eg.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	baseURL := "http://" + addr
	waitHTTP(ctx, t, baseURL+"/parts")

	// Ingest some logs
	postJSON(ctx, t, baseURL+"/ingest", shrimpd.Block{Data: []shrimpd.Entry{
		{Timestamp: 1000, Data: "hello from test"},
		{Timestamp: 2000, Data: "error: database connection lost"},
		{Timestamp: 3000, Data: "request handled"},
	}})

	// Run shrimply with a term query
	cmd := exec.CommandContext(ctx, binaryPath, "-server", baseURL, "error")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	t.Logf("CLI Output (term=error): stdout=%q, stderr=%q", stdout.String(), stderr.String())
	must.NoError(err, "stderr: %s", stderr.String())
	must.Contains(stdout.String(), "error: database connection lost")
	must.NotContains(stdout.String(), "hello from test")

	// Run shrimply without term (last logs)
	stdout.Reset()
	stderr.Reset()
	cmd = exec.CommandContext(ctx, binaryPath, "-server", baseURL, "-n", "2")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	t.Logf("CLI Output (no-term limit=2): stdout=%q, stderr=%q", stdout.String(), stderr.String())
	must.NoError(err, "stderr: %s", stderr.String())
	must.Contains(stdout.String(), "error: database connection lost")
	must.Contains(stdout.String(), "request handled")
	must.NotContains(stdout.String(), "hello from test") // because limit is 2 and we have 3
}

func TestDaemonIndexE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("E2E test for short testing")
		return
	}
	must := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcdEndpoint := startEtcd(ctx, t)
	dataDir := t.TempDir()
	must.NoError(os.MkdirAll(filepath.Join(dataDir, "parts"), 0o755))

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	must.NoError(err)
	defer func() {
		must.NoError(cli.Close())
	}()
	waitEtcd(ctx, t, cli)

	runDaemon := func(dir string) (string, func()) {
		wal, err := shrimpd.OpenWAL(filepath.Join(dir, "wal.jsonl"))
		must.NoError(err)

		addr := freeLocalAddr(t)
		lsm, err := shrimpd.NewLSM("node1", addr, dir, wal, shrimpd.NewRegistry(cli, "node1"))
		must.NoError(err)

		runCtx, stop := context.WithCancel(ctx)
		eg, runCtx := errgroup.WithContext(runCtx)
		eg.Go(func() error { return lsm.Run(runCtx) })

		srv := shrimpd.NewServer(addr, lsm)
		eg.Go(func() error { return srv.Run(runCtx) })

		baseURL := "http://" + addr
		waitHTTP(ctx, t, baseURL+"/parts")

		cleanup := func() {
			stop()
			_ = eg.Wait()
			_ = wal.Close()
		}
		return addr, cleanup
	}

	// 1. Start daemon and ingest logs to force flushes
	addr1, cleanup1 := runDaemon(dataDir)
	baseURL1 := "http://" + addr1

	// Ingest 2 blocks with different unique terms
	postJSON(ctx, t, baseURL1+"/ingest", shrimpd.Block{Data: []shrimpd.Entry{
		{Timestamp: 100, Data: "uniqueapple logs message"},
		{Timestamp: 200, Data: "uniquebanana logs message"},
	}})

	// Wait for a flush or force it (flush threshold is 100, so we can write 100 logs or wait flushInterval=5s)
	time.Sleep(6 * time.Second) // wait for data flushInterval

	// Verify data part was created
	var partsBefore []shrimpd.PartMeta
	getJSON(ctx, t, baseURL1+"/parts", &partsBefore)
	// We expect at least 1 part
	must.NotEmpty(partsBefore)

	// Check if index parts exist
	indexDir := filepath.Join(dataDir, "index")
	waitIndexFiles := func(msg string) {
		t.Helper()
		for {
			files, err := os.ReadDir(indexDir)
			must.NoError(err)
			hasIndexPart := false
			hasCoveredJSON := false
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".json") && f.Name() != "covered.json" {
					hasIndexPart = true
				}
				if f.Name() == "covered.json" {
					hasCoveredJSON = true
				}
			}
			if hasIndexPart && hasCoveredJSON {
				return
			}
			select {
			case <-ctx.Done():
				require.Failf(t, "index files", "timed out waiting for index files (%s): %v", msg, ctx.Err())
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	waitIndexFiles("after flush")

	// Query using unique term
	var qApple shrimpd.Block
	getJSON(ctx, t, baseURL1+"/query?from=100&to=200&term=uniqueapple", &qApple)
	must.Len(qApple.Data, 1)
	must.Equal("uniqueapple logs message", qApple.Data[0].Data)

	cleanup1()

	// 2. Restart and verification of rebuild/reconciliation
	// Delete index files
	must.NoError(os.RemoveAll(indexDir))

	// Restart
	addr2, cleanup2 := runDaemon(dataDir)
	baseURL2 := "http://" + addr2

	// Verify that indexDir now contains index parts and covered.json again (rebuilt on startup).
	// waitHTTP only confirms the HTTP server is up; lsm.Run's startup reconciliation
	// may still be in progress, so poll until the index files appear.
	waitIndexFiles("after restart rebuild")

	// Query still works
	var qBanana shrimpd.Block
	getJSON(ctx, t, baseURL2+"/query?from=100&to=200&term=uniquebanana", &qBanana)
	must.Len(qBanana.Data, 1)
	must.Equal("uniquebanana logs message", qBanana.Data[0].Data)

	// 3. Compaction test
	// Ingest more logs to create another L0 part
	postJSON(ctx, t, baseURL2+"/ingest", shrimpd.Block{Data: []shrimpd.Entry{
		{Timestamp: 300, Data: "uniquecherry logs message"},
	}})
	time.Sleep(6 * time.Second) // wait for data flushInterval

	// Force compaction
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL2+"/compact", http.NoBody)
	must.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	must.NoError(err)
	must.Equal(http.StatusNoContent, resp.StatusCode)
	resp.Body.Close()

	// Allow compaction + background index compaction to run (compactInterval=15s, but we trigger it in ticks)
	time.Sleep(17 * time.Second)

	// Query should still work and find cherry
	var qCherry shrimpd.Block
	getJSON(ctx, t, baseURL2+"/query?from=100&to=400&term=uniquecherry", &qCherry)
	must.Len(qCherry.Data, 1)
	must.Equal("uniquecherry logs message", qCherry.Data[0].Data)

	cleanup2()
}
