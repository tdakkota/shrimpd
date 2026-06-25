package e2e

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

func requireE2E(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("E2E test disabled in short mode")
	}
	if os.Getenv("E2E") != "1" {
		t.Skip("E2E test disabled; set E2E=1 to enable")
	}
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
