package main

import (
	"flag"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "HTTP listen address")
		upstreams = flag.String("upstreams", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated shrimpd upstream URLs")
	)
	flag.Parse()

	targets, err := parseUpstreams(*upstreams)
	if err != nil {
		slog.Error("parse upstreams", "error", err)
		os.Exit(1)
	}
	if len(targets) == 0 {
		slog.Error("no upstreams configured")
		os.Exit(1)
	}

	proxy := newGateway(targets)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           proxy,
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("gateway listening", "addr", *addr, "upstreams", len(targets))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("gateway exit", "error", err)
		os.Exit(1)
	}
}

func parseUpstreams(raw string) ([]*url.URL, error) {
	parts := strings.Split(raw, ",")
	targets := make([]*url.URL, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		target, err := url.Parse(part)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func newGateway(targets []*url.URL) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	var next uint32

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := targets[(atomic.AddUint32(&next, 1)-1)%uint32(len(targets))]
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.Transport = transport
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			slog.Warn("gateway upstream error", "method", r.Method, "path", r.URL.Path, "upstream", target.String(), "error", err)
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		}
		proxy.ServeHTTP(w, r)
	})
}
