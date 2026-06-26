package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/sdk/app"
	slogzap "github.com/samber/slog-zap/v2"
	"go.uber.org/zap"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "HTTP listen address")
		upstreams = flag.String("upstreams", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated shrimpd upstream URLs")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	app.Run(func(ctx context.Context, lg *zap.Logger, _ *app.Telemetry) error {
		slogLogger := slog.New(slogzap.Option{Level: slog.LevelInfo, Logger: lg}.NewZapHandler())
		slog.SetDefault(slogLogger)

		targets, err := parseUpstreams(*upstreams)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			return errors.New("no upstreams configured")
		}

		proxy := newGateway(targets, slogLogger)
		srv := &http.Server{
			Addr:              *addr,
			Handler:           proxy,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				slogLogger.Warn("gateway shutdown", "error", err)
			}
		}()

		slogLogger.Info("gateway listening", "addr", *addr, "upstreams", len(targets))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ctx.Err()
	}, app.WithContext(ctx), app.WithServiceName("shrimpgateway"))
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

func newGateway(targets []*url.URL, logger *slog.Logger) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	var next atomic.Uint32

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w}

		target := targets[(next.Add(1)-1)%uint32(len(targets))]
		fields := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"content_length", r.ContentLength,
			"content_length_human", humanize.Bytes(uint64(r.ContentLength)),
			"upstream", target.String(),
		}
		if r.Header.Get("Content-Type") != "" {
			fields = append(fields, "content_type", r.Header.Get("Content-Type"))
		}
		logger.Debug("gateway request received", fields...)
		requestLogger := logger.With(fields...)

		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.Transport = transport
		proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
			requestLogger.Warn("gateway upstream error", "error", err)
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		}
		proxy.ServeHTTP(lw, r)
		requestLogger.Info("gateway request",
			"status", lw.status,
			"response_length", lw.bytes,
			"response_length_human", humanize.Bytes(uint64(lw.bytes)),
			"duration", time.Since(start).String(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *loggingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}
