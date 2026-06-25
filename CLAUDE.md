# Repository Instructions

## Project Shape
- Go module: `github.com/oteldb/shrimpd`.
- The root package `shrimpd` re-exports filter types; actual logic lives in `internal/`.
- Three CLI binaries under `cmd/`: `shrimpd` (server), `shrimply` (query client), `ch2shrimpd` (ingests logs from oteldb).
- Runtime storage is local JSON part files plus `wal.jsonl`; etcd is the metadata plane for `/lsm/parts/` and `/lsm/nodes/`.
- Key internal packages: `shrimplication` (LSM engine), `shrimpblock` (part/block storage), `shrimpwal` (WAL), `shrimpapi` (HTTP handlers), `shrimpfilter` (label/line filters), `shrimptypes` (shared types).

## Commands
- `make test` runs `./go.test.sh`: normal tests, `-tags purego`, then `-race`, each with `--timeout 5m`.
- `make test_fast` runs only `go test -short ./...` (skips slow/integration tests).
- `make coverage` runs `go test -race -v -coverpkg=./... -coverprofile=profile.out ./...` then `go tool cover -func profile.out`.
- `make tidy` runs `go mod tidy`.
- Focused package check: `go test ./...` is the smallest repo-wide build check.
- Lint: `golangci-lint run ./...`

## Local Run Notes
- The CLI expects etcd at `localhost:2379` by default; example dependency: `docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest`.
- CLI flags from `cmd/shrimpd/main.go`: `-id`, `-addr`, `-data`, and comma-separated `-etcd` endpoints.
- HTTP API routes are registered with Go 1.22 method/path patterns: `POST /ingest`, `GET /query`, `GET /part/{id}`, `GET /parts`.
- E2E tests live in `e2e/` and spin up etcd via testcontainers — Docker must be available. They are skipped with `-short`.

## Lint And CI
- CI uses reusable workflows from `go-faster/x` for test, cover, lint, commit checks, and CodeQL; dependency review runs on pull requests.
- `.golangci.yml` enables `goimports` and `gofumpt`; its `goimports.local-prefixes` is currently `github.com/go-faster/gooners`, which does not match this module path.
- The lint config uses `golangci-lint` v2 syntax and enables strict linters including `gosec`, `gocritic`, `modernize`, `revive`, and `staticcheck`; test files have several linter exclusions.

## Agent Workflow Notes
- Do not treat generated-code exclusions in `.golangci.yml` as evidence of generated files; none are present in the current tree.
