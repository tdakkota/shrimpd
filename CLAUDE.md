# Repository Instructions

## Project Shape
- Go module: `github.com/tdakkota/shrimpd`.
- The importable implementation lives in the repository root as package `shrimpd`; `cmd/shrimpd/main.go` is the CLI entrypoint and wires the root package API.
- Runtime storage is local JSON part files plus `wal.jsonl`; etcd is the metadata plane for `/lsm/parts/` and `/lsm/nodes/`.

## Commands
- `make test` runs `./go.test.sh`: normal tests, `-tags purego`, then `-race`, each with `--timeout 5m`.
- `make test_fast` runs only `go test ./...`.
- `make coverage` runs `go test -race -v -coverpkg=./... -coverprofile=profile.out ./...` then `go tool cover -func profile.out`.
- `make tidy` runs `go mod tidy`.
- Focused package check: `go test ./...` is the smallest repo-wide build check.

## Local Run Notes
- The CLI expects etcd at `localhost:2379` by default; example dependency: `docker run -p 2379:2379 -e ALLOW_NONE_AUTHENTICATION=yes bitnami/etcd:latest`.
- CLI flags from `cmd/shrimpd/main.go`: `-id`, `-addr`, `-data`, and comma-separated `-etcd` endpoints.
- HTTP API routes are registered with Go 1.22 method/path patterns: `POST /ingest`, `GET /query`, `GET /part/{id}`, `GET /parts`.

## Lint And CI
- CI uses reusable workflows from `go-faster/x` for test, cover, lint, commit checks, and CodeQL; dependency review runs on pull requests.
- `.golangci.yml` enables `goimports` and `gofumpt`; its `goimports.local-prefixes` is currently `github.com/go-faster/gooners`, which does not match this module path.
- The lint config uses `golangci-lint` v2 syntax and enables strict linters including `gosec`, `gocritic`, `modernize`, `revive`, and `staticcheck`; test files have several linter exclusions.

## Agent Workflow Notes
- There are no `_test.go` files yet; add targeted tests when fixing behavior instead of assuming an existing test harness.
- Do not treat generated-code exclusions in `.golangci.yml` as evidence of generated files; none are present in the current tree.
