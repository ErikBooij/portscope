# Development

## Requirements

- Go 1.26.6 or newer
- Node.js 24 or newer
- Docker for live compatibility matrices

Install frontend dependencies and run the main checks:

```bash
npm ci
make test
make lint
```

## Common commands

| Command | Purpose |
| --- | --- |
| `make run` | Build the frontend and run Portscope on `127.0.0.1:8090` |
| `make dev` | Run the API on `:8090` and Vite hot reload on `:5173` |
| `make test` | Build the production frontend and run Go unit/integration tests |
| `make lint` | Run TypeScript checks, `gofmt`, and `go vet` |
| `make build` | Produce one binary with the React interface embedded |
| `make install` | Install the current checkout into `GOBIN` |
| `make snapshot` | Build local GoReleaser archives in `dist/` |

If the frontend changes, commit the regenerated `internal/webui/dist` output.
CI verifies that the embedded output matches the source.

## Protocol compatibility matrices

The matrix commands run maintained real servers in disposable Docker containers:

```bash
make test-mysql-matrix
make test-postgres-matrix
make test-search-matrix
make test-mongo-matrix
make test-rabbit-matrix
```

They cover MySQL 5.6 through current, PostgreSQL 14 through 18, Elasticsearch 8/9
and OpenSearch 2/3, authenticated MongoDB 6.0 through 8.3, and RabbitMQ 3.13
through 4.3. Exact targets live in `scripts/` and `.github/workflows/`.

Protocol changes should include bounded parsing tests, credential-redaction
coverage, and a live compatibility test when a maintained image is available.
Update the relevant compatibility contract in this directory at the same time.

## Docker development

```bash
docker compose up --build
```

The Compose file mounts `portscope.json` and `.portscope/`. Add an explicit port
mapping for every listener being tested. A container-local `127.0.0.1` target
refers to the container, not the host.

## Releases

Push a semantic-version tag such as `v0.1.0` from a clean `main` commit. The
release workflow rebuilds the embedded frontend, cross-compiles static macOS,
Linux, and Windows binaries for amd64 and arm64, publishes platform archives, and
attaches a SHA-256 checksum manifest to the GitHub Release. Pre-release tags stay
marked as pre-releases.

Read [CONTRIBUTING.md](../CONTRIBUTING.md) before opening a pull request and
[Architecture](architecture.md) before changing adapter boundaries.

[Back to the documentation index](README.md).
