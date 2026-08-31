.PHONY: dev dev-api dev-web build install init test test-mysql-matrix test-postgres-matrix test-search-matrix test-mongo-matrix test-rabbit-matrix lint fmt run snapshot clean

GO_CACHE ?= /tmp/portscope-go-cache
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_LDFLAGS := -X github.com/erikbooij/portscope/internal/buildinfo.Version=$(VERSION) -X github.com/erikbooij/portscope/internal/buildinfo.Commit=$(COMMIT) -X github.com/erikbooij/portscope/internal/buildinfo.Date=$(BUILD_DATE)

dev:
	@$(MAKE) dev-api & api_pid=$$!; trap 'kill $$api_pid 2>/dev/null || true' INT TERM EXIT; $(MAKE) dev-web

dev-api:
	GOCACHE=$(GO_CACHE) go run -ldflags "$(GO_LDFLAGS)" .

dev-web:
	npm run dev

run: build
	./portscope

build:
	npm run build
	GOCACHE=$(GO_CACHE) go build -trimpath -ldflags "$(GO_LDFLAGS)" -o portscope .

install:
	npm run build
	GOCACHE=$(GO_CACHE) go install -trimpath -ldflags "$(GO_LDFLAGS)" .

init:
	GOCACHE=$(GO_CACHE) go run . init

test:
	npm run build
	GOCACHE=$(GO_CACHE) go test ./...

test-mysql-matrix:
	./scripts/test-mysql-matrix.sh

test-postgres-matrix:
	./scripts/test-postgres-matrix.sh

test-search-matrix:
	./scripts/test-search-matrix.sh

test-mongo-matrix:
	./scripts/test-mongo-matrix.sh

test-rabbit-matrix:
	./scripts/test-rabbit-matrix.sh

lint:
	npm run lint
	@test -z "$$(gofmt -l main.go internal)"
	GOCACHE=$(GO_CACHE) go vet ./...

fmt:
	gofmt -w main.go internal

snapshot:
	goreleaser release --snapshot --clean

clean:
	go clean
	rm -rf portscope dist
