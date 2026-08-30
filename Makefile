.PHONY: dev dev-api dev-web build test test-mysql-matrix test-postgres-matrix test-search-matrix test-mongo-matrix test-rabbit-matrix lint fmt run demo clean

GO_CACHE ?= /tmp/portscope-go-cache

dev:
	@$(MAKE) dev-api & api_pid=$$!; trap 'kill $$api_pid 2>/dev/null || true' INT TERM EXIT; $(MAKE) dev-web

dev-api:
	GOCACHE=$(GO_CACHE) go run ./cmd/portscope --data data

dev-web:
	npm run dev

run: build
	./portscope --data data

build:
	npm run build
	GOCACHE=$(GO_CACHE) go build -o portscope ./cmd/portscope

test:
	npm run build
	GOCACHE=$(GO_CACHE) go test ./cmd/... ./internal/...

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
	@test -z "$$(gofmt -l cmd internal)"
	GOCACHE=$(GO_CACHE) go vet ./cmd/... ./internal/...

fmt:
	gofmt -w cmd internal

demo:
	curl -fsS -X POST http://127.0.0.1:8090/api/demo

clean:
	go clean
	rm -f portscope
