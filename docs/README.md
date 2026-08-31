# Portscope documentation

This is the documentation map for Portscope. If you are new to the project,
start with the first capture guide; if you are evaluating a specific adapter,
read its compatibility contract before depending on it.

## Start here

- [Getting started](getting-started.md) — install the binary, initialize a
  repository, and inspect HTTP or Redis traffic.
- [Project configuration](configuration.md) — understand `portscope.json`, file
  discovery, environment-backed secrets, and command-line overrides.
- [Protocols and upstreams](protocols.md) — choose an adapter and connect an
  application to it.

## Understand Portscope

- [Architecture](architecture.md) — the shared adapter contract, lifecycle,
  observation model, and persistence boundaries.
- [Security model](security.md) — authentication ownership, secret handling,
  redaction, TLS, and management-interface exposure.
- [Known limitations](limitations.md) — unsupported features and deliberate
  boundaries, organized by protocol.

## Compatibility contracts

These documents describe the exact versions, authentication modes, wire
features, inspection behavior, and test matrices promised by each specialized
adapter.

- [MySQL](mysql-compatibility.md)
- [PostgreSQL](postgres-compatibility.md)
- [Elasticsearch and OpenSearch](search-compatibility.md)
- [gRPC](grpc-compatibility.md)
- [MongoDB](mongodb-compatibility.md)
- [RabbitMQ](rabbitmq-compatibility.md)

HTTP, WebSocket, and Redis behavior is documented in
[Protocols and upstreams](protocols.md#http-and-websocket) and
[Protocols and upstreams](protocols.md#redis).

## Build and contribute

- [Development](development.md) — local requirements, common commands, matrix
  tests, Docker, and releases.
- [Contributing](../CONTRIBUTING.md) — pull-request expectations and protocol
  change requirements.
- [Security policy](../SECURITY.md) — report a vulnerability privately.

[Return to the project README](../README.md).
