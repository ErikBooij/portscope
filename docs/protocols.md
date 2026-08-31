# Protocols and upstreams

All adapters publish the same observation shape to the live dashboard, but each
owns its wire framing, session state, authentication, redaction, and request to
response correlation. This page helps you choose and configure an adapter. The
linked compatibility contracts define its exact support boundary.

## HTTP and WebSocket

Create an HTTP upstream such as listen `127.0.0.1:9000` to target
`http://127.0.0.1:3000`, then point the application at
`http://127.0.0.1:9000`.

Targets accept `http://`, `https://`, and `h2c://`. HTTPS negotiates HTTP/2
automatically. A TLS listener serves HTTP/1.1 and HTTP/2 through ALPN; a plaintext
listener accepts HTTP/1.1 and h2c. Capture streams up to 256 KiB per body while
forwarding continues, preserves trailers, and renders JSON when possible.

Header policies run in order and can set, append, or remove request and response
headers. Mark injected credentials as sensitive so the values remain write-only
in the API and redacted in captures. Custom certificate authorities, mutual TLS,
and automatic or configured credential redaction are supported.

Classic RFC 6455 upgrades are detected within an HTTP upstream and proxied over
`ws` or `wss` according to the target scheme. The handshake and every frame in
both directions appear as separate interactions. Text, JSON, binary,
continuation, ping, pong, and close frames retain their connection identity,
direction, opcode, FIN/mask state, size, and timing. Large frames keep streaming
after the 256 KiB inspection cap.

## Redis

Create listen `127.0.0.1:6380` to target `127.0.0.1:6379`, configure the
application-facing and upstream credentials independently, then use
`redis://127.0.0.1:6380` in the application.

The adapter understands nested RESP2 and RESP3 frames, correlates ordered
pipelines, observes errors and push frames, and enforces a 64 MiB frame bound.
It accepts `AUTH <password>`, Redis 6+ `AUTH <username> <password>`, and
`HELLO ... AUTH`. Portscope answers authentication locally, strips authentication
from `HELLO`, then authenticates upstream with its separate credentials.

`SELECT` is accepted locally only for the configured database so the application
cannot silently move the owned upstream session. Listener TLS and upstream TLS
are independent; private CAs, SNI overrides, and mutual TLS are supported.

## MySQL

Configure separate application-facing and upstream credentials and point the
application at the listener with the listener username and password. Portscope
authenticates that session locally, then opens its own upstream session using the
configured database identity and default schema. Neither password is returned
to the browser after storage.

The classic-protocol adapter supports MySQL 5.6 through current releases,
including 5.7, 8.0, 8.4 LTS, 9.7 LTS, and 26.7. It inspects text and binary
results, prepared parameters, long-data accounting, errors, affected rows, and
multiple results. Listener and upstream TLS are independent.

Read the [MySQL compatibility contract](mysql-compatibility.md).

## PostgreSQL

Configure separate application-facing and upstream credentials plus the upstream
database. Applications authenticate to Portscope with SCRAM-SHA-256; Portscope
then opens its own upstream session and handles trust, cleartext, MD5, or SCRAM
authentication. The startup identity is replaced rather than passed through,
while safe parameters such as `application_name`, `client_encoding`, and
`TimeZone` are retained.

The protocol-v3 adapter supports PostgreSQL 14 through 18, classic and direct
listener TLS, verified upstream TLS, simple and extended queries, prepared
parameters, text and binary rows, pipelines, COPY accounting, notifications, and
SQLSTATE errors. Cancellation keys are rewritten and mapped to a separate,
TLS-protected upstream cancellation connection when upstream TLS is enabled.

Read the [PostgreSQL compatibility contract](postgres-compatibility.md).

## Elasticsearch and OpenSearch

Choose the Search upstream type and target the cluster's HTTP endpoint. The
adapter uses the hardened HTTP transport while classifying search, count,
document, bulk, multi-search, scroll, reindex, cluster-health, and CAT operations.
It captures structured JSON/NDJSON and summarizes indexes, documents, server
`took`, hits, shard state, and partial failures.

API keys, bearer tokens, and basic credentials should be injected through
sensitive request-header rules. TLS, HTTP/2, header policies, and write-only
credential values work as they do for HTTP.

Read the [Search compatibility contract](search-compatibility.md).

## gRPC

Choose the gRPC upstream and use an `h2c://` target for cleartext HTTP/2 or
`https://` for TLS. The listener accepts h2c by default or HTTP/2 over TLS when
listener TLS is configured. Metadata policies can inject write-only authorization
values.

Without a schema, Portscope still inspects calls, arbitrary DATA-frame envelope
boundaries, unary and all streaming modes, compression, trailers, canonical
status, status messages, status details, and deadlines. To render Protobuf bodies
as JSON, create a self-contained descriptor set and configure its project-relative
path:

```bash
protoc --include_imports --descriptor_set_out=service.protoset path/to/service.proto
```

Read the [gRPC compatibility contract](grpc-compatibility.md).

## MongoDB

Configure optional application-facing SCRAM credentials independently from the
optional upstream SCRAM identity. Portscope answers application `hello` and
authentication locally, advertises itself as a direct server so drivers cannot
bypass inspection, and authenticates upstream only after the listener identity
succeeds.

Point the application at `mongodb://<listener>/` with `directConnection=true`.
The adapter supports MongoDB 6.0 through 8.3, `OP_MSG`, legacy handshake parsing,
SCRAM-SHA-256/SHA-1, document sequences, request-ID correlation, Extended JSON,
and cursor/write summaries. Post-auth forwarding preserves sessions,
transactions, cursors, change streams, and write concerns. TLS and mutual TLS are
independent on either leg.

Read the [MongoDB compatibility contract](mongodb-compatibility.md).

## RabbitMQ

Configure application-facing and broker-facing PLAIN credentials plus a virtual
host for each leg. Portscope validates the listener identity and virtual host
locally, then opens its separately configured broker identity and virtual host.
Use `amqp://` for plaintext listeners or `amqps://` with listener TLS.

The AMQP 0-9-1 adapter supports RabbitMQ 3.13 through 4.3, bounded frame/table
parsing, channel-aware RPC correlation, content method/header/body assembly, JSON
and message-property capture, publisher confirms, consumer delivery,
acknowledgements, returned messages, transactions, and heartbeats.

Read the [RabbitMQ compatibility contract](rabbitmq-compatibility.md).

## Editing a running upstream

Use an upstream's `•••` action in the browser. Configuration updates restart only
the affected listener, and captured history remains available. New listeners
bind to loopback by default.

Before depending on a specialized feature, review [Known limitations](limitations.md).

[Back to the documentation index](README.md).
