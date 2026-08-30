# Portscope

Portscope is a local inspection proxy. Applications connect to Portscope instead of directly to an upstream; Portscope forwards the traffic and shows the operation, request, response, outcome, size, connection, and timing live in a dense web interface.

Portscope currently has seven real protocol adapters:

- **HTTP/1.1 + HTTP/2 + WebSocket:** HTTP/1.1, cleartext HTTP/2 (`h2c`), and HTTP/2 over TLS on both sides; classic RFC 6455 WebSocket upgrades over HTTP or HTTPS; streaming capture up to 256 KiB per body or WebSocket frame; trailers; JSON rendering; request/response header policies; custom CAs; mutual TLS; and automatic or configured credential redaction.
- **Elasticsearch + OpenSearch:** a semantic profile over the hardened HTTP transport; search, count, document, bulk, multi-search, scroll, reindex, cluster-health, and CAT operation classification; index/document metadata; server `took`, hits, shard state, and partial-failure summaries; structured NDJSON capture; TLS, HTTP/2, header policies, and write-only injected credentials.
- **Redis RESP2/RESP3:** terminated listener authentication, an independently authenticated upstream session, nested frame parsing, ordered pipeline correlation, error and push-frame observation, 64 MiB frame bounds, locally enforced database state, TLS and mutual TLS on either leg, and secret-safe capture.
- **MySQL classic protocol:** MySQL 5.6 through current (including 5.7, 8.0, 8.4 LTS, 9.7 LTS, and 26.7); terminated listener authentication; an independently authenticated upstream session; TLS on either leg; `mysql_native_password` listener verification; upstream native, `sha256_password`, and `caching_sha2_password` authentication; text and binary result decoding; prepared-parameter inspection; safe long-data accounting; errors and affected-row summaries; and multi-result handling.
- **PostgreSQL protocol v3:** PostgreSQL 14 through 18; terminated SCRAM-SHA-256 listener authentication; an independently authenticated upstream session using trust, cleartext, MD5, or SCRAM; classic and direct listener TLS; verified upstream TLS; simple and extended-query inspection; prepared parameters; text and binary rows; pipelines; COPY forwarding/accounting; notifications; SQLSTATE errors; and securely rewritten cancellation keys.
- **MongoDB wire protocol:** MongoDB 6.0 through 8.3; modern `OP_MSG` plus legacy handshake parsing; terminated SCRAM-SHA-256/SHA-1 listener authentication; delayed, independently authenticated upstream SCRAM; direct TLS and mutual TLS on either leg; raw post-auth forwarding that preserves sessions, transactions, cursors, change streams, and write concerns; document-sequence inspection; request-ID correlation; Extended JSON capture; and cursor/write-result summaries.
- **RabbitMQ AMQP 0-9-1:** RabbitMQ 3.13 through 4.3; terminated listener PLAIN authentication and virtual-host policy; delayed, independently authenticated broker connection with a separate identity and virtual host; direct TLS and mutual TLS on either leg; bounded frame/table parsing; channel-aware RPC correlation; content method/header/body assembly; JSON and message-property capture; publisher confirms, consumer delivery, acknowledgements, returned messages, transactions, and heartbeats.

On first run Portscope creates an **Echo Lab** HTTP upstream on `127.0.0.1:9081`. “Generate traffic” sends three real requests through that proxy, so the interface is useful immediately.

## Run

Requirements: Go 1.26+, Node 22+.

```bash
npm ci
make run
```

Open <http://127.0.0.1:8090>. Configuration and the newest 5,000 interactions live under `./data` and survive restarts.

For frontend hot reload:

```bash
make dev       # API on :8090, Vite UI on :5173
make test      # frontend production build + Go unit/integration tests
make test-mysql-matrix # disposable real servers from MySQL 5.6 through current
make test-postgres-matrix # disposable PostgreSQL 14 through 18 servers
make test-search-matrix # maintained Elasticsearch 8/9 and OpenSearch 2/3
make test-mongo-matrix # authenticated MongoDB 6.0, 7.0, 8.0, and 8.3
make test-rabbit-matrix # RabbitMQ 3.13 and every released 4.x line through 4.3
make lint      # TypeScript, gofmt, and go vet
make build     # one binary with the React UI embedded
```

Docker is also supported with `docker compose up --build`. Add an explicit Compose port mapping for every configured proxy listener; container-local `127.0.0.1` targets refer to the container, not the host.

## Configure an application

For HTTP, create an upstream such as listen `127.0.0.1:9000` → target `http://127.0.0.1:3000`, then point the application at `http://127.0.0.1:9000`.

For Redis, create listen `127.0.0.1:6380` → target `127.0.0.1:6379`, configure the application-facing and upstream credentials independently, then use `redis://127.0.0.1:6380` in the application. Portscope validates application `AUTH` locally before opening an authenticated upstream session.

For MySQL, configure separate application-facing and upstream credentials. Point the application at Portscope’s listen address using the listener username/password; Portscope authenticates that session locally, then creates its own upstream session with the configured database username/password and default schema. Neither password is returned to the browser after storage.

For PostgreSQL, configure separate application-facing and upstream credentials plus the upstream database. The application always authenticates to Portscope with SCRAM-SHA-256; Portscope then starts its own upstream session and handles the server’s configured password method. Startup identity is replaced rather than passed through, while safe session parameters such as `application_name`, `client_encoding`, and `TimeZone` are retained. Cancellation keys are replaced locally and mapped back over a separate, TLS-protected upstream cancellation connection when upstream TLS is enabled.

For Elasticsearch or OpenSearch, choose the Search upstream type and point it at the cluster's HTTP endpoint. It retains the normal HTTP proxy controls but interprets REST paths and JSON/NDJSON bodies as search operations. API keys, bearer tokens, and basic credentials should be injected as sensitive request-header rules so they remain write-only and redacted from captures.

For MongoDB, configure optional application-facing SCRAM credentials separately from the optional upstream SCRAM identity. Portscope answers application `hello` and authentication locally, advertises itself as a direct server so drivers cannot bypass the proxy, and starts upstream authentication only after the listener identity succeeds. Point the application at `mongodb://<listener>/` with `directConnection=true`; TLS can be enabled independently on either leg.

For RabbitMQ, configure required application-facing and broker-facing PLAIN credentials plus a virtual host for each leg. The application opens the listener virtual host; Portscope validates it locally, then authenticates to and opens the separately configured broker virtual host. Use `amqp://` for plaintext listeners or `amqps://` when listener TLS is enabled. Broker credentials are not sent until listener authentication succeeds.

HTTP targets accept `http://`, `https://`, and `h2c://`. HTTPS negotiates HTTP/2 automatically. A TLS listener serves HTTP/1.1 and HTTP/2 via ALPN; a plaintext listener accepts HTTP/1.1 and h2c. Header policies are ordered and can set, append, or remove request and response headers. Mark injected credentials as sensitive so their values remain write-only in the API and redacted in captures.

WebSocket upgrades are detected inside an HTTP upstream and proxied over `ws` or `wss` according to the target’s `http://` or `https://` scheme. The opening handshake and every frame in both directions appear live as separate interactions. Text, JSON, binary, continuation, ping, pong, and close frames retain their connection identity, direction, opcode, FIN/mask state, size, and timing. Large frames continue streaming after the 256 KiB inspection cap.

Redis authentication can use `AUTH <password>`, Redis 6+ ACL-style `AUTH <username> <password>`, or `HELLO ... AUTH`. Listener credentials never reach Redis: Portscope answers `AUTH` locally and strips the authentication clause from `HELLO` before forwarding it. It then authenticates upstream with its separate credentials. `SELECT` is locally accepted only for the configured database, so the application cannot silently move the owned upstream session. Enable listener TLS for `rediss://` application connections and upstream TLS independently; private CAs, SNI overrides, and mutual TLS are supported.

Use an upstream’s `•••` action to edit it. Configuration updates restart only the affected proxy listener; captured history remains available. The app binds its UI and seeded listeners to loopback by default.

## Architecture and limits

The key design is documented in [docs/architecture.md](docs/architecture.md). Protocol adapters share lifecycle and the observation envelope, not parser or session semantics. Exact compatibility contracts are in [docs/mysql-compatibility.md](docs/mysql-compatibility.md), [docs/postgres-compatibility.md](docs/postgres-compatibility.md), [docs/search-compatibility.md](docs/search-compatibility.md), [docs/mongodb-compatibility.md](docs/mongodb-compatibility.md), and [docs/rabbitmq-compatibility.md](docs/rabbitmq-compatibility.md).

Current honest limits:

- HTTP CONNECT tunnels and RFC 8441 WebSockets over HTTP/2 extended CONNECT are not implemented. Classic RFC 6455 upgrades are supported over HTTP/1.1, including TLS on either side. Negotiated WebSocket extensions pass through unchanged; extension-transformed payloads such as per-message-deflate are captured as bytes rather than decompressed. gRPC can traverse HTTP/2, including trailers, but Portscope shows framed bytes rather than decoding protobuf messages.
- Redis cluster redirection topology and transaction-level grouping are not yet modeled. RESP3 push frames are captured, but their relationship to subscriptions is not modeled beyond the connection. `RESET` is rejected because Portscope owns authentication and database state.
- MySQL compression, query attributes, optional resultset metadata, and LOCAL INFILE are deliberately not negotiated yet. `COM_CHANGE_USER` and replication commands are rejected without touching the upstream session. Common prepared-parameter and binary-row types are decoded; unknown or newer binary types remain observable as undecoded payloads rather than risking incorrect values.
- PostgreSQL replication connections, GSS/SSPI, OAuth, certificate-only authentication, channel-binding SCRAM-PLUS, and function-call inspection are not implemented. Protocol 3 clients are downgraded to minor version 0 when needed. COPY streams are forwarded without buffering their full contents; their byte counts and final command status are attached to the initiating query.
- Elasticsearch/OpenSearch semantic inspection covers their common REST vocabulary and JSON/NDJSON formats. Product-specific plugins, CBOR, and SMILE bodies still pass through correctly but remain generic body captures.
- MongoDB is intentionally a direct-target proxy: replica-set discovery addresses, mongos identity, load-balanced service IDs, and compression are removed from the application-facing `hello` so traffic cannot bypass inspection and BSON remains visible. The configured target can itself be a `mongod` or `mongos`, but Portscope does not provide multi-host failover. SCRAM-SHA-256 and SCRAM-SHA-1 are supported; X.509, AWS IAM, OIDC, Kerberos, and LDAP authentication are not yet terminated. Post-auth wire messages are otherwise forwarded unchanged.
- RabbitMQ support is AMQP 0-9-1 over TCP/TLS, not AMQP 1.0, streams, MQTT, or STOMP. Connection setup is terminated only for PLAIN authentication and virtual-host mapping; post-open frames remain broker-owned and are forwarded unchanged. SASL challenge mechanisms and OAuth are not terminated. Captures assemble Basic content and common methods but deliberately do not reinterpret broker extensions or message encodings beyond JSON detection.
- Body capture is capped and binary bodies are described rather than rendered. Forwarding continues after the capture cap.
- Portscope is a local development tool. The dashboard has same-origin controls and defensive browser headers, but no user authentication; keep its management listener on loopback or behind your own authenticated ingress.

## Security behavior

Configuration is written atomically with owner-only permissions. Redis, MySQL, PostgreSQL, MongoDB, and RabbitMQ passwords for both connection legs, plus sensitive injected header values, are returned by the API only as a “value is set” marker, so editing a configuration preserves a secret without disclosing it to the browser. TLS verification uses system roots unless a custom CA is configured, requires TLS 1.2 or newer, and supports mutual TLS. Certificate-verification bypass is available for diagnosis but deliberately marked dangerous in the editor.

The interaction journal is periodically compacted to the configured retention window instead of growing forever. The health endpoint reports a degraded status if capture persistence fails. HTTP header mutation rejects hop-by-hop/framing headers and line breaks; Redis framing has nesting and size bounds.
