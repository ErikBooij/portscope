# Portscope

Portscope is a local inspection proxy. Applications connect to Portscope instead of directly to an upstream; Portscope forwards the traffic and shows the operation, request, response, outcome, size, connection, and timing live in a dense web interface.

Portscope currently has two real protocol adapters:

- **HTTP/1.1 + HTTP/2 + WebSocket:** HTTP/1.1, cleartext HTTP/2 (`h2c`), and HTTP/2 over TLS on both sides; classic RFC 6455 WebSocket upgrades over HTTP or HTTPS; streaming capture up to 256 KiB per body or WebSocket frame; trailers; JSON rendering; request/response header policies; custom CAs; mutual TLS; and automatic or configured credential redaction.
- **Redis RESP2/RESP3:** nested frame parsing, pipeline correlation, error and push-frame observation, 64 MiB frame bounds, password-only or ACL `AUTH`, database selection, TLS with custom CAs, mutual TLS, and secret-safe capture.

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
make lint      # TypeScript, gofmt, and go vet
make build     # one binary with the React UI embedded
```

Docker is also supported with `docker compose up --build`. Add an explicit Compose port mapping for every configured proxy listener; container-local `127.0.0.1` targets refer to the container, not the host.

## Configure an application

For HTTP, create an upstream such as listen `127.0.0.1:9000` → target `http://127.0.0.1:3000`, then point the application at `http://127.0.0.1:9000`.

For Redis, create listen `127.0.0.1:6380` → target `127.0.0.1:6379`, then use `redis://127.0.0.1:6380` in the application.

HTTP targets accept `http://`, `https://`, and `h2c://`. HTTPS negotiates HTTP/2 automatically. A TLS listener serves HTTP/1.1 and HTTP/2 via ALPN; a plaintext listener accepts HTTP/1.1 and h2c. Header policies are ordered and can set, append, or remove request and response headers. Mark injected credentials as sensitive so their values remain write-only in the API and redacted in captures.

WebSocket upgrades are detected inside an HTTP upstream and proxied over `ws` or `wss` according to the target’s `http://` or `https://` scheme. The opening handshake and every frame in both directions appear live as separate interactions. Text, JSON, binary, continuation, ping, pong, and close frames retain their connection identity, direction, opcode, FIN/mask state, size, and timing. Large frames continue streaming after the 256 KiB inspection cap.

Redis authentication can use `AUTH <password>` or Redis 6+ ACL-style `AUTH <username> <password>`. When configured, Portscope authenticates and optionally selects a database before any application command is forwarded. Enable Redis TLS for `rediss`-style upstreams; private CAs, an SNI override, and client certificates are supported.

Use an upstream’s `•••` action to edit it. Configuration updates restart only the affected proxy listener; captured history remains available. The app binds its UI and seeded listeners to loopback by default.

## Architecture and limits

The key design is documented in [docs/architecture.md](docs/architecture.md). Protocol adapters share lifecycle and the observation envelope, not parser or session semantics.

Current honest limits:

- HTTP CONNECT tunnels and RFC 8441 WebSockets over HTTP/2 extended CONNECT are not implemented. Classic RFC 6455 upgrades are supported over HTTP/1.1, including TLS on either side. Negotiated WebSocket extensions pass through unchanged; extension-transformed payloads such as per-message-deflate are captured as bytes rather than decompressed. gRPC can traverse HTTP/2, including trailers, but Portscope shows framed bytes rather than decoding protobuf messages.
- Redis cluster redirection topology and transaction-level grouping are not yet modeled. RESP3 push frames are captured, but their relationship to subscriptions is not modeled beyond the connection. Redis downstream listener TLS is not yet exposed; TLS support is for the upstream connection.
- Body capture is capped and binary bodies are described rather than rendered. Forwarding continues after the capture cap.
- Portscope is a local development tool. The dashboard has same-origin controls and defensive browser headers, but no user authentication; keep its management listener on loopback or behind your own authenticated ingress.
- PostgreSQL and MySQL are deliberately not advertised yet; both need first-class stateful protocol adapters rather than a thin wrapper around the existing ones.

## Security behavior

Configuration is written atomically with owner-only permissions. Redis passwords and sensitive injected header values are returned by the API only as a “value is set” marker, so editing a configuration preserves a secret without disclosing it to the browser. TLS verification uses system roots unless a custom CA is configured, requires TLS 1.2 or newer, and supports mutual TLS. Certificate-verification bypass is available for diagnosis but deliberately marked dangerous in the editor.

The interaction journal is periodically compacted to the configured retention window instead of growing forever. The health endpoint reports a degraded status if capture persistence fails. HTTP header mutation rejects hop-by-hop/framing headers and line breaks; Redis framing has nesting and size bounds.
