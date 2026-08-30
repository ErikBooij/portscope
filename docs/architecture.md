# Architecture

Portscope is a cohesive Go process: the web/API listener, configured protocol listeners, runtime manager, observation store, SSE fan-out, and embedded React application share one lifecycle.

## The protocol seam

The external seam is intentionally small:

```go
type Adapter interface {
    Run(context.Context, config.Upstream, observation.Sink, func(address string)) error
}
```

An adapter owns everything callers should not need to understand: listening, upstream connection behavior, framing, correlation, redaction, error classification, protocol limits, and shutdown. The runtime manager only starts adapters, observes readiness/failure, and restarts them after configuration changes.

The shared `Interaction` is an observation envelope, not a universal query abstraction. It standardizes identity, time, duration, outcome, request/response payloads, and searchable attributes. `Operation`, payload kind, and attributes retain protocol vocabulary. The UI can therefore offer shared filtering and timing while adding a protocol-specific renderer without teaching the manager or store about that protocol.

This passes the two-adapter test today: HTTP and Redis differ substantially behind the same small interface.

## Why Postgres and MySQL remain substantial

A future PostgreSQL adapter must model startup and optional TLS negotiation, authentication, simple queries, the extended Parse/Bind/Describe/Execute/Sync flow, prepared-statement and portal identity, COPY modes, asynchronous notices, cancellation, and connection/session state. Timing a single frontend message is not necessarily timing a query.

A MySQL adapter separately needs handshake and capability negotiation, authentication plugins, optional TLS and compression, text queries, binary prepared-statement execution, multi-result responses, server status flags, and session state. Its packet sequence and correlation rules do not map onto PostgreSQL or Redis.

Those adapters should produce the same `Interaction` envelope only after their own state machines have identified a meaningful operation. Adding either one will likely introduce internal seams for TLS termination/passthrough policy and credential handling, but it should not enlarge the runtime manager’s interface.

## Storage and live delivery

Configuration is an atomically replaced, owner-readable JSON document. Interactions are a compacted JSONL journal plus an in-memory newest-first ring. Startup replays the journal, retains the newest 5,000 interactions, and compacts an oversized journal. Runtime compaction keeps disk retention close to the same bound. SSE subscribers receive completed interactions and heartbeats; a slow browser drops live notifications and recovers authoritative state through the list endpoint rather than applying backpressure to proxied traffic.

## Trust and safety

Authorization, proxy-authorization, cookie, and set-cookie HTTP headers are redacted before persistence. Configured sensitive header rules extend that set. Redis `AUTH` and `HELLO ... AUTH` payloads are redacted. Persisted secrets are write-only at the API boundary: a public configuration carries only a `valueSet`/`passwordSet` marker, and edit requests merge unchanged secrets inside the store boundary.

TLS construction is another shared deep module used by both protocol adapters. It owns the TLS 1.2 floor, system/custom trust roots, SNI overrides, client certificates, listener certificates, client-CA verification, and ALPN. HTTP-specific protocol selection, WebSocket upgrade/frame handling, and Redis-specific AUTH/SELECT handshakes remain inside their adapters.

Classic WebSockets are an internal seam within the HTTP adapter rather than a new runtime adapter. The upgrade retains HTTP header policy and TLS behavior, then the adapter hijacks the two HTTP/1.1 connections and streams RFC 6455 frames byte-for-byte. Inspection unmasks only the bounded capture copy; bytes forwarded to the upstream are never rewritten. Handshake and frame observations use the same shared envelope with a `websocket` protocol vocabulary and stable connection identity.

Capture limits are independent of forwarding limits, so a large body remains functional while its observation is marked truncated. HTTP mutation validation rejects framing and hop-by-hop headers as well as CR/LF injection. Redis protocol frame limits defend memory use. New adapters must define credential-redaction policy, framing limits, and startup-handshake semantics before registration.
