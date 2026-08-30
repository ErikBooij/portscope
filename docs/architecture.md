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

This passes the multi-adapter test today: HTTP, Redis, MySQL, and PostgreSQL differ substantially behind the same small interface.

## Why database protocols remain substantial

The PostgreSQL adapter models startup and optional TLS negotiation, authentication, simple queries, the extended Parse/Bind/Describe/Execute/Sync flow, prepared-statement and portal identity, COPY modes, asynchronous messages, cancellation, and connection/session state. Timing a single frontend message is not necessarily timing a query. Portscope therefore terminates listener SCRAM authentication, opens an independently authenticated upstream session, rewrites cancellation keys, and correlates frontend and backend messages before it emits an observation.

The MySQL adapter therefore remains a deep module behind the existing seam. It generates and authenticates its own downstream server handshake, independently negotiates and authenticates an upstream client session, aligns the two capability sets, owns classic-packet sequencing, and emits observations only after its response state machine has completed a command. Text results, prepared parameter bindings, binary rows, long-data accounting, and statement metadata share connection-scoped state without leaking that state into the runtime manager.

Database adapters produce the same `Interaction` envelope only after their own state machines have identified a meaningful operation. MySQL and PostgreSQL confirm that adding one does not require enlarging the runtime manager’s interface.

## Storage and live delivery

Configuration is an atomically replaced, owner-readable JSON document. Interactions are a compacted JSONL journal plus an in-memory newest-first ring. Startup replays the journal, retains the newest 5,000 interactions, and compacts an oversized journal. Runtime compaction keeps disk retention close to the same bound. SSE subscribers receive completed interactions and heartbeats; a slow browser drops live notifications and recovers authoritative state through the list endpoint rather than applying backpressure to proxied traffic.

## Trust and safety

Authorization, proxy-authorization, cookie, and set-cookie HTTP headers are redacted before persistence. Configured sensitive header rules extend that set. Redis listener authentication is terminated before an upstream connection is authenticated; `AUTH` and `HELLO ... AUTH` payloads are redacted, and the latter is rewritten without its listener credentials. MySQL and PostgreSQL authentication packets are never captured as payloads. Redis, MySQL, and PostgreSQL have separate listener/upstream write-only password settings. Persisted secrets are write-only at the API seam: a public configuration carries only a `valueSet`/`passwordSet` marker, and edit requests merge unchanged secrets inside the store module.

TLS construction is another shared deep module used by all four protocol adapters. It owns the TLS 1.2 floor, system/custom trust roots, SNI overrides, client certificates, listener certificates, client-CA verification, and ALPN. HTTP-specific protocol selection, WebSocket upgrade/frame handling, Redis authentication/database state, MySQL handshakes, and PostgreSQL SSLRequest/direct-TLS negotiation remain inside their adapters.

Classic WebSockets are an internal seam within the HTTP adapter rather than a new runtime adapter. The upgrade retains HTTP header policy and TLS behavior, then the adapter hijacks the two HTTP/1.1 connections and streams RFC 6455 frames byte-for-byte. Inspection unmasks only the bounded capture copy; bytes forwarded to the upstream are never rewritten. Handshake and frame observations use the same shared envelope with a `websocket` protocol vocabulary and stable connection identity.

Capture limits are independent of forwarding limits, so a large body remains functional while its observation is marked truncated. HTTP mutation validation rejects framing and hop-by-hop headers as well as CR/LF injection. Redis protocol frame limits defend memory use. New adapters must define credential-redaction policy, framing limits, and startup-handshake semantics before registration.
