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

This passes the multi-adapter test today: HTTP, Redis, MySQL, PostgreSQL, and MongoDB differ substantially behind the same small interface. Elasticsearch/OpenSearch also demonstrates a second extension shape: a semantic profile can deepen an existing transport adapter without duplicating its listener, TLS, mutation, streaming, or WebSocket implementation.

## Why database protocols remain substantial

The PostgreSQL adapter models startup and optional TLS negotiation, authentication, simple queries, the extended Parse/Bind/Describe/Execute/Sync flow, prepared-statement and portal identity, COPY modes, asynchronous messages, cancellation, and connection/session state. Timing a single frontend message is not necessarily timing a query. Portscope therefore terminates listener SCRAM authentication, opens an independently authenticated upstream session, rewrites cancellation keys, and correlates frontend and backend messages before it emits an observation.

The MySQL adapter therefore remains a deep module behind the existing seam. It generates and authenticates its own downstream server handshake, independently negotiates and authenticates an upstream client session, aligns the two capability sets, owns classic-packet sequencing, and emits observations only after its response state machine has completed a command. Text results, prepared parameter bindings, binary rows, long-data accounting, and statement metadata share connection-scoped state without leaking that state into the runtime manager.

Database adapters produce the same `Interaction` envelope only after their own state machines have identified a meaningful operation. MySQL and PostgreSQL confirm that adding one does not require enlarging the runtime manager’s interface.

The MongoDB adapter terminates only the state that must differ across the two trust legs: `hello`, topology advertisement, and SCRAM. It reads an unauthenticated upstream `hello` to preserve real server limits, but does not disclose an upstream username or start upstream SCRAM until the listener identity succeeds. After both legs authenticate, commands are forwarded as their original wire bytes instead of being reissued through a database client. That keeps logical sessions, transaction numbers, cursor IDs, document sequences, write concerns, and change-stream semantics owned by MongoDB while a bounded BSON parser observes request IDs and results alongside the stream.

The RabbitMQ adapter follows the same trust-boundary principle without pretending AMQP is request/response TCP. It terminates the AMQP 0-9-1 connection handshake, validates application PLAIN credentials and the listener virtual host locally, and only then sends a separately configured PLAIN identity and virtual host to the broker. Once open, complete frames are forwarded unchanged. A channel-aware observer correlates synchronous `*-ok` methods, Basic.get content, publisher confirms, deliveries, and asynchronous events while assembling method/header/body sequences into bounded message observations. Heartbeats continue in both directions without becoming traffic noise.

## Storage and live delivery

Configuration is an atomically replaced, owner-readable JSON document. Interactions are a compacted JSONL journal plus an in-memory newest-first ring. Startup replays the journal, retains the newest 5,000 interactions, and compacts an oversized journal. Runtime compaction keeps disk retention close to the same bound. SSE subscribers receive completed interactions and heartbeats; a slow browser drops live notifications and recovers authoritative state through the list endpoint rather than applying backpressure to proxied traffic.

## Trust and safety

Authorization, proxy-authorization, cookie, and set-cookie HTTP headers are redacted before persistence. Configured sensitive header rules extend that set. Redis listener authentication is terminated before an upstream connection is authenticated; `AUTH` and `HELLO ... AUTH` payloads are redacted, and the latter is rewritten without its listener credentials. MySQL and PostgreSQL authentication packets are never captured as payloads. MongoDB SCRAM and RabbitMQ PLAIN payloads are represented only as redaction markers. Redis, MySQL, PostgreSQL, MongoDB, and RabbitMQ have separate listener/upstream write-only password settings. Persisted secrets are write-only at the API seam: a public configuration carries only a `valueSet`/`passwordSet` marker, and edit requests merge unchanged secrets inside the store module.

TLS construction is another shared deep module used by every transport adapter. It owns the TLS 1.2 floor, system/custom trust roots, SNI overrides, client certificates, listener certificates, client-CA verification, and ALPN. HTTP-specific protocol selection, WebSocket upgrade/frame handling, Redis authentication/database state, MySQL handshakes, PostgreSQL SSLRequest/direct-TLS negotiation, MongoDB direct TLS, and RabbitMQ direct AMQPS remain inside their adapters.

Semantic profiles are internal seams inside a transport adapter, not new runtime interfaces. The Elasticsearch/OpenSearch profile receives the already bounded, redacted HTTP observation and enriches its protocol vocabulary, structured payload, outcome, and searchable attributes. HTTP forwarding has no dependency on search semantics, and the runtime manager still sees only `Adapter.Run`.

Classic WebSockets are an internal seam within the HTTP adapter rather than a new runtime adapter. The upgrade retains HTTP header policy and TLS behavior, then the adapter hijacks the two HTTP/1.1 connections and streams RFC 6455 frames byte-for-byte. Inspection unmasks only the bounded capture copy; bytes forwarded to the upstream are never rewritten. Handshake and frame observations use the same shared envelope with a `websocket` protocol vocabulary and stable connection identity.

Capture limits are independent of forwarding limits, so a large body remains functional while its observation is marked truncated. HTTP mutation validation rejects framing and hop-by-hop headers as well as CR/LF injection. Redis protocol frame limits defend memory use. New adapters must define credential-redaction policy, framing limits, and startup-handshake semantics before registration.
