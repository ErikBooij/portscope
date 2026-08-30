# gRPC compatibility contract

Portscope proxies native gRPC over HTTP/2. Use `h2c://host:port` for a cleartext upstream or `https://host:port` for TLS. The application-facing listener accepts h2c by default; enabling listener TLS serves HTTP/2 using ALPN. Upstream and listener trust roots, server names, client certificates, and mutual TLS are independently configurable.

## What is terminated and what is preserved

Portscope terminates the HTTP/2 transport on both legs and establishes its own upstream HTTP/2 connection. Request and response metadata policies run at that boundary. Streaming DATA is forwarded as it is read; backpressure, cancellation, deadlines, and trailers remain part of the live call rather than being converted into a buffered unary request.

The gRPC five-byte message envelope is parsed from a bounded inspection copy. Envelope headers and messages may span any number of HTTP/2 DATA frames. Declared message sizes never cause equivalent allocations: only the first 256 KiB is captured and forwarding continues unchanged. At most 1,000 live message observations and 100 aggregate message snapshots are retained per call.

## Inspection

Every call records its service/method path, duration, HTTP/2 transport details, request/response message counts, deadline metadata, response trailers, canonical `grpc-status`, decoded `grpc-message`, and summarized `grpc-status-details-bin` types. Unary, client-streaming, server-streaming, and bidirectional-streaming methods are classified when a descriptor is available. Each complete streamed message is also emitted live in its direction while the call remains open.

Portscope understands uncompressed and per-message gzip payloads. Other registered or custom message encodings are forwarded byte-for-byte and shown as opaque bounded payloads.

Without a schema, protobuf messages are represented by their wire size. Configure a binary `FileDescriptorSet` to decode request and response messages to protobuf JSON and identify streaming cardinality:

```bash
protoc --include_imports --descriptor_set_out=service.protoset service.proto
```

The descriptor set is loaded and linked before the proxy listener becomes ready and is capped at 64 MiB. Reflection RPCs are ordinary gRPC calls and pass through, but Portscope does not automatically call reflection: doing so would create hidden traffic and could require application-specific metadata.

## Deliberate boundaries

- Only native `application/grpc` traffic over HTTP/2 is accepted. gRPC-Web and HTTP/1.x requests are rejected.
- `identity` and `gzip` message encodings are decoded. Snappy, zstd, and custom codecs are not decoded.
- A descriptor set is static for the lifetime of a listener. Updating its path or reapplying the upstream restarts that listener and reloads the schema.
- Unknown fields remain subject to normal protobuf JSON behavior. Corrupt, truncated, or schema-mismatched payloads are reported as decode errors without changing forwarded bytes.
- Header policies can inject or remove metadata, but HTTP/2 framing and hop-by-hop headers remain transport-owned and cannot be mutated.
