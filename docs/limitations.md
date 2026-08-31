# Known limitations

Portscope favors correct forwarding and explicit boundaries over claiming
support for protocol features it cannot inspect safely. These are the current
known limitations.

## HTTP and WebSocket

- HTTP `CONNECT` tunnels and RFC 8441 WebSockets over HTTP/2 extended CONNECT are
  not implemented.
- Classic RFC 6455 upgrades are supported over HTTP/1.1, including TLS on either
  side.
- Negotiated WebSocket extensions pass through unchanged. Extension-transformed
  payloads such as per-message-deflate are captured as bytes, not decompressed.

## gRPC

- Server reflection calls pass through and remain visible, but Portscope does
  not issue hidden reflection RPCs. Supply a descriptor set for JSON rendering.
- `identity` and `gzip` message encodings are decoded. Other encodings are
  forwarded correctly and shown as bounded opaque messages.
- gRPC-Web and HTTP/1.x gRPC are rejected rather than mislabeled as native gRPC.

## Redis

- Cluster redirection topology and transaction-level grouping are not modeled.
- RESP3 push frames are captured, but subscription relationships are not modeled
  beyond the connection.
- `RESET` is rejected because Portscope owns authentication and database state.

## MySQL

- Compression, query attributes, optional result-set metadata, and `LOCAL INFILE`
  are not negotiated.
- `COM_CHANGE_USER` and replication commands are rejected without touching the
  upstream session.
- Common prepared-parameter and binary-row types are decoded. Unknown or newer
  binary types remain observable as undecoded payloads rather than risking an
  incorrect value.

## PostgreSQL

- Replication connections, GSS/SSPI, OAuth, certificate-only authentication,
  channel-binding SCRAM-PLUS, and function-call inspection are not implemented.
- Protocol 3 clients are downgraded to minor version 0 when needed.
- COPY streams are forwarded without buffering their full contents. Byte counts
  and final command status are attached to the initiating query.

## Elasticsearch and OpenSearch

- Semantic inspection covers the common REST vocabulary and JSON/NDJSON formats.
- Product-specific plugins, CBOR, and SMILE bodies pass through correctly but
  remain generic body captures.

## MongoDB

- Portscope is intentionally a direct-target proxy. It removes replica-set
  discovery addresses, mongos identity, load-balanced service IDs, and
  compression from the application-facing `hello` so traffic cannot bypass
  inspection and BSON remains visible.
- A target can be a `mongod` or `mongos`, but Portscope does not provide
  multi-host failover.
- SCRAM-SHA-256 and SCRAM-SHA-1 are supported. X.509, AWS IAM, OIDC, Kerberos,
  and LDAP authentication are not terminated.
- Post-auth wire messages are otherwise forwarded unchanged.

## RabbitMQ

- Support is AMQP 0-9-1 over TCP/TLS, not AMQP 1.0, streams, MQTT, or STOMP.
- Connection setup is terminated only for PLAIN authentication and virtual-host
  mapping. Post-open frames remain broker-owned and are forwarded unchanged.
- SASL challenge mechanisms and OAuth are not terminated.
- Captures assemble Basic content and common methods, but do not reinterpret
  broker extensions or message encodings beyond JSON detection.

## General

- Body capture is capped and binary bodies are described rather than rendered.
  Forwarding continues after the capture cap.
- Portscope is a local development tool. The dashboard has same-origin controls
  and defensive browser headers, but no user authentication. Keep the management
  listener on loopback or behind an authenticated ingress you control.

See [Protocols and upstreams](protocols.md) for supported behavior and the
adapter-specific [compatibility contracts](README.md#compatibility-contracts) for
tested version ranges.

[Back to the documentation index](README.md).
