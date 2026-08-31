# Security model

Portscope handles development credentials and may capture sensitive application
traffic. It is designed to reduce accidental disclosure on a trusted workstation;
it is not an authenticated production gateway.

## Management interface

The management interface and generated proxy listeners bind to loopback by
default. The dashboard uses same-origin controls and defensive browser headers,
but it has no user authentication. Keep it on loopback or put it behind an
authenticated ingress you control. Anyone who can reach the management interface
can inspect captured traffic and change upstream configuration.

## Authentication ownership

Stateful adapters such as Redis, MySQL, PostgreSQL, MongoDB, and RabbitMQ
terminate application-facing authentication. Portscope verifies the listener
identity locally and creates an independently authenticated upstream connection.
Listener credentials are not forwarded to the dependency.

This separation allows a project to use a development-facing identity while the
actual upstream credential remains local. Exact supported authentication methods
are listed in [Protocols and upstreams](protocols.md) and the adapter-specific
[compatibility contracts](README.md#compatibility-contracts).

## Secrets and redaction

Never commit literal credentials. Password fields and sensitive HTTP/gRPC header
values accept strict `${ENV_VAR}` references. A missing variable, malformed
reference, or invalid variable name fails startup instead of becoming an empty
value.

Expansion happens only in an adapter's runtime copy. The stored configuration and
management responses never contain the resolved value; the API returns only a
“value is set” marker so an editor round trip preserves the reference without
revealing it to the browser. Sensitive injected headers and known credential
fields are redacted from captures.

Configuration writes are atomic and use owner-only file permissions.

## TLS

Listener and upstream TLS are configured independently. Upstream verification
uses system roots unless a custom CA is supplied, requires TLS 1.2 or newer, and
supports mutual TLS, private CAs, and protocol-appropriate SNI overrides.
Certificate-verification bypass exists for diagnosis but is deliberately marked
dangerous in the editor.

Certificate, key, and CA paths are resolved relative to `portscope.json`; see
[Project configuration](configuration.md#repository-relative-files).

## Capture safety

Captures are bounded so inspection cannot force an entire oversized body or
message into memory. Forwarding continues after the inspection cap. Redis framing
also has nesting and size limits, and HTTP header mutation rejects hop-by-hop or
framing headers and values containing line breaks.

The interaction journal is periodically compacted to its retention window rather
than growing indefinitely. The health endpoint reports degraded status if capture
persistence fails.

Captured bodies can still contain private application data. The `.portscope/`
directory is gitignored by `portscope init`; do not copy or publish it without
reviewing and sanitizing its contents.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Follow the private
reporting instructions in the project [security policy](../SECURITY.md).

[Back to the documentation index](README.md).
