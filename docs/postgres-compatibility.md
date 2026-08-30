# PostgreSQL compatibility

Portscope implements a stateful PostgreSQL protocol v3 endpoint. It authenticates the application locally, creates a separate upstream session, and correlates frontend and backend protocol messages into query observations. It is not a transparent TCP tunnel.

## Tested server families

The compatibility matrix runs against the official PostgreSQL Docker images for every community-supported major release:

- PostgreSQL 14
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

The local matrix records and validates the exact server version exposed by each image. CI runs the same behavioral test against every major family. The matrix exercises connection setup, SCRAM listener authentication, startup parameter preservation, simple queries, extended prepared queries, text and binary values, a value larger than the capture limit, batched/pipelined queries, cancellation-key rewriting, and continued use of the session after cancellation.

## Authentication and TLS

The two authentication legs are intentionally independent:

- Applications authenticate to Portscope with a configured username and SCRAM-SHA-256 password. The configured secret never travels to PostgreSQL.
- Portscope authenticates upstream with a separate username and password. Trust, cleartext password, MD5 password, and SCRAM-SHA-256 server challenges are supported.
- Listener TLS supports PostgreSQL's classic `SSLRequest` negotiation and PostgreSQL 17+'s direct TLS negotiation. Plaintext is rejected when listener TLS is required.
- Upstream TLS uses `SSLRequest`, TLS 1.2 or newer, hostname verification, system or private roots, and optional client certificates.
- Portscope replaces `BackendKeyData` with random local cancellation keys. A matching `CancelRequest` is translated back to the actual upstream key over a separate connection; that connection also uses TLS when upstream TLS is enabled.

Passwords are write-only in the management API and browser. Stored values survive an edit without being disclosed to the client.

## Inspected protocol behavior

- Simple Query messages, including multiple command tags in one query cycle.
- Extended Parse, Bind, Execute, Close, and Sync flows, including named and unnamed statements and portals.
- Text and binary bind parameters, row descriptions, text and binary data rows, SQLSTATE error fields, affected-row command tags, empty results, and portal suspension.
- Pipelined extended-query groups without inserting synchronization or changing message order.
- COPY traffic is streamed unchanged. Client COPY bytes contribute to request size; server COPY bytes contribute to response size; the final command tag is retained.
- Asynchronous `NOTIFY` messages are recorded separately without disrupting query correlation.
- Startup parameters needed for ordinary application behavior (`application_name`, `client_encoding`, `DateStyle`, `TimeZone`, and `options`) are preserved. User, database, replication, and unsupported `_pq_.` parameters are owned or negotiated by Portscope.

Capture is bounded: query text stops at 256 KiB, individual parameter and cell previews stop at 4 KiB, and result capture stops at 100 rows. Forwarding does not stop when capture is truncated. A single framed protocol message is limited to 64 MiB and a startup packet to 64 KiB.

## Explicit exclusions

- Physical and logical replication startup modes.
- GSSAPI/SSPI, OAuth, certificate-only authentication, and channel-binding `SCRAM-SHA-256-PLUS`.
- PostgreSQL Function Call message decoding.
- Semantic decoding of PostgreSQL type OIDs. Text values remain text; binary values remain bounded hexadecimal captures so the inspector never invents an incorrect value.

Unsupported authentication methods fail closed before application traffic is accepted. Protocol v3 minor versions newer than Portscope's implemented minor version are explicitly negotiated down to 3.0 rather than silently claimed.
