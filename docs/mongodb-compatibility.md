# MongoDB compatibility

Portscope implements a terminated MongoDB wire proxy for direct connections. The compatibility matrix exercises the official MongoDB Go driver against distinct listener credentials while Portscope independently authenticates to official MongoDB 6.0.28, 7.0.40, 8.0.29, and 8.3.8 images.

## Negotiation and authentication

- Modern `OP_MSG` messages and legacy `OP_QUERY` handshakes are bounded at 64 MiB and parsed as little-endian MongoDB wire frames.
- Portscope obtains the upstream server's real wire-version and size limits, then removes replica-set hosts, primary addresses, mongos identity, load-balanced service IDs, topology versions, speculative authentication, and compression from the listener `hello`.
- Listener authentication supports SCRAM-SHA-256 and SCRAM-SHA-1. Its salt, nonce, iteration count, proof, and server signature are owned by Portscope; SCRAM payloads are never persisted.
- Upstream SCRAM starts only after listener authentication succeeds. Automatic selection queries `saslSupportedMechs`, prefers SHA-256, and falls back to SHA-1 when that is what the target advertises.
- Listener and upstream TLS are independent direct-TLS legs. The shared TLS policy supports private CAs, SNI override, client certificates, mutual TLS, and TLS 1.2 or newer.

## Forwarding and inspection

After authentication, Portscope forwards original application messages rather than reconstructing commands. This preserves logical-session IDs, transaction numbers, retryable writes, cursor IDs, change-stream/getMore state, write concerns, checksums, and server error documents. `hello` and authentication remain locally terminated so topology and credentials cannot cross trust boundaries.

`OP_MSG` body and document-sequence sections are rendered as bounded Extended JSON. Requests and responses are correlated through `requestID`/`responseTo`; cursor batch sizes, cursor IDs, affected counts, modified counts, write errors, command failures, database, and collection become searchable observation attributes.

## Deliberate limits

- Portscope exposes one configured target as a direct server and does not implement replica-set discovery or multi-host failover. The target may be a `mongod` or `mongos`, but applications never receive addresses that could bypass the proxy.
- Compression is deliberately not negotiated. This keeps every BSON command inspectable while remaining compatible with drivers, which must support uncompressed messages.
- X.509, AWS IAM, OIDC, Kerberos/GSSAPI, and LDAP/PLAIN authentication are not yet terminated.
- Legacy CRUD opcodes removed by MongoDB 5.1 are forwarded but not semantically decoded. Current `OP_MSG` command traffic and the legacy handshake exception are decoded.

Run the disposable matrix with `make test-mongo-matrix`. Each server and image is removed after its case succeeds. The runner retries the known transient MongoDB container bootstrap failure where the initialization process has not released its listener before the final server starts; unrelated startup failures still fail immediately with their container logs.

[Back to the documentation index](README.md).
