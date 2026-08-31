# MySQL compatibility

Portscope supports the MySQL classic protocol from MySQL 5.6 through the current release. “Supports” here means an application can authenticate to Portscope, Portscope independently authenticates to the configured server, and normal application traffic is forwarded and inspected without changing its result.

The adapter is capability-driven rather than branching on version strings. It preserves the upstream version in the application-facing greeting because connectors use that value for their own compatibility decisions. Authentication capabilities are negotiated independently on the upstream leg; response-shaping capabilities such as deprecated EOF handling remain mirrored between both legs because result packets are forwarded unchanged.

## Automated server matrix

`make test-mysql-matrix` starts disposable official MySQL images and runs the same end-to-end contract against every release family:

| Family | Exact version verified on 2026-08-30 | Default upstream auth | Result terminator |
| --- | --- | --- | --- |
| 5.6 | 5.6.51 | `mysql_native_password` | EOF |
| 5.7 | 5.7.44 | `mysql_native_password` | negotiated EOF/OK |
| 8.0 | 8.0.46 | `caching_sha2_password` | negotiated OK |
| 8.4 LTS | 8.4.11 | `caching_sha2_password` | negotiated OK |
| 9.7 LTS | 9.7.2 | `caching_sha2_password` | negotiated OK |
| 26.7 | 26.7.0 | `caching_sha2_password` | negotiated OK |

Each matrix case verifies connection setup, the reported server version, text queries, prepared inserts and selects, signed integers, strings, doubles, datetime values, a large binary value, transaction rollback, multi-statement/multi-result handling, and emitted inspection events. Focused protocol tests additionally cover MySQL's in-protocol TLS upgrade with certificate verification, `sha256_password` over TLS and through an RSA public-key exchange, authentication switches, both EOF modes, bounded capture, common binary types, and credentials being terminated rather than forwarded.

The default matrix follows the moving minor tags (`5.6`, `5.7`, `8.0`, `8.4`, `9.7`, `26.7`) so patch releases are picked up automatically. The same six-family matrix runs in GitHub Actions on every push and pull request. Specific families can be selected locally with, for example, `./scripts/test-mysql-matrix.sh 5.6 26.7`. On Apple silicon the script runs the archived 5.6 and 5.7 images under `linux/amd64`; newer images run natively.

## Authentication contract

- The downstream listener currently presents `mysql_native_password`, which gives old and current connectors a common application-facing mechanism. This does not require the upstream server to provide that plugin.
- The independently authenticated upstream leg supports `mysql_native_password`, `sha256_password`, and `caching_sha2_password`, including auth-switch packets.
- SHA-2 full authentication sends a cleartext password only inside a verified TLS session. Without TLS, Portscope requests the server public key and uses MySQL's RSA OAEP exchange.
- Listener and upstream TLS are independent and both require TLS 1.2 or newer. Very old MySQL 5.6 installations that only enable obsolete TLS versions remain reachable over a trusted plaintext network, but Portscope does not weaken its TLS policy to negotiate TLS 1.0/1.1.

## Deliberate protocol exclusions

Server-version compatibility does not mean every optional classic-protocol extension is enabled. Compression, Zstandard compression, query attributes, optional resultset metadata, connection attributes, and `LOCAL INFILE` are not negotiated. `COM_CHANGE_USER` and replication commands are rejected because they would replace the independently owned upstream session. These exclusions are explicit capability choices and apply consistently across versions; ordinary SQL, prepared statements, transactions, multiple results, and TLS/authentication are supported.

[Back to the documentation index](README.md).
