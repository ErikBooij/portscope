# Elasticsearch and OpenSearch compatibility

Search upstreams reuse Portscope's HTTP/1.1, HTTP/2, TLS, mutual-TLS, header-policy, streaming, redaction, and capture implementation. A semantic profile enriches the completed HTTP observation; it does not modify REST requests or responses.

## Tested product lines

The real-server matrix exercises maintained major product lines:

- Elasticsearch 8.19
- Elasticsearch 9.5
- OpenSearch 2.19
- OpenSearch 3.8

For each server, the matrix creates an index, submits a bulk request, refreshes and searches it, executes a multi-search request, validates semantic observations, and deletes the index. The normal test suite separately covers partial bulk and multi-search failures, NDJSON decoding, API-key injection/redaction, wildcard index paths, document operations, cluster health, and CAT endpoints.

## Semantic behavior

- `_search` and `_count` expose the target index, server `took`, timeout state, total hits/count, hit-count relation, and shard totals/failures.
- `_bulk` request lines are grouped into action/metadata/document entries. Response items are counted and partial item failures make the Portscope interaction an error even when HTTP status is 200.
- `_msearch` metadata/query line pairs are grouped. Partial response errors are counted and reflected in the interaction outcome.
- Document create, index, get, update, and delete calls expose index, document ID, and result.
- Scroll, reindex, update-by-query, delete-by-query, multi-get, cluster-health, and CAT endpoints receive stable operation names.
- Elasticsearch error reasons are extracted into the shared interaction error while the complete bounded response remains available.

Capture remains bounded at the HTTP adapter's 256 KiB limit. NDJSON decoding records at most 100 logical items and never changes the forwarded stream. Authorization, proxy authorization, API-key, cookie, and configured sensitive headers are redacted before semantic inspection.

## Explicit limits

Product plugins and newly introduced REST endpoints fall back to `ELASTIC <METHOD>` while retaining their path, status, headers, and payloads. JSON and NDJSON receive semantic decoding; YAML remains text, while CBOR and SMILE remain binary captures. AWS SigV4 signing is not performed by Portscope, though already signed requests and injected authorization headers are forwarded unchanged.
