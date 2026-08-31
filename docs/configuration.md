# Project configuration

Portscope is repository-aware by convention. Running `portscope` reads `portscope.json` from the current working directory and stores the newest 5,000 interactions in `.portscope/interactions.jsonl`. `portscope init` preserves existing `.gitignore` contents while adding `/.portscope/`; commit both the configuration and that ignore rule.

`portscope init` creates a version 1 document with a Local API example. It refuses to overwrite an existing file. The bundled [JSON Schema](../portscope.schema.json) supplies editor completion and catches structural mistakes; Portscope itself uses strict JSON decoding and rejects unknown fields.

```json
{
  "$schema": "https://raw.githubusercontent.com/erikbooij/portscope/main/portscope.schema.json",
  "version": 1,
  "upstreams": [
    {
      "id": "api",
      "name": "Local API",
      "protocol": "http",
      "listenAddr": "127.0.0.1:9000",
      "target": "http://127.0.0.1:3000",
      "enabled": true,
      "http": {}
    }
  ]
}
```

Every upstream needs a stable `id`. IDs may contain letters, numbers, dots, underscores, and hyphens. The web interface generates IDs for new upstreams and writes changes atomically to the same project file. An enabled listen address must be unique within the document.

## Repository-relative files

Relative paths are resolved from the directory containing the selected configuration file, including:

- listener certificate, private-key, and client-CA files;
- upstream CA, client-certificate, and client-key files; and
- gRPC descriptor sets.

This remains true when Portscope is started from another directory with `--config path/to/portscope.json`.

## Secrets from the environment

Literal secrets work locally but should not be committed. Password fields and sensitive HTTP/gRPC header values support strict `${NAME}` interpolation:

```json
{
  "id": "database",
  "name": "Development MySQL",
  "protocol": "mysql",
  "listenAddr": "127.0.0.1:3307",
  "target": "127.0.0.1:3306",
  "enabled": true,
  "mysql": {
    "listenerUsername": "portscope",
    "listenerPassword": "${PORTSCOPE_MYSQL_PASSWORD}",
    "upstreamUsername": "app",
    "upstreamPassword": "${MYSQL_PASSWORD}",
    "database": "app"
  }
}
```

An unset variable, malformed reference, or invalid variable name stops startup with the upstream and field identified. Expansion occurs only in runtime copies. The stored configuration and management responses never contain the resolved value.

## Overrides

```text
--config path       configuration file (default ./portscope.json)
--state-dir path    interaction state (default .portscope beside the config file)
--addr host:port    management interface (default 127.0.0.1:8090)
```

Legacy pre-release configuration files containing a top-level upstream array are still accepted. The next web-interface edit rewrites them as a version 1 document. Unknown document versions are rejected instead of being guessed.

[Back to the documentation index](README.md).
