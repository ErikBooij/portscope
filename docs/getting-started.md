# Getting started

Portscope runs between a local application and the services it uses. Each
configured upstream has a **listen address** for the application and a **target**
for the real service. The browser at `127.0.0.1:8090` shows the traffic that
passes between them.

## Install

Download the archive for your platform from
[GitHub Releases](https://github.com/ErikBooij/portscope/releases), verify it
against the published `checksums.txt`, and put `portscope` on your `PATH`.

With Go 1.26.6 or newer, install the latest source release instead:

```bash
go install github.com/erikbooij/portscope@latest
```

The web interface is embedded in the binary. Node.js is not required to run
Portscope.

## Initialize a project

Run the initializer from the repository whose dependencies you want to inspect:

```bash
cd your-project
portscope init
git add portscope.json .gitignore
portscope
```

The initializer creates a versioned `portscope.json` with a Local API example and
adds `/.portscope/` to the existing `.gitignore`. It will not overwrite either
file. Portscope reads the configuration from the current directory and stores
recent captures in `.portscope/interactions.jsonl`.

Open <http://127.0.0.1:8090>. Changes made in the web interface are saved
atomically to `portscope.json`; only the affected listener restarts, and captured
history remains available.

## Capture HTTP traffic

The generated configuration listens on `127.0.0.1:9000` and forwards to
`http://127.0.0.1:3000`. Start the service on port 3000, then configure the
application or client to use Portscope instead:

```bash
curl http://127.0.0.1:9000/health
```

The request, response, status, headers, body preview, and timing appear in the
dashboard. Edit the Local API upstream if your service uses another address.
HTTPS, HTTP/2, h2c, request/response header policies, listener TLS, and upstream
TLS are available in the upstream editor.

## Capture Redis traffic

Choose **Add upstream**, select **Redis**, and configure:

```text
Listen address    127.0.0.1:6380
Upstream target   127.0.0.1:6379
```

Set application-facing credentials and upstream credentials independently when
authentication is enabled. Then point the application at
`redis://127.0.0.1:6380`. Portscope validates the application `AUTH` locally and
opens its own authenticated upstream session; listener credentials never reach
Redis. Enable listener TLS for `rediss://` connections and upstream TLS
independently.

## Use another configuration location

Defaults can be overridden without editing repository configuration:

```bash
portscope --config ./dev/portscope.json \
  --state-dir ./.cache/portscope \
  --addr 127.0.0.1:8091
```

When only `--config` changes, the default state directory becomes `.portscope/`
beside that configuration. Certificate, key, CA, and Protobuf descriptor paths
are also resolved relative to the configuration file.

See [Project configuration](configuration.md) for the full file convention and
[Protocols and upstreams](protocols.md) for every adapter.

## Run with Docker

```bash
docker compose up --build
```

The included Compose file mounts `portscope.json` and `.portscope/`. Add an
explicit port mapping for every configured proxy listener. Inside the container,
a target on `127.0.0.1` refers to the container itself, not the host.

## Keep it local

Portscope is a development tool. The management listener defaults to loopback
and does not provide user authentication. Do not expose it to an untrusted
network; see the [security model](security.md) for details.

[Back to the documentation index](README.md).
