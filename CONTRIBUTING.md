# Contributing to Portscope

Portscope keeps a deliberately small runtime seam: every protocol adapter implements `proxy.Adapter.Run`, while framing, session state, authentication, redaction, and correlation remain inside that adapter. Read `docs/architecture.md` and the relevant compatibility contract before changing protocol behavior.

## Development

Requirements are Go 1.26.6 or newer and Node.js 24 or newer.

```bash
npm ci
make test
make lint
```

Use `make dev` for the API and Vite development server, or `make build` for the same single embedded binary distributed in releases. If the frontend changes, commit the regenerated `internal/webui/dist` output; CI verifies that it matches the source.

Protocol changes should include bounded parsing tests, credential-redaction coverage, and a real-server compatibility test when a maintained container image is available. The compatibility matrix targets live under `scripts/` and `.github/workflows/`.

## Pull requests

- Keep changes focused and explain the wire-level compatibility impact.
- Add or update the relevant `docs/*-compatibility.md` contract.
- Do not include captures, credentials, local `.portscope/` state, IDE settings, or generated release archives.
- Run `make test` and `make lint` before requesting review.

## Releases

Push a semantic-version tag such as `v0.1.0` from a clean `main` commit. The release workflow rebuilds the embedded frontend, cross-compiles static macOS, Linux, and Windows binaries for amd64 and arm64, publishes platform archives, and attaches a SHA-256 checksum manifest to the GitHub Release. Pre-release tags remain marked as pre-releases.
