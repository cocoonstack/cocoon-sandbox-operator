# e2b-compatible API

The aggregated apiserver can serve an [e2b](https://e2b.dev)-compatible REST
surface, so an **unmodified e2b SDK** (JS or Python) claims from the same warm
microVM pools this operator already manages. Point `E2B_API_URL` at it and
`Sandbox.create()` works.

It is a translation layer, not a second control plane. Every request lands on
the same `SandboxStore` the Kubernetes API uses, so an e2b create *is* the
node-local claim a `kubectl create sandbox` performs — and the sandbox it
returns shows up in `kubectl get sandboxes`. Nothing extra is stored: sandbox
identity is the sandboxd claim id the owning node already assigns.

## Enable it

```bash
sandbox-apiserver \
  --enable-e2b-api \
  --e2b-bind-address=:8080 \
  --e2b-namespace=sandboxes \
  --e2b-api-key-file=/etc/e2b/keys \
  --e2b-domain=sandbox.example.com
```

| Flag | Default | Meaning |
|---|---|---|
| `--enable-e2b-api` | `false` | Serve the surface at all. |
| `--e2b-bind-address` | `:8080` | Its own listener; the aggregated API is untouched. |
| `--e2b-namespace` | `default` | Namespace claims land in — e2b has no namespace concept. |
| `--e2b-api-key-file` | — | File of accepted `X-API-KEY` values, one per line (`#` comments ignored). |
| `--e2b-domain` | — | Base domain reported to the SDK for reaching in-sandbox `envd`. |
| `--e2b-allow-anonymous` | `false` | Serve with **no** API key. Development only. |

Startup **fails** if neither `--e2b-api-key-file` nor `--e2b-allow-anonymous` is
set, so a misconfiguration cannot silently expose an open claim endpoint.

## Use it

```bash
export E2B_API_URL=https://your-apiserver:8080
export E2B_API_KEY=e2b_yourkey
```

```js
import { Sandbox } from '@e2b/code-interpreter'

// templateID is the pool template — the container image the warm pool is built
// from, the same axis SandboxWarmPool keys on.
const sandbox = await Sandbox.create('registry.example.com/rt:24.04')
```

## Endpoint mapping

| e2b endpoint | Maps to | Notes |
|---|---|---|
| `POST /sandboxes` | `store.Claim` | `templateID` → pool template; `allow_internet_access` → `egress` lane, else the hardened `none` lane. `201` on success, `503` when the pool is drained (retryable). |
| `GET /sandboxes`, `GET /v2/sandboxes` | `store.List` | Live sandboxes in the compat namespace. |
| `GET /sandboxes/{id}` | `store.List` + id match | `404` when no live sandbox carries the id. |
| `DELETE /sandboxes/{id}` | `store.Release` | Releases the node-local claim id, never by Kubernetes name. `204`. |
| `POST /sandboxes/{id}/timeout` | existence check | TTL is fixed by the node at claim time; the call is verified and acknowledged, not silently faked. |
| `POST /sandboxes/{id}/refreshes` | existence check | Keepalive; liveness is node-owned. |
| `GET /health` | — | Unauthenticated, for probes. |

## Limits worth knowing

- **Reaching `envd` (the in-sandbox data plane).** The SDK derives the sandbox
  host as `{port}-{sandboxID}.{domain}`. A cocoon `sandboxID` is the sandboxd
  claim id, which is not a valid DNS label, so that subdomain form only works
  behind a proxy routing on the `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers
  the SDK also sends. Otherwise set `E2B_SANDBOX_URL` explicitly.
- **`envdVersion`** is reported as `0.4.0` by default. The SDK version-compares
  it and *kills the sandbox* if it cannot parse it, so it is always sent.
- **Not implemented:** pause/resume, fork, snapshots, templates, metrics, and
  team/node admin endpoints. `pause`/`fork` map onto cocoon capabilities and are
  the natural next step; the rest are e2b-hosted concerns.
- **Size class** is pinned (`small`) — e2b's `NewSandbox` carries no size
  selector.
- `metadata` and `envVars` are accepted so SDK calls do not fail, but the
  node-local claim path takes neither today.
