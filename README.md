# Edge rate limit

Global, per-user rate limits at the Akamai edge. An EdgeWorker builds a key from anything in the
request, including JWT claims and POST body fields. Regional counters share their counts over
[NATS](https://nats.io), so one user has one count worldwide.

![Architecture: a client request reaches an Akamai EdgeWorker, which asks the nearest regional rl-adapter to count the hit and answers 429 or forwards to origin. Adapters in each region exchange deltas every 100 ms over NATS JetStream.](docs/architecture.svg)

## Why

A rate limit counted separately on each edge server can't hold a global limit. A user sending 80
requests through each of three locations stays under a limit of 100 everywhere, while actually
sending 240. Keying on a JWT claim or a body field also usually means custom edge code. This project
is that code, plus the counting layer behind it.

| | |
|---|---|
| **Keys** | IP, headers, query parameters, JWT claims (optionally verified), JSON or form body fields, combined per policy |
| **Scope** | One count per key across all regions |
| **Windows** | Sliding, up to `MAX_WINDOW_SEC`, with any limit per window, such as 5 per 10 minutes |
| **Modes** | `enforce` or `shadow`, per policy |
| **Block lifts** | As soon as the rolling count drops under the limit. Blocked attempts still count. |
| **Failure** | Fails open. A slow or unavailable counter never fails a request. |

## How NATS is used

The counters run on **NATS JetStream, the storage layer underneath NATS KV**. They use JetStream
streams directly instead of KV buckets. An atomic KV increment on every request is too slow at this
scale and puts a quorum round trip in the request path, so each region counts in memory and
publishes batched deltas every 100 ms ([why](docs/decisions.md)). Each `rl-adapter` embeds its own
NATS server, so there's no separate cluster to run.

## Try it: three regions on your machine

Needs Docker and Node 20 or later. This runs the real EdgeWorker handlers under Node against three
clustered adapters, rotating one user's requests through the regions.

```bash
cd examples/three-regions
docker compose up -d --build
sleep 10
node run.js
```

```
limit 30 per 60 s, 60 requests for one user, rotating through 3 regions

region  sent  blocked  count seen by region
us      20    10       60
eu      20    10       60
ap      20    10       60

First 429 at request 31. Each region received only 20 requests, so counting per region would never have reached the limit of 30.
```

Clean up with `docker compose down`.

## Writing policies

Policies are config in the EdgeWorker bundle. See [`edgeworker/src/policies.js`](edgeworker/src/policies.js).

```js
{
  id: 'search-repeat',
  match: { hosts: ['api.example.com'], paths: ['/v1/search'], methods: ['POST'] },
  key: ['jwt:sub', 'body:query'],   // one user repeating one search
  limit: 300,
  window: 600,                      // seconds
  mode: 'shadow',                   // or 'enforce'
}
```

| Key part | Value |
|---|---|
| `ip` | Client IP |
| `header:<name>` | First value of a request header |
| `query:<name>` | Query-string parameter |
| `jwt:<claim>` | Claim from the bearer JWT (dot path) |
| `body:<field>` | JSON (dot path) or form field; `responseProvider` bundle only |
| `const:<value>` | A literal, for one shared bucket |

If any part of a key is missing, that policy is skipped for the request. Pair claim-keyed policies
with an `ip` policy, so that dropping the token isn't a way out.

**Verify your tokens.** With `jwt.verify: false`, a claim is as spoofable as any header, and a caller
can rotate `sub` to evade a limit. Set `verify: true` and supply JWKs, or combine the claim with `ip`.

## EdgeWorker bundles

```bash
cd edgeworker
npm test
./build.sh request 1.0.0    # dist/rl-request-1.0.0.tgz
./build.sh provider 1.0.0   # dist/rl-provider-1.0.0.tgz
```

| Bundle | Event | Key parts | Origin path |
|---|---|---|---|
| `request` | `onClientRequest` | Everything except `body:` | Unchanged |
| `provider` | `responseProvider` | All, including `body:` | The worker forwards to origin itself |

Start with `request` unless you need body keys. The property needs a few variables and rules: see
[property setup](docs/property-setup.md).

## The adapter

```bash
# Build and test (Go 1.25)
go test ./...
CGO_ENABLED=0 go build -o bin/ ./cmd/...

# One node
JETSTREAM_DIR=/tmp/rl NODE_ID=local RL_TOKEN=t MAX_WINDOW_SEC=600 ./bin/rl-adapter &
curl -s -H 'X-RL-Token: t' \
  -d '{"hits":[{"policy":"login","key":"abc","limit":2,"window":600}]}' \
  localhost:8080/v1/rl/hit

# Load test
./bin/rl-load -url http://127.0.0.1:8080 -token t -c 64 -d 15s
```

| Endpoint | |
|---|---|
| `POST /v1/rl/hit` | Count hits and return each count and decision. Needs `X-RL-Token`. |
| `GET /v1/rl/peek?policy=&key=` | This node's count for a key, without counting a hit. **Unauthenticated**: keys are hashes, but restrict it at your network edge if you don't need it. |
| `GET /v1/health` | Node and replication status |
| `GET /metrics` | Prometheus metrics. Also unauthenticated. |

| Environment variable | Default | |
|---|---|---|
| `NODE_ID` | hostname | Node and NATS server name |
| `REGION` | | Placement tag; names the stream `RL_D_<REGION>` |
| `HTTP_ADDR` | `:8080` | |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | | Serve HTTPS |
| `RL_TOKEN` | | Shared secret from the EdgeWorker. Empty disables auth. |
| `MAX_WINDOW_SEC` | `3600` | Longest policy window, and the stream's `MaxAge` |
| `FLUSH_MS` | `100` | Delta publish interval |
| `STREAM_REPLICAS` | `1` | Replicas of the region's stream, within the region |
| `JETSTREAM_DIR` | `/var/lib/rl/js` | |
| `NATS_CLIENT_PORT` | `4222` | |
| `NATS_CLUSTER_PORT` | `0` | `0` runs standalone |
| `NATS_ROUTES` | | Comma-separated `nats-route://` URLs |
| `CLUSTER_ADVERTISE` | | Public `host:port` for the route |

## Measured

From a prototype with nine regions on four continents, with one node per region, on Akamai Cloud:

- **Added latency:** one edge-to-counter round trip, 0.5–9 ms where a counter is in-region.
- **Convergence:** regions agree within about 100–250 ms. Between continents, add one-way network
  latency.
- **Adapter throughput:** about 47K req/s per 4 vCPU, from a local benchmark. See [sizing](docs/sizing.md).

## Status and limits

This is a working prototype, not a supported product.

- Every request pays one edge-to-counter call, and coverage depends on where you place regions.
- Failing open means an over-limit caller gets through whenever the counter doesn't answer in time.
- A caller spraying many regions at once can overshoot by about rate × convergence delay.
- Every node holds every live key worldwide, so memory scales with distinct keys.
- JWT signature verification is unit-tested under Node but hasn't run on a live EdgeWorker. Test
  `crypto.subtle` with your key type in a sandbox first.
- Tested with one node per region. Multi-node regions (`STREAM_REPLICAS=3`) are the intended
  production shape; test them before relying on them.
