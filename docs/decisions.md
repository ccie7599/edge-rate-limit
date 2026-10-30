# Design decisions

## 1. Count in memory and replicate deltas; don't increment a KV per request

An atomic NATS KV increment is a compare-and-swap: one RAFT append and fsync per write. In earlier
testing it peaked around 13K/s per node, and it can't be batched, because a batch is all-or-nothing
across unrelated keys. At hundreds of thousands of requests per second that would take dozens of
leaders and put an inter-region quorum round trip on every request.

So each regional adapter holds sliding-window counters in memory and acts on a hit immediately.
Every 100 ms it publishes one message carrying all its accumulated `(policy, key, slot) += n` deltas
to JetStream, the storage layer NATS KV is built on. Other regions merge them. The streams' `MaxAge`
equals the longest window, so a restarted adapter rebuilds its counters by replay.

- (+) A hit is a local memory operation. JetStream load is about 10 messages/s per adapter, whatever
  the request rate.
- (+) A region sees other regions' hits about one flush interval plus the network delay later.
- (−) A caller spraying many regions at once can overshoot by up to rate × convergence delay. For
  long windows this is noise.
- (−) Every node's memory scales with distinct active keys worldwide. See [sizing](sizing.md).

## 2. One delta stream per region

With one shared stream, every delta passes through that stream's leader. Across continents, a Tokyo
hit would travel through a US leader before Singapore saw it, and losing two of the stream's three
regions would stop replication everywhere.

Each region publishes to its own stream, `RL_D_<REGION>`, placed on that region's servers by a server
tag. Publishing never leaves the region. Every adapter runs one ordered consumer per region stream,
so a delta takes the direct path to each reader. Adapters look for new `RL_D_*` streams every few
seconds, so adding a region needs no restart elsewhere. A region outage stops only that region's
deltas, and on restart its stream replays from local disk.

For production, run at least three nodes per region with `STREAM_REPLICAS=3`. The placement tag keeps
every replica local, so replication cost doesn't cross oceans.

Convergence between regions is bounded by distance. Sydney to Frankfurt is about 250 ms round trip,
so "within 100 ms" holds within a continent; between continents, add the one-way network latency.

## 3. One adapter call per edge request, fail open

The worker batches every matched policy's hit into a single `POST /v1/rl/hit` and enforces on the
response. It bounds the call with a tight timeout and treats a timeout or error as "allow". A timeout
abandons only the reply: the adapter counts a hit when the request arrives, so a late answer still
registers (tested from Sydney: 50 requests, 12 timed out, every region counted 50). Only calls that
never reach the adapter go uncounted.

An edge-cached "is this key blocked" flag was rejected. API callers rarely repeat a key on the same
edge server within a cache TTL, so it adds a call without reducing load.

## 4. Keys are SHA-256 of the composed parts

The adapter stores only `policy + hash`, so raw JWTs, claims and body values never leave the edge.
Tokens rotate and carry personal data, so key on claims such as `sub`, not on the token.

A claim from an **unverified** JWT is as spoofable as any header: a caller can rotate it to evade a
limit. Policies keyed only on claims should set `verify: true` or combine the claim with `ip`.

## 5. Blocked requests still count

Every attempt increments the counter, including those that get a 429. A client that keeps hammering
stays blocked until it backs off, instead of being let through again in bursts.

## 6. Strong consistency through key ownership: deferred

For a strongly consistent limit, the adapter could forward each key to a single owner node. That
costs an inter-region round trip on most requests and makes resharding an event. Worth revisiting
only for a policy that can't tolerate a few hundred milliseconds of convergence.
