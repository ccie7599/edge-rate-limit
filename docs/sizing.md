# Sizing

A first pass from a local benchmark, for an illustrative 500K req/s peak. Load test your own fleet
before relying on these numbers.

| Input | Value |
|---|---|
| Planning peak | 500K req/s |
| Policies matched per request | 2 (one adapter call carries both) |
| Longest window | 600 s (`MAX_WINDOW_SEC=600`) |

## CPU

- About **47K req/s per 4 vCPU** with 2 hits per request. The load generator shared the same 8-core
  host, so this is a floor. Server-side handling averages 23 µs per request.
- The counter itself runs at about 26 ns per hit. JSON and HTTP are the bottleneck.
- Planning figure: 40K req/s per 4 vCPU, so 500K needs about 13 × 4 vCPU. Spread across regions with
  N+1, that's roughly 8 regions × 2 × 8-vCPU nodes.

## Replication

- Each delta entry is about 50 bytes.
- Upper bound: one entry per hit, or 1M entries/s across the cluster at peak, which is about
  **50 MB/s**. The 100 ms flush coalesces repeat hits, so real traffic comes in under that.
- Every node merges all regions' deltas, so plan for that much inbound per node.
- Message rate stays around 10 messages/s per node. Bytes are the cost, not messages.
- Stream disk at that rate: 50 MB/s × 600 s ≈ **30 GB**.
- If needed: compress delta messages (key IDs are hex, so expect about 2×).

## Memory

- Every node holds every live key worldwide, at about 250 bytes per key.
- 10M distinct keys per window is about 2.5 GB per node; 100M is about 25 GB.
- Distinct keys per window is the number most likely to change the design. Measure it. If it's far
  above 100M, consider key-ownership routing, or replicate only keys above a fraction of their limit.

## EdgeWorkers

- One sub-request per matched request, all policies batched. `responseProvider` adds one origin
  sub-request.
- Edge-to-adapter round trip sets the added latency, so place adapters near your traffic.
- Check the EdgeWorkers resource tier limits and per-invocation pricing at your volume.
