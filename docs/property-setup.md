# Akamai property setup

The EdgeWorker needs a few variables and rules on the property that runs it.

## Variables

| Variable | Value | Purpose |
|---|---|---|
| `PMUSER_RL_CLIENT_IP` | Set from `{{builtin.AK_CLIENT_IP}}` | Client IP for `ip`-keyed policies |
| `PMUSER_RL_TOKEN` | Shared secret | Sent to the adapter as `X-RL-Token`. Must be non-sensitive so the EdgeWorker can read it; mark it hidden. |
| `PMUSER_RL_RESULT` | Empty | The worker writes its decision summary here |
| `PMUSER_RL_BYPASS` | Random secret | `responseProvider` bundle only: value of `X-RL-Bypass` on the origin sub-request |

Setting the client IP variable:

```json
{
  "name": "setVariable",
  "options": {
    "variableName": "PMUSER_RL_CLIENT_IP",
    "valueSource": "EXPRESSION",
    "transform": "NONE",
    "variableValue": "{{builtin.AK_CLIENT_IP}}"
  }
}
```

## Rules

**Adapter path.** EdgeWorkers sub-requests go through a property, so the adapter URL must be
Akamai-delivered. Route `/v1/rl/*` to a GTM property that resolves to the nearest region, with
caching off.

**Run the worker on the paths you limit:**

```json
{
  "name": "EdgeWorker rate limit",
  "criteria": [
    { "name": "path", "options": { "matchOperator": "MATCHES_ONE_OF", "matchCaseSensitive": false, "values": ["/v1/*"] } }
  ],
  "behaviors": [
    { "name": "edgeWorker", "options": { "enabled": true, "edgeWorkerId": "<your id>" } }
  ]
}
```

**`responseProvider` bundle only: skip the worker on its own origin sub-request**, or it loops. Add
this as a child of the rule above:

```json
{
  "name": "Skip on EdgeWorker origin sub-request",
  "criteria": [
    { "name": "requestHeader", "options": { "headerName": "X-RL-Bypass", "matchOperator": "IS_ONE_OF", "values": ["<PMUSER_RL_BYPASS value>"], "matchCaseSensitiveValue": true } }
  ],
  "behaviors": [
    { "name": "edgeWorker", "options": { "enabled": false } }
  ]
}
```

## GTM

Use a geographic map with one datacenter per region. Write US states and Canadian provinces as
`US/WA` and `CA/ON`: `US-WA` is accepted without error and never matches. GTM doesn't use EDNS
client subnet.

## Debugging

With `debugHeader` set, responses carry a summary such as:

```
X-RL-Result: n=2;block=api-user;rg=eu;rtt=4.1;new=0
```

| Field | Meaning |
|---|---|
| `n` | Hits sent to the adapter |
| `block` / `shadow` | Policies over their limit, by mode |
| `err` | Why the worker failed open |
| `rg` | Region that answered |
| `rtt` | Network round trip from that region to this edge server, in ms |
| `new` | 1 if the call opened a new connection |

Remove `debugHeader` in production if you don't want clients to see it.
