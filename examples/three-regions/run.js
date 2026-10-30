// Runs the real EdgeWorker handlers under Node against three local regions.
// One user's requests rotate through the regions, as if arriving at different
// edge servers worldwide, and the limit holds on the combined count.
//
//   docker compose up -d --build && node run.js
import { createHandlers } from '../../edgeworker/src/handlers.js';

const REGIONS = { us: 8081, eu: 8082, ap: 8083 };
const TOKEN = 'local-example-token';
const LIMIT = 30;
const USER = 'user-' + Date.now();

const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
const jwt = `${b64({ alg: 'none' })}.${b64({ sub: USER })}.sig`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function httpRequest(url, opts) {
  const r = await fetch(url, { ...opts, signal: AbortSignal.timeout(opts.timeout || 1000) });
  return { status: r.status, json: () => r.json() };
}

// One set of handlers per region; on the edge, GTM makes this choice.
const edges = Object.fromEntries(Object.entries(REGIONS).map(([region, port]) => [region, createHandlers({
  adapter: { url: `http://127.0.0.1:${port}/v1/rl/hit`, token: TOKEN, timeoutMs: 500 },
  jwt: { header: 'Authorization', scheme: 'Bearer', verify: false },
  policies: [{ id: 'per-user', match: { paths: ['/v1/*'] }, key: ['jwt:sub'], limit: LIMIT, window: 60, mode: 'enforce' }],
}, { httpRequest, subtle: crypto.subtle, logger: console, now: Date.now })]));

async function send(region) {
  let status = 200;
  await edges[region].onClientRequest({
    host: 'api.example.com', path: '/v1/orders', method: 'GET', url: '/v1/orders',
    getHeader: (n) => (n.toLowerCase() === 'authorization' ? ['Bearer ' + jwt] : undefined),
    getVariable: () => undefined,
    setVariable: () => {},
    respondWith: (s) => { status = s; },
  });
  return status;
}

const sent = { us: 0, eu: 0, ap: 0 };
const blocked = { us: 0, eu: 0, ap: 0 };
let first = 0;
const regions = Object.keys(REGIONS);
const TOTAL = 60;
for (let i = 0; i < TOTAL; i++) {
  const region = regions[i % regions.length];
  sent[region]++;
  if ((await send(region)) === 429) {
    blocked[region]++;
    first ||= i + 1;
  }
  await sleep(50);
}

console.log(`limit ${LIMIT} per 60 s, ${TOTAL} requests for one user, rotating through ${regions.length} regions\n`);
console.log('region  sent  blocked  count seen by region');
await sleep(300);
const { createHash } = await import('node:crypto');
const key = createHash('sha256').update(`per-user\u001f${USER}`).digest('hex').slice(0, 32);
for (const [region, port] of Object.entries(REGIONS)) {
  const { count } = await (await fetch(`http://127.0.0.1:${port}/v1/rl/peek?policy=per-user&key=${key}`)).json();
  console.log(`${region.padEnd(6)}  ${String(sent[region]).padEnd(4)}  ${String(blocked[region]).padEnd(7)}  ${count}`);
}
console.log(first
  ? `\nFirst 429 at request ${first}. Each region received only ${Math.max(...Object.values(sent))} requests, so counting per region would never have reached the limit of ${LIMIT}.`
  : '\nNo request was blocked. Check that the three regions are clustered: curl localhost:8081/v1/health');
