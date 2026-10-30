import { test } from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { createHandlers } from '../src/handlers.js';
import { b64urlToBytes } from '../src/jwt.js';

const subtle = webcrypto.subtle;
const ADAPTER = 'https://rl.test/v1/rl/hit';

function fakeRequest({ host = 'api.test', path = '/v1/item', method = 'GET', query = '', headers = {}, vars = {}, body } = {}) {
  const h = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), [v]]));
  const req = {
    host, path, method, query, scheme: 'https',
    url: path + (query ? '?' + query : ''),
    vars: { ...vars },
    responded: null,
    getHeader: (n) => h[n.toLowerCase()],
    getHeaders: () => h,
    getVariable: (n) => req.vars[n],
    setVariable: (n, v) => { req.vars[n] = v; },
    respondWith: (status, headers, body) => { req.responded = { status, headers, body }; },
    text: async () => body,
  };
  return req;
}

// Fake adapter: exact counting per (policy,key), like rl-adapter without windows.
function fakeDeps({ adapterMode = 'ok' } = {}) {
  const counts = new Map();
  const calls = { adapter: [], origin: [] };
  const deps = {
    subtle,
    logger: { log: () => {} },
    now: () => Date.now(),
    createResponse: (status, headers, body) => ({ status, headers, body }),
    httpRequest: async (url, opts) => {
      if (url === ADAPTER) {
        const req = JSON.parse(opts.body);
        calls.adapter.push({ ...req, token: opts.headers['X-RL-Token'] });
        if (adapterMode === 'hang') return new Promise(() => {});
        if (adapterMode === 'down') return { status: 503, json: async () => ({}) };
        let blocked = false;
        let retry = 0;
        const results = req.hits.map((h) => {
          const id = h.policy + '|' + h.key;
          const c = (counts.get(id) || 0) + h.cost;
          counts.set(id, c);
          const b = c > h.limit;
          if (b) { blocked = true; retry = 42; }
          return { policy: h.policy, count: c, limit: h.limit, blocked: b, retry_after: b ? 42 : 0 };
        });
        return { status: 200, json: async () => ({ blocked, retry_after: retry, results, region: 'test', rtt_ms: 12.5, conn_seq: 1 }) };
      }
      calls.origin.push({ url, ...opts });
      return { status: 200, getHeaders: () => ({ 'content-type': ['text/plain'], 'content-length': ['2'] }), body: 'ok' };
    },
  };
  return { deps, calls };
}

function cfg(policies, extra = {}) {
  return {
    adapter: { url: ADAPTER, tokenVar: 'PMUSER_RL_TOKEN', timeoutMs: 30 },
    clientIp: { variable: 'PMUSER_RL_CLIENT_IP', header: 'True-Client-IP' },
    jwt: { header: 'Authorization', scheme: 'Bearer', verify: false, keys: [] },
    origin: { bypassHeader: 'X-RL-Bypass', bypassVar: 'PMUSER_RL_BYPASS' },
    debugHeader: 'X-RL-Result',
    policies,
    ...extra,
  };
}

function b64url(bytes) {
  return Buffer.from(bytes).toString('base64url');
}

function unsignedJwt(claims) {
  return b64url(Buffer.from(JSON.stringify({ alg: 'none' }))) + '.' + b64url(Buffer.from(JSON.stringify(claims))) + '.';
}

async function signedJwt(alg, claims) {
  const params = alg === 'RS256'
    ? { name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' }
    : { name: 'ECDSA', namedCurve: 'P-256' };
  const kp = await subtle.generateKey(params, true, ['sign', 'verify']);
  const jwk = { ...(await subtle.exportKey('jwk', kp.publicKey)), kid: 'k1' };
  const input = b64url(Buffer.from(JSON.stringify({ alg, kid: 'k1' }))) + '.' + b64url(Buffer.from(JSON.stringify(claims)));
  const sigAlg = alg === 'RS256' ? { name: 'RSASSA-PKCS1-v1_5' } : { name: 'ECDSA', hash: 'SHA-256' };
  const sig = await subtle.sign(sigAlg, kp.privateKey, Buffer.from(input));
  return { token: input + '.' + b64url(new Uint8Array(sig)), jwk };
}

const ipPolicy = { id: 'ip', match: { paths: ['/v1/*'] }, key: ['ip'], limit: 2, window: 600, mode: 'enforce' };

test('enforce: blocks on the call that exceeds the limit, with Retry-After', async () => {
  const { deps, calls } = fakeDeps();
  const h = createHandlers(cfg([ipPolicy]), deps);
  const vars = { PMUSER_RL_CLIENT_IP: '203.0.113.9', PMUSER_RL_TOKEN: 'secret' };
  for (let i = 0; i < 2; i++) {
    const r = fakeRequest({ vars });
    await h.onClientRequest(r);
    assert.equal(r.responded, null);
  }
  const r = fakeRequest({ vars });
  await h.onClientRequest(r);
  assert.equal(r.responded.status, 429);
  assert.deepEqual(r.responded.headers['Retry-After'], ['42']);
  assert.match(r.vars.PMUSER_RL_RESULT, /^n=1;block=ip;rg=test;rtt=12.5;new=1$/);
  assert.equal(calls.adapter[0].token, 'secret');
  assert.match(calls.adapter[0].hits[0].key, /^[0-9a-f]{32}$/);
  assert.ok(!JSON.stringify(calls.adapter).includes('203.0.113.9'), 'raw IP must not leave the edge');
});

test('shadow: counts and reports but never blocks', async () => {
  const { deps } = fakeDeps();
  const h = createHandlers(cfg([{ ...ipPolicy, mode: 'shadow' }]), deps);
  let r;
  for (let i = 0; i < 5; i++) {
    r = fakeRequest({ headers: { 'True-Client-IP': '198.51.100.1' } });
    await h.onClientRequest(r);
    assert.equal(r.responded, null);
  }
  assert.match(r.vars.PMUSER_RL_RESULT, /^n=1;shadow=ip;rg=test;rtt=12.5;new=1$/);
  const resp = { headers: {}, setHeader(k, v) { this.headers[k] = v; } };
  h.onClientResponse(r, resp);
  assert.equal(resp.headers['X-RL-Result'], r.vars.PMUSER_RL_RESULT);
});

test('fail open on adapter timeout and on adapter error', async () => {
  for (const adapterMode of ['hang', 'down']) {
    const { deps } = fakeDeps({ adapterMode });
    const h = createHandlers(cfg([{ ...ipPolicy, limit: 0 }]), deps);
    const r = fakeRequest({ vars: { PMUSER_RL_CLIENT_IP: '1.2.3.4' } });
    const t = Date.now();
    await h.onClientRequest(r);
    assert.equal(r.responded, null, adapterMode);
    assert.match(r.vars.PMUSER_RL_RESULT, /^err=[^;]+;n=1$/);
    assert.ok(Date.now() - t < 500, 'timeout bounded');
  }
});

test('non-matching requests and missing key parts make no adapter call', async () => {
  const { deps, calls } = fakeDeps();
  const h = createHandlers(cfg([ipPolicy, { id: 'u', key: ['jwt:sub'], limit: 1, window: 60, mode: 'enforce' }]), deps);
  await h.onClientRequest(fakeRequest({ path: '/other' })); // ip policy doesn't match; jwt policy has no token
  assert.equal(calls.adapter.length, 0);
});

test('jwt claim + header compose one key; different claims are different keys', async () => {
  const { deps, calls } = fakeDeps();
  const p = { id: 'user', key: ['jwt:sub', 'header:X-Market'], limit: 100, window: 600, mode: 'enforce' };
  const h = createHandlers(cfg([p]), deps);
  for (const sub of ['alice', 'alice', 'bob']) {
    await h.onClientRequest(fakeRequest({ headers: { Authorization: 'Bearer ' + unsignedJwt({ sub }), 'X-Market': 'US' } }));
  }
  const keys = calls.adapter.map((c) => c.hits[0].key);
  assert.equal(keys[0], keys[1]);
  assert.notEqual(keys[0], keys[2]);
});

for (const alg of ['RS256', 'ES256']) {
  test(`jwt verify ${alg}: valid token keys on sub; forged or expired token is ignored`, async () => {
    const now = Math.floor(Date.now() / 1000);
    const good = await signedJwt(alg, { sub: 'alice', exp: now + 300, iss: 'https://idp.test' });
    const expired = await signedJwt(alg, { sub: 'alice', exp: now - 3600 });
    const { deps, calls } = fakeDeps();
    const p = { id: 'user', key: ['jwt:sub'], limit: 100, window: 600, mode: 'enforce' };
    const jwtCfg = { header: 'Authorization', scheme: 'Bearer', verify: true, issuer: 'https://idp.test', keys: [good.jwk] };
    const h = createHandlers(cfg([p], { jwt: jwtCfg }), deps);

    await h.onClientRequest(fakeRequest({ headers: { Authorization: 'Bearer ' + good.token } }));
    assert.equal(calls.adapter.length, 1);

    // Forged: same claims, signature from a different key.
    const forged = await signedJwt(alg, { sub: 'alice', exp: now + 300, iss: 'https://idp.test' });
    await h.onClientRequest(fakeRequest({ headers: { Authorization: 'Bearer ' + forged.token } }));
    await h.onClientRequest(fakeRequest({ headers: { Authorization: 'Bearer ' + unsignedJwt({ sub: 'alice' }) } }));
    const h2 = createHandlers(cfg([p], { jwt: { ...jwtCfg, keys: [expired.jwk], issuer: undefined } }), deps);
    await h2.onClientRequest(fakeRequest({ headers: { Authorization: 'Bearer ' + expired.token } }));
    assert.equal(calls.adapter.length, 1, 'forged, unsigned and expired tokens must not produce a key');
  });
}

test('responseProvider: body policy (JSON and form) enforces before origin', async () => {
  const { deps, calls } = fakeDeps();
  const p = { id: 'search', match: { methods: ['POST'] }, key: ['ip', 'body:q.term'], limit: 1, window: 600, mode: 'enforce' };
  const h = createHandlers(cfg([p]), deps);
  const mk = () => fakeRequest({ method: 'POST', vars: { PMUSER_RL_CLIENT_IP: '1.1.1.1' },
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ q: { term: 'gpu' } }) });
  const first = await h.responseProvider(mk());
  assert.equal(first.status, 200);
  assert.equal(calls.origin.length, 1);
  assert.equal(calls.origin[0].body, JSON.stringify({ q: { term: 'gpu' } }), 'body forwarded intact');
  assert.deepEqual(calls.origin[0].headers['x-rl-bypass'] ?? calls.origin[0].headers['X-RL-Bypass'], ['1']);
  const second = await h.responseProvider(mk());
  assert.equal(second.status, 429);
  assert.match(second.headers['X-RL-Result'][0], /^n=1;block=search;rg=test;rtt=12.5;new=1$/, '429s carry the decision header');
  assert.equal(calls.origin.length, 1, 'blocked request never reaches origin');

  const form = fakeRequest({ method: 'POST', vars: { PMUSER_RL_CLIENT_IP: '1.1.1.1' },
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'q.term=x&other=1' });
  const hf = createHandlers(cfg([{ ...p, key: ['body:other'] }]), fakeDeps().deps);
  assert.equal((await hf.responseProvider(form)).status, 200);
});

test('responseProvider: shadow runs alongside origin and annotates the response', async () => {
  const { deps, calls } = fakeDeps();
  const h = createHandlers(cfg([{ ...ipPolicy, limit: 0, mode: 'shadow' }]), deps);
  const resp = await h.responseProvider(fakeRequest({ vars: { PMUSER_RL_CLIENT_IP: '9.9.9.9' } }));
  assert.equal(resp.status, 200);
  assert.match(resp.headers['X-RL-Result'][0], /^n=1;shadow=ip;rg=test;rtt=12.5;new=1$/);
  assert.equal(resp.headers['content-length'], undefined, 'unsafe headers stripped');
  assert.equal(calls.origin.length, 1);
});

test('b64url decoder handles padding-free input', () => {
  assert.deepEqual([...b64urlToBytes('AQID')], [1, 2, 3]);
  assert.deepEqual([...b64urlToBytes('_-8')], [255, 239]);
});
