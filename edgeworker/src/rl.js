// Edge rate limiting core. Pure logic with injected EdgeWorkers built-ins so
// it runs unchanged under Node tests.
//
// deps: { httpRequest, createResponse, subtle, logger, now: () => ms }

import * as jwt from './jwt.js';

const SEP = '\u001f';

// ---- policy matching ------------------------------------------------------

function matchesAny(patterns, value, caseless) {
  if (!patterns || patterns.length === 0) return true;
  const v = caseless ? value.toLowerCase() : value;
  return patterns.some((p) => {
    if (p instanceof RegExp) return p.test(value);
    if (p.endsWith('*')) return v.startsWith(caseless ? p.slice(0, -1).toLowerCase() : p.slice(0, -1));
    return v === (caseless ? p.toLowerCase() : p);
  });
}

export function matchPolicies(policies, request) {
  return policies.filter((p) => {
    const m = p.match || {};
    return (
      matchesAny(m.hosts, request.host, true) &&
      matchesAny(m.paths, request.path, false) &&
      matchesAny(m.methods, request.method, true)
    );
  });
}

export const needsBody = (policy) => policy.key.some((k) => k.startsWith('body:'));

// ---- key parts ------------------------------------------------------------

function firstHeader(request, name) {
  const v = request.getHeader(name);
  return v && v.length ? v[0] : undefined;
}

function dig(obj, path) {
  let v = obj;
  for (const seg of path.split('.')) {
    if (v == null || typeof v !== 'object') return undefined;
    v = v[seg];
  }
  return v == null || typeof v === 'object' ? (v == null ? undefined : JSON.stringify(v)) : String(v);
}

function parseBody(text, contentType) {
  if (!text) return null;
  if (contentType && contentType.includes('application/x-www-form-urlencoded')) {
    const out = {};
    for (const pair of text.split('&')) {
      const i = pair.indexOf('=');
      const k = decodeURIComponent((i < 0 ? pair : pair.slice(0, i)).replace(/\+/g, ' '));
      if (!(k in out)) out[k] = decodeURIComponent((i < 0 ? '' : pair.slice(i + 1)).replace(/\+/g, ' '));
    }
    return out;
  }
  try {
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function queryParam(query, name) {
  if (!query) return undefined;
  for (const pair of query.split('&')) {
    const i = pair.indexOf('=');
    if (decodeURIComponent(i < 0 ? pair : pair.slice(0, i)) === name) {
      return decodeURIComponent((i < 0 ? '' : pair.slice(i + 1)).replace(/\+/g, ' '));
    }
  }
  return undefined;
}

// Context resolves key parts lazily and memoizes JWT parsing per request.
export class Context {
  constructor(request, cfg, deps, bodyText) {
    this.request = request;
    this.cfg = cfg;
    this.deps = deps;
    this.bodyText = bodyText;
    this._claims = undefined;
    this._body = undefined;
  }

  ip() {
    const c = this.cfg.clientIp || {};
    if (c.variable) {
      const v = this.request.getVariable(c.variable);
      if (v) return v;
    }
    return c.header ? firstHeader(this.request, c.header) : undefined;
  }

  async claims() {
    if (this._claims !== undefined) return this._claims;
    this._claims = null;
    const jc = this.cfg.jwt || {};
    let raw = firstHeader(this.request, jc.header || 'Authorization');
    if (raw && jc.scheme) {
      const prefix = jc.scheme + ' ';
      raw = raw.slice(0, prefix.length).toLowerCase() === prefix.toLowerCase() ? raw.slice(prefix.length) : undefined;
    }
    const decoded = raw && jwt.decode(raw.trim());
    if (!decoded) return null;
    if (jc.verify) {
      this._claims = await jwt.verify(decoded, jc, this.deps.subtle, Math.floor(this.deps.now() / 1000));
    } else {
      this._claims = decoded.payload;
    }
    return this._claims;
  }

  body() {
    if (this._body === undefined) {
      this._body = parseBody(this.bodyText, firstHeader(this.request, 'Content-Type'));
    }
    return this._body;
  }

  async part(spec) {
    if (spec === 'ip') return this.ip();
    const i = spec.indexOf(':');
    const kind = spec.slice(0, i);
    const name = spec.slice(i + 1);
    switch (kind) {
      case 'header':
        return firstHeader(this.request, name);
      case 'query':
        return queryParam(this.request.query, name);
      case 'jwt': {
        const c = await this.claims();
        return c ? dig(c, name) : undefined;
      }
      case 'body': {
        const b = this.body();
        return b ? dig(b, name) : undefined;
      }
      case 'const':
        return name;
      default:
        throw new Error('unknown key part ' + spec);
    }
  }
}

function hex(buf) {
  const b = new Uint8Array(buf);
  let s = '';
  for (let i = 0; i < b.length; i++) s += b[i].toString(16).padStart(2, '0');
  return s;
}

// UTF-8 encode without relying on a TextEncoder global.
function utf8Bytes(s) {
  const e = encodeURIComponent(s);
  const out = [];
  for (let i = 0; i < e.length; i++) {
    if (e[i] === '%') {
      out.push(parseInt(e.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      out.push(e.charCodeAt(i));
    }
  }
  return new Uint8Array(out);
}

// buildHits returns adapter hits for policies whose key parts all resolved.
// A policy with a missing part is skipped (e.g. no JWT); pair claim-keyed
// policies with an ip-keyed one so dropping the token isn't an escape hatch.
export async function buildHits(policies, ctx) {
  const hits = [];
  for (const p of policies) {
    const parts = [];
    let missing = false;
    for (const spec of p.key) {
      const v = await ctx.part(spec);
      if (v === undefined || v === '') {
        missing = true;
        break;
      }
      parts.push(v);
    }
    if (missing) continue;
    const digest = await ctx.deps.subtle.digest('SHA-256', utf8Bytes(p.id + SEP + parts.join(SEP)));
    hits.push({ policy: p.id, key: hex(digest).slice(0, 32), limit: p.limit, window: p.window, cost: p.cost || 1 });
  }
  return hits;
}

// ---- adapter call ---------------------------------------------------------

// callAdapter returns the adapter decision, or { error } on timeout/failure (fail open).
// The EdgeWorkers clock doesn't advance during a request, so the worker can't
// time this call; the adapter reports its side instead (rtt_ms, conn_seq).
export async function callAdapter(hits, cfg, deps, token) {
  const a = cfg.adapter;
  const timeoutMs = a.timeoutMs || 50;
  // httpRequest's own `timeout` option is the bound on EdgeWorkers; the timer
  // race is a backstop where a setTimeout global exists (Node tests, sandbox).
  let timer;
  const races = [];
  if (typeof setTimeout === 'function') {
    races.push(new Promise((resolve) => {
      timer = setTimeout(() => resolve('timeout'), timeoutMs);
    }));
  }
  try {
    const call = deps.httpRequest(a.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-RL-Token': token || '' },
      body: JSON.stringify({ hits }),
      timeout: timeoutMs,
    });
    const resp = await Promise.race([call, ...races]);
    if (resp === 'timeout') return { error: 'timeout' };
    if (resp.status !== 200) return { error: 'status_' + resp.status };
    return await resp.json();
  } catch (e) {
    // Keep the reason header-safe and short: "exception:<first words of the message>".
    // EdgeWorkers prefixes sub-request errors with "<file>:<line> Subrequest to URL <url>"; keep what follows.
    const raw = String((e && e.message) || e || '').replace(/^.*?subrequest to url \S+\s*/i, '');
    const msg = raw.toLowerCase().replace(/[^a-z0-9 _.-]/g, '').trim().slice(0, 48);
    return { error: msg ? 'exception:' + msg.replace(/ /g, '_') : 'exception' };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

// decide splits the adapter result by policy mode.
export function decide(result, policies) {
  const mode = Object.fromEntries(policies.map((p) => [p.id, p.mode || 'shadow']));
  const out = { block: false, retryAfter: 0, enforced: [], shadowed: [], error: result && result.error,
    region: result && result.region, rtt: result && result.rtt_ms, connSeq: result && result.conn_seq };
  if (!result || result.error || !Array.isArray(result.results)) return out;
  for (const r of result.results) {
    if (!r.blocked) continue;
    if (mode[r.policy] === 'enforce') {
      out.block = true;
      out.retryAfter = Math.max(out.retryAfter, r.retry_after || 0);
      out.enforced.push(r.policy);
    } else {
      out.shadowed.push(r.policy);
    }
  }
  return out;
}

// summary is what we stash in a PMUSER variable for onClientResponse / logs.
// rg is the adapter region that answered, rtt its network round trip to this
// edge server in ms, and new=1 when the call opened a fresh connection (paying
// TCP and TLS setup on top of the request round trip).
export function summary(decision, hits) {
  const s = decision.error ? ['err=' + decision.error, 'n=' + hits.length] : ['n=' + hits.length];
  if (decision.enforced.length) s.push('block=' + decision.enforced.join(','));
  if (decision.shadowed.length) s.push('shadow=' + decision.shadowed.join(','));
  if (decision.region) s.push('rg=' + decision.region);
  if (decision.rtt) s.push('rtt=' + decision.rtt);
  if (decision.connSeq) s.push('new=' + (decision.connSeq === 1 ? 1 : 0));
  return s.join(';');
}

export function tooMany(cfg, decision) {
  const body = JSON.stringify({ error: 'rate_limited', retry_after: decision.retryAfter });
  return {
    status: cfg.blockStatus || 429,
    headers: { 'Content-Type': ['application/json'], 'Retry-After': [String(decision.retryAfter)], 'Cache-Control': ['no-store'] },
    body,
  };
}
