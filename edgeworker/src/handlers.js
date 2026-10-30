// EdgeWorker event handlers, built from config + injected built-ins.
//
// Two deployment shapes (see the README):
//   onClientRequest  — header / ip / jwt / query policies; CDN origin path unchanged.
//   responseProvider — adds body: policies; the worker forwards to origin itself.
// Both stash a summary in PMUSER_RL_RESULT; onClientResponse can surface it as a
// response header (cfg.debugHeader) for shadow-mode validation.

import { matchPolicies, needsBody, Context, buildHits, callAdapter, decide, summary, tooMany } from './rl.js';

const RESULT_VAR = 'PMUSER_RL_RESULT';

// Headers not forwarded on the origin sub-request.
const HOP_BY_HOP = new Set([
  'host', 'connection', 'keep-alive', 'proxy-connection', 'te', 'trailer', 'transfer-encoding',
  'upgrade', 'content-length', 'accept-encoding', 'pragma', 'expect',
]);

// Response headers EdgeWorkers manage themselves.
const UNSAFE_RESPONSE = new Set(['content-length', 'transfer-encoding', 'connection', 'vary', 'content-encoding', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailers', 'upgrade']);

export function createHandlers(cfg, deps) {
  const token = (request) => (cfg.adapter.tokenVar ? request.getVariable(cfg.adapter.tokenVar) : cfg.adapter.token);

  function setResult(request, s) {
    try {
      request.setVariable(RESULT_VAR, s);
    } catch (_) {
      // variable not defined on the property; observability only
    }
  }

  async function onClientRequest(request) {
    const policies = matchPolicies(cfg.policies, request).filter((p) => !needsBody(p));
    if (policies.length === 0) return;
    const ctx = new Context(request, cfg, deps);
    const hits = await buildHits(policies, ctx);
    if (hits.length === 0) return;
    const decision = decide(await callAdapter(hits, cfg, deps, token(request)), policies);
    setResult(request, summary(decision, hits));
    if (decision.error) deps.logger.log('rl fail-open %s', decision.error);
    if (decision.block) {
      const r = tooMany(cfg, decision);
      if (cfg.debugHeader) r.headers[cfg.debugHeader] = [summary(decision, hits)];
      request.respondWith(r.status, r.headers, r.body);
    }
  }

  function onClientResponse(request, response) {
    if (!cfg.debugHeader) return;
    const v = request.getVariable(RESULT_VAR);
    if (v) response.setHeader(cfg.debugHeader, v);
  }

  async function responseProvider(request) {
    const policies = matchPolicies(cfg.policies, request);
    const wantBody = policies.some(needsBody);
    const bodyText = request.method === 'GET' || request.method === 'HEAD' ? undefined : await request.text();

    let decision = null;
    let pending = null;
    let hits = [];
    if (policies.length) {
      const ctx = new Context(request, cfg, deps, wantBody ? bodyText : undefined);
      hits = await buildHits(policies, ctx);
    }
    if (hits.length) {
      pending = callAdapter(hits, cfg, deps, token(request));
      // Only enforce-mode policies are worth delaying the origin call for.
      if (policies.some((p) => p.mode === 'enforce')) {
        decision = decide(await pending, policies);
        if (decision.block) {
          const r = tooMany(cfg, decision);
          if (cfg.debugHeader) r.headers[cfg.debugHeader] = [summary(decision, hits)];
          return deps.createResponse(r.status, r.headers, r.body);
        }
      }
    }

    const headers = {};
    for (const [k, v] of Object.entries(request.getHeaders())) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) headers[k] = v;
    }
    // The property skips this EdgeWorker when the bypass header is present,
    // so the origin sub-request doesn't loop back into responseProvider.
    if (cfg.origin && cfg.origin.bypassHeader) headers[cfg.origin.bypassHeader] = [request.getVariable(cfg.origin.bypassVar) || '1'];
    const url = `${request.scheme}://${request.host}${request.url}`;
    const originResp = await deps.httpRequest(url, { method: request.method, headers, body: bodyText });

    if (pending && !decision) decision = decide(await pending, policies); // shadow-only: ran in parallel
    const respHeaders = {};
    for (const [k, v] of Object.entries(originResp.getHeaders())) {
      if (!UNSAFE_RESPONSE.has(k.toLowerCase())) respHeaders[k] = v;
    }
    if (decision) {
      if (decision.error) deps.logger.log('rl fail-open %s', decision.error);
      if (cfg.debugHeader) respHeaders[cfg.debugHeader] = [summary(decision, hits)];
    }
    return deps.createResponse(originResp.status, respHeaders, originResp.body);
  }

  return { onClientRequest, onClientResponse, responseProvider };
}
