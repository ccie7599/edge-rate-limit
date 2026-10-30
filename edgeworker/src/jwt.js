// Minimal JWT decode + optional signature verification (RS256, ES256) using
// WebCrypto. `subtle` is injected so the same code runs in EdgeWorkers
// (crypto.subtle from the 'crypto' built-in) and in Node tests.

const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
const B64_LOOKUP = (() => {
  const t = new Int16Array(128).fill(-1);
  for (let i = 0; i < B64.length; i++) t[B64.charCodeAt(i)] = i;
  t['+'.charCodeAt(0)] = 62;
  t['/'.charCodeAt(0)] = 63;
  return t;
})();

export function b64urlToBytes(s) {
  s = s.replace(/=+$/, '');
  const out = new Uint8Array(Math.floor((s.length * 3) / 4));
  let bits = 0, acc = 0, o = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    const v = c < 128 ? B64_LOOKUP[c] : -1;
    if (v < 0) throw new Error('bad base64url');
    acc = (acc << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out[o++] = (acc >> bits) & 0xff;
    }
  }
  return out.subarray(0, o);
}

function utf8(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += '%' + bytes[i].toString(16).padStart(2, '0');
  return decodeURIComponent(s);
}

function asciiBytes(s) {
  const b = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) b[i] = s.charCodeAt(i);
  return b;
}

// decode returns {header, payload, signingInput, signature} or null if malformed.
export function decode(token) {
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  try {
    return {
      header: JSON.parse(utf8(b64urlToBytes(parts[0]))),
      payload: JSON.parse(utf8(b64urlToBytes(parts[1]))),
      signingInput: asciiBytes(parts[0] + '.' + parts[1]),
      signature: b64urlToBytes(parts[2]),
    };
  } catch (_) {
    return null;
  }
}

const ALGS = {
  RS256: { importAlg: { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, verifyAlg: { name: 'RSASSA-PKCS1-v1_5' } },
  ES256: { importAlg: { name: 'ECDSA', namedCurve: 'P-256' }, verifyAlg: { name: 'ECDSA', hash: 'SHA-256' } },
};

// Imported keys are cached per isolate; key material comes from bundle config.
const keyCache = new Map();

async function importKey(subtle, jwk, alg) {
  const id = alg + ':' + (jwk.kid || jwk.n || jwk.x);
  let k = keyCache.get(id);
  if (!k) {
    k = await subtle.importKey('jwk', jwk, ALGS[alg].importAlg, false, ['verify']);
    keyCache.set(id, k);
  }
  return k;
}

// verify checks signature (against cfg.keys, matched by kid when present) and
// exp/nbf/iss/aud. Returns the payload on success, null on any failure.
export async function verify(decoded, cfg, subtle, nowSec) {
  const { header, payload } = decoded;
  const alg = ALGS[header.alg];
  if (!alg || !cfg.keys || cfg.keys.length === 0) return null;
  const candidates = cfg.keys.filter((k) => !header.kid || !k.kid || k.kid === header.kid);
  let ok = false;
  for (const jwk of candidates) {
    try {
      const key = await importKey(subtle, jwk, header.alg);
      if (await subtle.verify(alg.verifyAlg, key, decoded.signature, decoded.signingInput)) {
        ok = true;
        break;
      }
    } catch (_) {
      // wrong key type for this alg; try the next
    }
  }
  if (!ok) return null;
  const skew = cfg.clockSkewSec ?? 30;
  if (typeof payload.exp === 'number' && nowSec > payload.exp + skew) return null;
  if (typeof payload.nbf === 'number' && nowSec < payload.nbf - skew) return null;
  if (cfg.issuer && payload.iss !== cfg.issuer) return null;
  if (cfg.audience) {
    const aud = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
    if (!aud.includes(cfg.audience)) return null;
  }
  return payload;
}
