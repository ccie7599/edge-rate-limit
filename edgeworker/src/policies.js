// Example rate-limit policies, compiled into the bundle. Replace the hosts,
// paths and limits with your own.
//
// Every policy here starts in shadow mode: counted and reported in the debug
// header, never blocked. Set mode: 'enforce' per policy once the counts look right.
//
// key parts (combinable, joined then SHA-256'd at the edge):
//   ip              client IP (PMUSER variable, else header)
//   header:<name>   first value of a request header
//   query:<name>    query-string parameter
//   jwt:<claim>     claim from the bearer JWT (dot path); set jwt.verify for anti-spoofing
//   body:<field>    JSON (dot path) or form field; responseProvider bundle only
//   const:<value>   literal, for a single shared bucket

export default {
  adapter: {
    // Must be an Akamai-delivered hostname (EdgeWorkers sub-requests go through a property),
    // fronting the rl-adapter GTM property.
    url: 'https://rl-adapter.example.com/v1/rl/hit',
    tokenVar: 'PMUSER_RL_TOKEN',
    timeoutMs: 150,
  },
  clientIp: { variable: 'PMUSER_RL_CLIENT_IP', header: 'True-Client-IP' },
  jwt: {
    header: 'Authorization',
    scheme: 'Bearer',
    verify: false, // set true and supply keys (JWKs) before enforcing on jwt:* alone
    keys: [],
    issuer: undefined,
    audience: undefined,
  },
  origin: { bypassHeader: 'X-RL-Bypass', bypassVar: 'PMUSER_RL_BYPASS' },
  debugHeader: 'X-RL-Result',
  blockStatus: 429,

  policies: [
    {
      // Credential stuffing: logins per client IP.
      id: 'login-ip',
      match: { hosts: ['api.example.com'], paths: ['/oauth2/token'], methods: ['POST'] },
      key: ['ip'],
      limit: 60,
      window: 600,
      mode: 'shadow',
    },
    {
      // Per-user quota from a JWT claim, split by tenant header.
      id: 'api-user',
      match: { hosts: ['api.example.com'], paths: ['/v1/*'] },
      key: ['jwt:sub', 'header:X-Tenant-ID'],
      limit: 3000,
      window: 600,
      mode: 'shadow',
    },
    {
      // Catches callers who drop the token: a policy with a missing key part is skipped.
      id: 'api-ip',
      match: { hosts: ['api.example.com'], paths: ['/v1/*'] },
      key: ['ip'],
      limit: 20000,
      window: 600,
      mode: 'shadow',
    },
    {
      // The same search repeated by one user. Needs the responseProvider bundle.
      id: 'search-repeat',
      match: { hosts: ['api.example.com'], paths: ['/v1/search'], methods: ['POST'] },
      key: ['jwt:sub', 'body:query'],
      limit: 300,
      window: 600,
      mode: 'shadow',
    },
  ],
};
