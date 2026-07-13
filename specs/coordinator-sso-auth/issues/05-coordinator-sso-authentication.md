## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Enable real SSO authentication on coordinators, built scheme-agnostically and demonstrated
with OIDC.

End-to-end behavior:

- Replace the coordinator branch of the Bolt handshake's authentication so that:
  basic/none schemes always succeed (passthrough, credentials ignored); a scheme present in
  `--auth-module-mappings` runs the SSO path; any other/unknown scheme is rejected.
- Add a dedicated coordinator SSO authenticator (session/glue layer) that reuses the existing
  auth-module subprocess machinery to run the module, then validates the returned roles
  against the coordinator's committed role list rather than the auth kvstore. Authentication
  succeeds only if every returned role exists; an invalid token, any missing role, or an
  empty/invalid response rejects the connection. There is no privilege gating once
  authenticated.
- SSO authentication and the role queries are enterprise-gated: without a valid license they
  are rejected with a license error, while basic-auth passthrough keeps working.
- `MEMGRAPH_SSO_*` variables are inherited into the spawned module subprocess; no new C++ env
  parsing is added.

The authenticator is shaped as a testable unit (scheme + IdP response → accepted/rejected,
with an injected module-runner and an injected role-existence provider), though this slice's
verification is through the e2e suite.

This slice extends `auth.py` with an OIDC dummy module and fixtures.

## Acceptance criteria

- [ ] On a coordinator, basic/none auth always succeeds; an SSO scheme in the mappings runs the module; an unknown scheme is rejected.
- [ ] SSO succeeds only when every role returned by the module exists in the coordinator role list; invalid token / missing role / multi-role with any missing → rejected.
- [ ] Multi-role response with all roles existing → accepted.
- [ ] SSO auth and role queries require a valid enterprise license; without it they are rejected with a license error while basic-auth passthrough still works.
- [ ] The role-existence check reads the coordinator's committed role list; the auth kvstore is not used for coordinator roles.
- [ ] E2E (OIDC dummy module through the real Bolt handshake): success; bad-token rejection; missing-role rejection; multi-role all-exist success / any-missing rejection; SSO for a role rejected before `CREATE ROLE` and succeeding after.

## Blocked by

- `03-coordinator-role-store.md`
