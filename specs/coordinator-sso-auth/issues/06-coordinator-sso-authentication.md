## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Enable real SSO authentication on coordinators for all three schemes (OIDC, SAML, Kerberos),
which behave identically because the coordinator SSO path is scheme-agnostic, and prove that
the READ/WRITE privilege model actually restricts SSO sessions.

End-to-end behavior:

- Replace the coordinator branch of the Bolt handshake's authentication so that:
  basic/none schemes always succeed (passthrough, full WRITE, credentials ignored); a scheme
  present in `--auth-module-mappings` runs the SSO path; any other/unknown scheme is rejected.
- Add a dedicated coordinator SSO authenticator (session/glue layer) that reuses the existing
  auth-module subprocess machinery to run the module, then validates the returned roles
  against the coordinator's committed role set rather than the auth kvstore. Authentication
  succeeds only if every returned role exists; an invalid token, any missing role, or an
  empty/invalid response rejects the connection.
- On success, the session carries an **effective privilege mask** equal to the union of the
  matched roles' masks. The privilege enforcement built in slice 04 then applies: a READ-only
  session can read the routing table and run the read/introspection queries but is denied
  every mutating query; a WRITE session can run everything; a session whose role(s) have no
  grant is denied everything including the routing table.
- SSO authentication and the role/privilege queries are enterprise-gated: without a valid
  license they are rejected with a license error, while basic-auth passthrough keeps working
  with full WRITE.
- `MEMGRAPH_SSO_*` variables are inherited into the spawned module subprocess; no new C++ env
  parsing is added.

The authenticator is shaped as a testable unit (scheme + IdP response → accepted/rejected +
effective mask, with an injected module-runner and an injected role/mask provider), though
this slice's verification is through the e2e suite.

This slice extends `auth.py` with a dummy auth module and fixtures covering OIDC, SAML, and
Kerberos, all driven through the real coordinator Bolt handshake. Kerberos in particular is
exercised through the server SSO path (not just as an isolated Python module import),
closing the long-standing gap where Kerberos had no server-path coverage.

## Acceptance criteria

- [ ] On a coordinator, basic/none auth always succeeds with full WRITE; an SSO scheme in the mappings runs the module; an unknown scheme is rejected.
- [ ] SSO succeeds only when every role returned by the module exists; invalid token / missing role / multi-role with any missing → rejected; multi-role all-exist → accepted.
- [ ] The session's effective privilege is the union of its matched roles' masks; a bare-role session is denied everything including the routing table.
- [ ] Privilege enforcement bites via SSO: a READ-only SSO session reads the routing table + read/introspection queries but is denied mutating queries; a WRITE SSO session runs everything.
- [ ] SSO auth and role/privilege queries require a valid enterprise license; without it they are rejected while basic-auth passthrough (full WRITE) still works.
- [ ] The role-existence and mask lookup read the coordinator's committed role set; the auth kvstore is not used for coordinator roles.
- [ ] The dummy auth module and fixtures cover OIDC, SAML, and Kerberos schemes (success + error responses).
- [ ] E2E (OIDC through the real Bolt handshake): success; bad-token rejection; missing-role rejection; multi-role all-exist success / any-missing rejection; SSO for a role rejected before `CREATE ROLE` and succeeding after.
- [ ] E2E (SAML through the Bolt handshake): success when role exists with a privilege; rejection on bad token and on missing role.
- [ ] E2E (Kerberos through the Bolt handshake): success when role exists with a privilege; rejection on bad token and on missing role; coverage goes through the coordinator SSO server path, not an isolated module import.

## Blocked by

- `04-coordinator-privilege-model.md`
- `05-role-query-forwarding.md`
