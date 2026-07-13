## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Prove that SAML and Kerberos authenticate on coordinators exactly the same way as OIDC, and
close the long-standing gap that Kerberos has no server-path coverage (today it exists only
as an isolated Python unit test with mocked GSSAPI/LDAP).

Because the coordinator SSO path is scheme-agnostic, this slice is primarily coverage: extend
the dummy auth module and fixtures to handle the SAML and Kerberos schemes and drive them
through the real coordinator Bolt handshake.

End-to-end behavior:

- A SAML client and a Kerberos client each authenticate to a coordinator through the Bolt
  handshake, exercising the same authenticate-and-validate-roles path as OIDC: success when
  returned roles exist, rejection on bad token / missing role.
- Kerberos is specifically exercised through the coordinator Bolt handshake (not just as a
  standalone module unit test).

## Acceptance criteria

- [ ] The dummy auth module and fixtures cover the SAML and Kerberos schemes (success + error responses).
- [ ] E2E: SAML through the Bolt handshake — success when role exists; rejection on bad token and on missing role.
- [ ] E2E: Kerberos through the Bolt handshake — success when role exists; rejection on bad token and on missing role.
- [ ] Kerberos coverage goes through the coordinator SSO server path, not an isolated module import.

## Blocked by

- `05-coordinator-sso-authentication.md`
