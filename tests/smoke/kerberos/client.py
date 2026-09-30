#!/usr/bin/env python3
"""Log in to Memgraph over Bolt with a real Kerberos service ticket.

Runs inside the KDC container (see setup-kdc.sh), which is where the krb5
client config, the realm's KDC and python3-gssapi live. It:

  1. gets a TGT for the client principal straight from its password, so no
     kinit and no credential cache are needed;
  2. asks the KDC for a service ticket for Memgraph's service principal and
     wraps it in a GSSAPI init token;
  3. hands that token to Memgraph as the Bolt "kerberos" credentials, which is
     exactly what neo4j's kerberos_auth() puts on the wire;
  4. checks Memgraph mapped the ticket onto the expected user and role, that
     the role's privileges actually apply, and that a bogus ticket is refused.

Mutual authentication is deliberately not requested: the reference auth module
(src/auth/reference_modules/kerberos.py) rejects a handshake whose acceptor has
to send a token back to the client.
"""

import base64
import os
import sys

import gssapi
from gssapi.exceptions import GSSError
from gssapi.raw import acquire_cred_with_password
from neo4j import GraphDatabase, kerberos_auth
from neo4j.exceptions import Neo4jError


def service_ticket(client_principal: str, password: str, service_principal: str) -> str:
    """Return a base64 GSSAPI init token for service_principal."""
    client_name = gssapi.Name(client_principal, gssapi.NameType.kerberos_principal)
    creds = acquire_cred_with_password(client_name, password.encode(), usage="initiate").creds
    # A kerberos_principal target name is used verbatim, unlike a hostbased
    # service name, which krb5 would try to canonicalize via DNS first.
    target = gssapi.Name(service_principal, gssapi.NameType.kerberos_principal)
    ctx = gssapi.SecurityContext(
        name=target,
        creds=creds,
        usage="initiate",
        mech=gssapi.MechType.kerberos,
        flags=gssapi.RequirementFlag.out_of_sequence_detection,
    )
    token = ctx.step()
    if not token:
        raise RuntimeError("GSSAPI returned no init token for " + service_principal)
    return base64.b64encode(token).decode("ascii")


def check_login(uri: str, ticket: str, expected_user: str, expected_role: str) -> None:
    with GraphDatabase.driver(uri, auth=kerberos_auth(ticket)) as driver:
        driver.verify_connectivity()
        with driver.session() as session:
            users = [record["user"] for record in session.run("SHOW CURRENT USER;")]
            roles = [record["role"] for record in session.run("SHOW CURRENT ROLE;")]
            print(f"SHOW CURRENT USER -> {users}, SHOW CURRENT ROLE -> {roles}")
            if users != [expected_user]:
                raise AssertionError(f"expected user [{expected_user!r}], got {users}")
            if roles != [expected_role]:
                raise AssertionError(f"expected role [{expected_role!r}], got {roles}")

            # The mapped role holds ALL PRIVILEGES, so a write has to go
            # through: this separates "authenticated" from "authorized".
            session.run("CREATE (:KerberosSmokeTest);").consume()
            count = session.run("MATCH (n:KerberosSmokeTest) RETURN count(n) AS count;").single()["count"]
            if count != 1:
                raise AssertionError(f"expected the mapped role to be able to write, got count {count}")


def check_bogus_ticket_refused(uri: str) -> None:
    """A ticket the keytab can't decrypt must not authenticate anyone."""
    bogus = base64.b64encode(b"not-a-kerberos-ticket").decode("ascii")
    try:
        with GraphDatabase.driver(uri, auth=kerberos_auth(bogus)) as driver:
            driver.verify_connectivity()
    except Exception as error:  # noqa: BLE001 - any refusal is a pass
        print(f"Bogus ticket refused, as expected: {type(error).__name__}")
        return
    raise AssertionError("a bogus Kerberos ticket was accepted")


def main() -> int:
    uri = os.environ["MEMGRAPH_URI"]
    service_principal = os.environ["KRB5_SERVICE_PRINCIPAL"]

    ticket = service_ticket(
        os.environ["KRB5_CLIENT_PRINCIPAL"],
        os.environ["KRB5_CLIENT_PASSWORD"],
        service_principal,
    )
    print(f"Got a service ticket for {service_principal} ({len(ticket)} base64 chars)")

    check_login(uri, ticket, os.environ["KRB5_EXPECTED_USER"], os.environ["KRB5_EXPECTED_ROLE"])
    check_bogus_ticket_refused(uri)
    print("Kerberos authentication OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, Neo4jError, RuntimeError, GSSError) as error:
        print(f"Kerberos authentication FAILED: {error}", file=sys.stderr)
        sys.exit(1)
