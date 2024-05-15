import base64
import sys

import neo4j.exceptions
import pytest
from neo4j import Auth, GraphDatabase

MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "{code: Memgraph.ClientError.Security.Unauthenticated} {message: Authentication failure}"


def test_sso_module_error():
    response = base64.b64encode(b"send_error").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_fields():
    response = base64.b64encode(b"wrong_fields").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_value_types():
    response = base64.b64encode(b"wrong_value_types").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_missing_username():
    response = base64.b64encode(b"skip_username").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_role_does_not_exist():
    response = base64.b64encode(b"nonexistent_role").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_successful():
    BASIC_AUTH = ("", "")

    with GraphDatabase.driver(MG_URI, auth=BASIC_AUTH) as client:
        client.verify_connectivity()

        with client.session() as session:
            session.run("CREATE ROLE architect;")
            session.run("GRANT MATCH TO architect;")

    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        client.verify_connectivity()

        client.execute_query("MATCH (n) RETURN n;")

    with GraphDatabase.driver(MG_URI, auth=BASIC_AUTH) as client:
        client.verify_connectivity()

        with client.session() as session:
            session.run("DROP ROLE architect;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
