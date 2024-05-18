import base64
import os
import sys
import tempfile

import interactive_mg_runner
import neo4j.exceptions
import pytest
from neo4j import Auth, GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
TEMP_DIR = tempfile.TemporaryDirectory().name
INSTANCE_NAME = "test_instance"
INSTANCE_DESCRIPTION = {
    INSTANCE_NAME: {
        "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
        "log_file": "sso/main.log",
        "data_directory": TEMP_DIR,
        "setup_queries": [],
        "skip_auth": True,
    }
}

MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "{code: Memgraph.ClientError.Security.Unauthenticated} {message: Authentication failure}"


@pytest.fixture
def provide_role():
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("CREATE ROLE architect;")
            session.run("GRANT MATCH TO architect;")

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)

    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    yield None

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)
    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].pop()

    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("DROP ROLE architect;")

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)


def test_sso_empty_module():
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()
    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)


def test_sso_module_error(provide_role):
    response = base64.b64encode(b"send_error").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_fields(provide_role):
    response = base64.b64encode(b"wrong_fields").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_value_types(provide_role):
    response = base64.b64encode(b"wrong_value_types").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_missing_username(provide_role):
    response = base64.b64encode(b"skip_username").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_role_does_not_exist(provide_role):
    response = base64.b64encode(b"nonexistent_role").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ClientError, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_successful(provide_role):
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        client.verify_connectivity()

        client.execute_query("MATCH (n) RETURN n;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
