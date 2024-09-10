import base64
import os
import sys

import interactive_mg_runner
import neo4j.exceptions
import pytest
from neo4j import Auth, GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"
INSTANCE_DESCRIPTION = {
    INSTANCE_NAME: {
        "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
        "log_file": "sso.log",
        "setup_queries": [],
        "skip_auth": True,
    }
}

MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "Authentication failure"
USERNAME = "anthony"


@pytest.fixture(autouse=True)
def wrapper():
    # 1. Create roles
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("CREATE ROLE architect;")
            session.run("GRANT ALL PRIVILEGES TO architect;")

    interactive_mg_runner.stop(INSTANCE_DESCRIPTION, INSTANCE_NAME)

    # 2. Restart to use SSO
    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    # 3. Run test
    yield None

    # 4. Stop intance
    interactive_mg_runner.stop(INSTANCE_DESCRIPTION, INSTANCE_NAME, keep_directories=False)
    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].pop()

    # 5. Start instance again and delete role

    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)
    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("DROP ROLE architect;")
    interactive_mg_runner.stop(INSTANCE_DESCRIPTION, INSTANCE_NAME)


def test_sso_with_no_module_provided():
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_module_error():
    response = base64.b64encode(b"send_error").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")
    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_fields():
    response = base64.b64encode(b"wrong_fields").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_json_with_wrong_value_types():
    response = base64.b64encode(b"wrong_value_types").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_missing_username():
    response = base64.b64encode(b"skip_username").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_role_does_not_exist():
    response = base64.b64encode(b"nonexistent_role").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
            client.verify_connectivity()


def test_sso_successful():
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("MATCH (n) RETURN n;")
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == USERNAME


def test_sso_create_owned():
    # Triggers and streams are owned by the user who made them
    # 1. Create an owned object (trigger) while logged in via SSO
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run(
                """CREATE TRIGGER exampleTrigger1 ON () CREATE AFTER COMMIT
                EXECUTE UNWIND createdVertices AS newNodes SET newNodes.created = timestamp();"""
            )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
