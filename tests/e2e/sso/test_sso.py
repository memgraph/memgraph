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
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
TEMP_DIR = tempfile.TemporaryDirectory().name
INSTANCE_NAME = "test_instance"
INSTANCE_DESCRIPTION = {
    INSTANCE_NAME: {
        "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
        "log_file": "sso.log",
        "data_directory": TEMP_DIR,
        "setup_queries": [],
        "skip_auth": True,
    }
}

MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "Authentication failure"
USERNAME = "anthony"


def create_role():
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("CREATE ROLE architect;")
            session.run("GRANT ALL PRIVILEGES TO architect;")

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)


def delete_role():
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run("DROP ROLE architect;")

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)


class TestSSO:
    @pytest.fixture(scope="class")
    def provide_role(self):
        create_role()

        INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
        interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

        yield None

        interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)
        print("aaaaa", INSTANCE_DESCRIPTION)
        INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].pop()

        delete_role()

    def test_sso_with_no_module_provided(self):
        interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)
        response = base64.b64encode(b"dummy_value").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()
        interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)

    def test_sso_module_error(self, provide_role):
        response = base64.b64encode(b"send_error").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")
        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()

    def test_sso_json_with_wrong_fields(self, provide_role):
        response = base64.b64encode(b"wrong_fields").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()

    def test_sso_json_with_wrong_value_types(self, provide_role):
        response = base64.b64encode(b"wrong_value_types").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()

    def test_sso_missing_username(self, provide_role):
        response = base64.b64encode(b"skip_username").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()

    def test_sso_role_does_not_exist(self, provide_role):
        response = base64.b64encode(b"nonexistent_role").decode("utf-8")
        MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with pytest.raises(neo4j.exceptions.ServiceUnavailable, match=CLIENT_ERROR_MESSAGE) as _:
                client.verify_connectivity()

    def test_sso_successful(self, provide_role):
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
    create_role()
    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        client.verify_connectivity()
        with client.session() as session:
            session.run(
                """CREATE TRIGGER exampleTrigger1 ON () CREATE AFTER COMMIT
                EXECUTE UNWIND createdVertices AS newNodes SET newNodes.created = timestamp();"""
            )

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)

    # 2. Start a new session without SSO
    #  * Verify that Memgraph can start up without exceptions (loading streams & triggers)

    INSTANCE_DESCRIPTION[INSTANCE_NAME]["args"].pop()
    interactive_mg_runner.start_all(INSTANCE_DESCRIPTION)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        client.verify_connectivity()

    interactive_mg_runner.stop_instance(INSTANCE_DESCRIPTION, INSTANCE_NAME)
    delete_role()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
