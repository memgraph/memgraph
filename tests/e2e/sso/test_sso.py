import base64
import os
import sys

import interactive_mg_runner
import neo4j.exceptions
import pytest
from common import get_data_path, get_logs_path
from neo4j import Auth, GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"

file = "test_sso"


def get_instances(test_name: str):
    return {
        INSTANCE_NAME: {
            "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/test_instance.log",
            "data_directory": f"{get_data_path(file, test_name)}",
            "setup_queries": [],
        }
    }


MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "Authentication failure"
USERNAME = "anthony"


@pytest.fixture(autouse=True)
def wrapper(request):
    # 1. Create roles

    test_name = request.function.__name__
    instances = get_instances(test_name)
    interactive_mg_runner.start_all(instances)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            session.run("CREATE ROLE architect;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    # 2. Restart to use SSO
    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    # 3. Run test
    yield None

    # 4. Stop intance
    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_sso_missing_username():
    response = base64.b64encode(b"skip_username").decode("utf-8")
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
            session.run("MATCH (n) RETURN n;").consume()
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == USERNAME


def test_sso_create_owned():
    # Triggers and streams are owned by the user who made them
    # 1. Create an owned object (trigger) while logged in via SSO
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            session.run(
                """CREATE TRIGGER exampleTrigger1 ON () CREATE AFTER COMMIT
                EXECUTE UNWIND createdVertices AS newNodes SET newNodes.created = timestamp();"""
            ).consume()


def test_sso_ttl():
    # Make sure TTL can be started even under SSO
    # 1. Create an owned object (trigger) while logged in via SSO
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            session.run("""ENABLE TTL AT "14:30:00";""").consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
