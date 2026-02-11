"""
Test that verifies the pipe-drain fix for the "wrong user" bug.

Scenario: First SSO request causes the auth module to respond after our GetData
timeout. That response is left in the pipe. A second connection then authenticates.
Without draining the pipe after GetData failure, we would read the first response
for the second request and return the previous user (wrong-user bug).
With the fix we drain one line after GetData fails, so the second connection
gets its own response and the correct user.
"""
import base64
import os
import sys

import interactive_mg_runner
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
FILE = "test_sso_pipe_stale_response"

# Short timeout so GetData fails before the module writes (module sleeps 3s)
AUTH_MODULE_TIMEOUT_MS = 2000

MG_URI = "bolt://localhost:7687"
SECOND_USER = "admin_user"


def get_instances(test_name: str):
    return {
        INSTANCE_NAME: {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--data-recovery-on-startup=true",
                f"--auth-module-timeout-ms={AUTH_MODULE_TIMEOUT_MS}",
            ],
            "log_file": f"{get_logs_path(FILE, test_name)}/test_instance.log",
            "data_directory": f"{get_data_path(FILE, test_name)}",
            "setup_queries": [],
        }
    }


@pytest.fixture(autouse=True)
def wrapper(request):
    test_name = request.function.__name__
    instances = get_instances(test_name)
    interactive_mg_runner.start_all(instances)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            session.run("CREATE ROLE architect;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()
            session.run("CREATE ROLE admin;").consume()
            session.run("GRANT ALL PRIVILEGES TO admin;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    yield None

    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_second_connection_gets_correct_user_after_first_times_out():
    # Sanity check: successful SSO connection to verify module and roles work.
    success_token = base64.b64encode(b"dummy_value").decode("utf-8")
    auth_success = Auth(scheme="saml-entra-id", credentials=success_token, principal="")
    with GraphDatabase.driver(MG_URI, auth=auth_success) as client:
        client.verify_connectivity()
        with client.session() as session:
            result = list(session.run("SHOW CURRENT USER;"))
            assert len(result) == 1
            assert result[0]["user"] == "anthony"

    # First connection: token triggers module to sleep 3s then return anthony.
    # GetData times out (2s), so we never read that response. At 3s the script
    # writes; without drain that line stays in the pipe and the second connection
    # reads it (wrong user). With the fix we drain so the second gets its own.
    delay_token = base64.b64encode(b"delay_then_anthony").decode("utf-8")
    auth_delay = Auth(scheme="saml-entra-id", credentials=delay_token, principal="")

    try:
        with GraphDatabase.driver(MG_URI, auth=auth_delay) as client:
            client.verify_connectivity()
    except Exception:
        # First connection may fail (timeout) or succeed after drain; both are ok
        pass

    # Second connection: different user. Without the drain fix we would read the
    # first response (anthony) from the pipe and get the wrong user.
    second_token = base64.b64encode(b"admin_user").decode("utf-8")
    auth_second = Auth(scheme="saml-entra-id", credentials=second_token, principal="")

    with GraphDatabase.driver(MG_URI, auth=auth_second) as client:
        client.verify_connectivity()
        with client.session() as session:
            result = list(session.run("SHOW CURRENT USER;"))
            assert len(result) == 1
            assert result[0]["user"] == SECOND_USER, (
                f"Expected current user {SECOND_USER} (without pipe drain fix, "
                f"stale first response would yield anthony)"
            )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
