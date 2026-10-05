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
import threading
import time

import interactive_mg_runner
import pytest
from common import get_data_path, get_logs_path
from neo4j import Auth, GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"
FILE = "test_sso_pipe_stale_response"

# Short timeout so GetData fails before the module writes (module sleeps 3s)
AUTH_MODULE_TIMEOUT_MS = 2000

MG_URI = "bolt://localhost:7687"
SECOND_USER = "admin_user"

# Latency cap per query on a pre-authenticated session while a slow SSO login is in flight.
QUERY_MAX_ELAPSED_S = 1.0
# Head start so the slow module call is in flight before probing begins.
SSO_FLIGHT_DELAY_S = 0.3
# Probe deadline relative to login start; must be before AUTH_MODULE_TIMEOUT_MS expires.
PROBE_DEADLINE_S = 1.5
MIN_PROBE_ITERATIONS = 3


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


def test_slow_sso_login_does_not_stall_queries():
    fast_token = base64.b64encode(b"dummy_value").decode("utf-8")
    fast_auth = Auth(scheme="saml-entra-id", credentials=fast_token, principal="")
    slow_token = base64.b64encode(b"delay_then_anthony").decode("utf-8")
    slow_auth = Auth(scheme="saml-entra-id", credentials=slow_token, principal="")

    sso_done = threading.Event()

    def _do_slow_sso():
        # The login fails at the module timeout; only its duration matters here.
        try:
            with GraphDatabase.driver(MG_URI, auth=slow_auth) as slow_client:
                slow_client.verify_connectivity()
        except Exception:
            pass
        finally:
            sso_done.set()

    with GraphDatabase.driver(MG_URI, auth=fast_auth) as pre_client:
        pre_client.verify_connectivity()
        with pre_client.session() as pre_session:
            # Warm the connection so the timed section excludes TCP setup.
            pre_session.run("RETURN 1").consume()

            start = time.monotonic()
            sso_thread = threading.Thread(target=_do_slow_sso, daemon=True)
            sso_thread.start()
            time.sleep(SSO_FLIGHT_DELAY_S)

            queries = ("RETURN 1", "SHOW DATABASES")
            max_elapsed = 0.0
            iterations = 0
            while time.monotonic() - start < PROBE_DEADLINE_S:
                t0 = time.monotonic()
                pre_session.run(queries[iterations % len(queries)]).consume()
                max_elapsed = max(max_elapsed, time.monotonic() - t0)
                iterations += 1

            # Captured before join: proves probing overlapped the module call.
            sso_in_flight = not sso_done.is_set()

    sso_thread.join(timeout=AUTH_MODULE_TIMEOUT_MS / 1000 + 5.0)

    assert max_elapsed < QUERY_MAX_ELAPSED_S, (
        f"A query took {max_elapsed:.3f}s while an SSO login was waiting on the auth module "
        f"(threshold {QUERY_MAX_ELAPSED_S}s) - auth-lock contention detected."
    )
    assert sso_in_flight, "Slow SSO login finished before probing ended; the probe did not overlap the module call."
    assert (
        iterations >= MIN_PROBE_ITERATIONS
    ), f"Only {iterations} probe iteration(s) ran (need >= {MIN_PROBE_ITERATIONS})."
    assert sso_done.is_set(), "Slow SSO login did not finish within the bounded join."


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
