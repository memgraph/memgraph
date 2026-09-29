"""
Queries on an already-authenticated session must not stall while another SSO
login is waiting on the auth module.
"""

from __future__ import annotations

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
FILE = "test_sso_auth_lock_contention"
MG_URI = "bolt://localhost:7687"

# seconds the delay_then_anthony module case sleeps before responding.
MODULE_SLEEP_S: float = 3.0
# Timeout must exceed the module sleep so the slow SSO login can actually complete.
AUTH_MODULE_TIMEOUT_MS: int = 6000
# latency cap per query during the module-sleep contention window.
QUERY_MAX_ELAPSED_S: float = 1.0
# Head start before probing begins, so the module call is in flight.
SSO_FLIGHT_DELAY_S: float = 0.3
# Probe deadline relative to thread start; must be well before MODULE_SLEEP_S.
PROBE_DEADLINE_S: float = 2.0
# Minimum probe iterations — ensures the loop ran inside the contention window.
MIN_PROBE_ITERATIONS: int = 3


def get_instances(test_name: str) -> dict:
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

    # Phase 1: start without the auth module so we can create roles.
    interactive_mg_runner.start_all(instances)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            session.run("CREATE ROLE architect;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()
            session.run("CREATE ROLE admin;").consume()
            session.run("GRANT ALL PRIVILEGES TO admin;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    # Phase 2: restart with the dummy SSO module active.
    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    yield

    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_auth_lock_no_contention_with_slow_sso() -> None:
    fast_token = base64.b64encode(b"dummy_value").decode("utf-8")
    fast_auth = Auth(scheme="saml-entra-id", credentials=fast_token, principal="")

    # Slow SSO path: module sleeps MODULE_SLEEP_S before returning.
    slow_token = base64.b64encode(b"delay_then_anthony").decode("utf-8")
    slow_auth = Auth(scheme="saml-entra-id", credentials=slow_token, principal="")

    sso_succeeded = threading.Event()
    sso_errors: list[Exception] = []

    def _do_slow_sso() -> None:
        try:
            with GraphDatabase.driver(MG_URI, auth=slow_auth) as slow_client:
                slow_client.verify_connectivity()
            sso_succeeded.set()
        except Exception as exc:
            sso_errors.append(exc)

    with GraphDatabase.driver(MG_URI, auth=fast_auth) as pre_client:
        pre_client.verify_connectivity()

        with pre_client.session() as pre_session:
            # Warm the connection so the timed section does not include TCP setup.
            pre_session.run("RETURN 1").consume()

            thread_start = time.monotonic()
            sso_thread = threading.Thread(target=_do_slow_sso, daemon=True)
            sso_thread.start()

            time.sleep(SSO_FLIGHT_DELAY_S)

            queries = ("RETURN 1", "SHOW DATABASES")
            max_elapsed: float = 0.0
            iteration_count: int = 0

            while time.monotonic() - thread_start < PROBE_DEADLINE_S:
                query = queries[iteration_count % len(queries)]
                t0 = time.monotonic()
                pre_session.run(query).consume()
                elapsed = time.monotonic() - t0
                if elapsed > max_elapsed:
                    max_elapsed = elapsed
                iteration_count += 1

            # Capture before join — proves the loop ran while the module was sleeping.
            sso_in_flight = not sso_succeeded.is_set()

    sso_thread.join(timeout=MODULE_SLEEP_S + AUTH_MODULE_TIMEOUT_MS / 1000 + 2.0)

    assert max_elapsed < QUERY_MAX_ELAPSED_S, (
        f"A query took {max_elapsed:.3f}s while the SSO module was sleeping {MODULE_SLEEP_S}s "
        f"(threshold {QUERY_MAX_ELAPSED_S}s) — auth-lock contention detected."
    )
    assert sso_in_flight, (
        "The slow SSO login completed before the probe loop ended — "
        "the probing window did not overlap the auth module call; "
        f"increase PROBE_DEADLINE_S (currently {PROBE_DEADLINE_S}s) or SSO_FLIGHT_DELAY_S."
    )
    assert iteration_count >= MIN_PROBE_ITERATIONS, (
        f"Only {iteration_count} probe iteration(s) ran inside the contention window "
        f"(need >= {MIN_PROBE_ITERATIONS}); the test may not have exercised the lock path."
    )
    assert not sso_errors, f"Slow SSO login raised an unexpected error: {sso_errors[0]}"
    assert sso_succeeded.is_set(), (
        "Slow SSO login did not complete within the expected window — "
        "check AUTH_MODULE_TIMEOUT_MS and the module sleep duration."
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
