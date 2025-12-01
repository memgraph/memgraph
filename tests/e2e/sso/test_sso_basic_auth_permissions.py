import os
import sys

import interactive_mg_runner
import pytest
from common import get_data_path, get_logs_path
from neo4j import GraphDatabase, basic_auth

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"

file = "test_sso_basic_auth_permissions"

MG_URI = "bolt://localhost:7687"
BASIC_AUTH_USERNAME = "user"
BASIC_AUTH_PASSWORD = "password"
BASIC_AUTH_ROLE = "admin"


def get_instances(test_name: str):
    return {
        INSTANCE_NAME: {
            "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/test_instance.log",
            "data_directory": f"{get_data_path(file, test_name)}",
            "setup_queries": [],
        }
    }


@pytest.fixture(autouse=True)
def wrapper(request):
    """
    Setup fixture that:
    1. Creates roles and basic auth user before SSO is enabled
    2. Enables SSO module
    3. Runs the test
    4. Cleans up
    """
    test_name = request.function.__name__
    instances = get_instances(test_name)
    interactive_mg_runner.start_all(instances)

    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            session.run("CREATE ROLE admin;").consume()
            session.run("GRANT ALL PRIVILEGES TO admin;").consume()

            session.run(f"CREATE USER {BASIC_AUTH_USERNAME} IDENTIFIED BY '{BASIC_AUTH_PASSWORD}';").consume()
            session.run(f"SET ROLE FOR {BASIC_AUTH_USERNAME} TO {BASIC_AUTH_ROLE};").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    yield None

    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_basic_auth_user_has_correct_permissions():
    """
    Test that basic auth user has correct admin permissions even when SSO module is enabled.
    """
    with GraphDatabase.driver(MG_URI, auth=basic_auth(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD)) as client:
        client.verify_connectivity()
        with client.session() as session:
            # verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1
            assert current_user_result[0]["user"] == BASIC_AUTH_USERNAME

            # verify current role
            current_role_result = list(session.run("SHOW CURRENT ROLE;"))
            assert len(current_role_result) == 1
            assert current_role_result[0]["role"] == BASIC_AUTH_ROLE

            # should be able to create nodes
            session.run("CREATE (n:TestNode {name: 'test', value: 42});").consume()
            result = list(session.run("MATCH (n:TestNode) RETURN n.name AS name, n.value AS value;"))
            assert len(result) == 1
            assert result[0]["name"] == "test"
            assert result[0]["value"] == 42

            # should be able to update
            session.run("MATCH (n:TestNode) SET n.value = 100;").consume()
            result = list(session.run("MATCH (n:TestNode) RETURN n.value AS value;"))
            assert len(result) == 1
            assert result[0]["value"] == 100

            # should be able to delete
            session.run("MATCH (n:TestNode) DELETE n;").consume()
            result = list(session.run("MATCH (n:TestNode) RETURN count(n) AS count;"))
            assert len(result) == 1
            assert result[0]["count"] == 0

            # should be able to manage users
            session.run("CREATE USER test_user IDENTIFIED BY 'test_pass';").consume()
            session.run("DROP USER test_user;").consume()

            create_trigger_query = """
                CREATE TRIGGER test_trigger
                ON CREATE AFTER COMMIT EXECUTE
                UNWIND createdVertices AS node
                SET node:test_label;
                """

            session.run(create_trigger_query).consume()
            result = list(session.run("SHOW TRIGGERS;"))
            assert len(result) == 1

            session.run("DROP TRIGGER test_trigger;").consume()
            result = list(session.run("SHOW TRIGGERS;"))
            assert len(result) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
