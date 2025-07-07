import logging
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Dict, List

import pytest
import yaml
from gqlalchemy import Memgraph, Node
from gqlalchemy import Path as path_gql
from gqlalchemy import Relationship
from mgclient import Node as node_mgclient
from mgclient import Relationship as relationship_mgclient

log = logging.getLogger("memgraph.tests.query_modules")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(name)s] %(message)s")


# create one memgraph instance that gets used on all tests
# since its a lot faster than creating new one for each test
@pytest.fixture(scope="session")
def db():
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
    BUILD_PATH = os.path.join(PROJECT_DIR, "build", "memgraph")
    QM_PATH = os.path.join(PROJECT_DIR, "build", "query_modules")
    LOGS_PATH = os.path.join(PROJECT_DIR, "build", "memgraph-logs")
    os.makedirs(os.path.join(LOGS_PATH), exist_ok=True)

    ARGS = [
        "--telemetry-enabled=false",
        "--storage-properties-on-edges=true",
        f"--query-modules-directory={QM_PATH}",
        "--log-level=TRACE",
        "--log-file={}".format(os.path.join(LOGS_PATH, "memgraph.log")),
    ]

    process = subprocess.Popen([BUILD_PATH] + ARGS, stderr=subprocess.STDOUT)
    pid = process.pid
    memgraph = Memgraph()
    timeout = 15
    # wait for memgraph to start
    while timeout > 0:
        try:
            memgraph.execute("RETURN 1;")
            log.info(f"Memgraph ready.")
            break
        except Exception as e:
            log.info(f"Memgraph not ready yet, retrying in 1 second.")
            timeout -= 1
            time.sleep(1)
    if timeout == 0:
        log.error(f"Memgraph did not start in time.")
        raise Exception("Memgraph did not start in time.")
    log.info(f"Memgraph started as pid: {pid}")
    yield memgraph

    try:
        os.kill(pid, signal.SIGTERM)
        log.info(f"Memgraph successfully terminated on pid: {pid}.")
    except OSError as e:
        log.error(f"Killing Memgraph failed with error: {e}")


class TestConstants:
    ABSOLUTE_TOLERANCE = 1e-3

    EXCEPTION = "exception"
    INPUT_FILE = "input.cyp"
    OUTPUT = "output"
    QUERY = "query"
    TEST_FILE = "test.yml"
    FILENAME_PLACEHOLDER = "_file"
    TEST_MODULE_DIR_SUFFIX = "_test"
    TEST_GROUP_DIR_SUFFIX = "_group"

    ONLINE_TEST_E2E_SETUP = "setup"
    ONLINE_TEST_E2E_CLEANUP = "cleanup"
    ONLINE_TEST_E2E_INPUT_QUERIES = "queries"
    ONLINE_TEST_SUBDIR_SUFFIX = "test_online"

    EXPORT_TEST_E2E_NODES = "nodes"
    EXPORT_TEST_E2E_RELATIONSHIPS = "relationships"
    EXPORT_TEST_E2E_INPUT_QUERIES = "queries"
    EXPORT_TEST_E2E_EXPORT_QUERY = "export"
    EXPORT_TEST_E2E_IMPORT_QUERY = "import"
    EXPORT_TEST_E2E_PLACEHOLDER_FILENAME = "_exportfile"
    EXPORT_TEST_E2E_OUTPUT_FILE = "/".join([os.getcwd(), "_exported_data"])
    EXPORT_TEST_SUBDIR_SUFFIX = "test_export"


def _node_to_dict(data):
    labels = data.labels if hasattr(data, "labels") else (data._labels if isinstance(data, Node) else [])
    properties = data.properties if hasattr(data, "properties") else data._properties
    return {"labels": list(labels), "properties": properties}


def _relationship_to_dict(data):
    label = data.type if hasattr(data, "type") else (data._type if isinstance(data, Relationship) else "")
    properties = data.properties if hasattr(data, "properties") else data._properties
    return {"label": label, "properties": properties}


def _path_to_dict(data):
    nodes = data.nodes if hasattr(data, "nodes") else data._nodes
    relationships = data.relationships if hasattr(data, "relationships") else data._relationships
    return {
        "nodes": [_node_to_dict(node) for node in nodes],
        "relationships": [_relationship_to_dict(relationship) for relationship in relationships],
    }


def _replace(data, match_classes):
    if isinstance(data, dict):
        return {k: _replace(v, match_classes) for k, v in data.items()}
    elif isinstance(data, list):
        return [_replace(i, match_classes) for i in data]
    elif isinstance(data, float):
        return pytest.approx(data, abs=TestConstants.ABSOLUTE_TOLERANCE)
    elif isinstance(data, node_mgclient) or isinstance(data, Node):
        return _node_to_dict(data)
    elif isinstance(data, relationship_mgclient) or isinstance(data, Relationship):
        return _relationship_to_dict(data)
    elif isinstance(data, path_gql):
        return _path_to_dict(data)
    else:
        return _node_to_dict(data) if isinstance(data, match_classes) else data


def _replace_filename(query: str, dir: Path):
    return query.replace(TestConstants.FILENAME_PLACEHOLDER, "/".join([str(dir), "file"]))


def prepare_tests():
    """
    Fetch all the tests in the testing folders, and prepare them for execution
    """
    tests = []

    test_path = Path().cwd()
    for module_test_dir in test_path.iterdir():
        if not module_test_dir.is_dir() or not module_test_dir.name.endswith(TestConstants.TEST_MODULE_DIR_SUFFIX):
            continue

        for test_or_group_dir in module_test_dir.iterdir():
            if not test_or_group_dir.is_dir():
                continue

            if test_or_group_dir.name.endswith(TestConstants.TEST_GROUP_DIR_SUFFIX):
                for test_dir in test_or_group_dir.iterdir():
                    if not test_dir.is_dir():
                        continue

                    tests.append(
                        pytest.param(
                            test_dir,
                            id=f"{module_test_dir.stem}-{test_or_group_dir.stem}-{test_dir.stem}",
                        )
                    )
            else:
                tests.append(
                    pytest.param(
                        test_or_group_dir,
                        id=f"{module_test_dir.stem}-{test_or_group_dir.stem}",
                    )
                )
    return tests


def _load_yaml(path: Path) -> Dict:
    """
    Load YAML based file in Python dictionary.
    """
    file_handle = path.open("r")
    return yaml.load(file_handle, Loader=yaml.Loader)


def _execute_cyphers(input_cyphers: List[str], db: Memgraph):
    """
    Execute commands against Memgraph
    """
    for query in input_cyphers:
        db.execute(query)


def _run_test(test_dict: Dict, db: Memgraph):
    """
    Run queries on Memgraph and compare them to expected results stored in test_dict
    """
    test_query = test_dict[TestConstants.QUERY]
    output_test = TestConstants.OUTPUT in test_dict
    exception_test = TestConstants.EXCEPTION in test_dict

    if not (output_test ^ exception_test):
        pytest.fail("Test file has no valid format.")

    if output_test:
        result_query = list(db.execute_and_fetch(test_query))

        result = _replace(result_query, Node)

        expected = test_dict[TestConstants.OUTPUT]

        assert result == expected

    if exception_test:
        # TODO: Implement for different kinds of errors
        with pytest.raises(Exception):
            db.execute(test_query)


def _get_nodes_and_relationships(nodes_query: str, relationships_query: str, db: Memgraph):
    return (
        list(db.execute_and_fetch(nodes_query)),
        list(db.execute_and_fetch(relationships_query)),
    )


def _test_export(test_dir: Path, db: Memgraph):
    """
    Testing export modules.
    """

    input_dict = _load_yaml(test_dir.joinpath(TestConstants.INPUT_FILE))

    queries = input_dict.get(TestConstants.EXPORT_TEST_E2E_INPUT_QUERIES, None)
    db.execute(queries)

    nodes_query = input_dict.get(TestConstants.EXPORT_TEST_E2E_NODES, None)
    relationships_query = input_dict.get(TestConstants.EXPORT_TEST_E2E_RELATIONSHIPS, None)

    old_nodes, old_relationships = _get_nodes_and_relationships(nodes_query, relationships_query, db)

    test_dict = _load_yaml(test_dir.joinpath(TestConstants.TEST_FILE))
    export_query = test_dict[TestConstants.EXPORT_TEST_E2E_EXPORT_QUERY].replace(
        TestConstants.EXPORT_TEST_E2E_PLACEHOLDER_FILENAME,
        "".join(["'", TestConstants.EXPORT_TEST_E2E_OUTPUT_FILE, "'"]),
    )
    import_query = test_dict[TestConstants.EXPORT_TEST_E2E_IMPORT_QUERY].replace(
        TestConstants.EXPORT_TEST_E2E_PLACEHOLDER_FILENAME,
        "".join(["'", TestConstants.EXPORT_TEST_E2E_OUTPUT_FILE, "'"]),
    )

    db.execute(export_query)
    db.execute("MATCH (n) DETACH DELETE n;")
    db.execute(import_query)

    try:
        os.remove(TestConstants.EXPORT_TEST_E2E_OUTPUT_FILE)
    except Exception:
        raise OSError("Could not delete file.")

    new_nodes, new_relationships = _get_nodes_and_relationships(nodes_query, relationships_query, db)

    assert _replace(old_nodes, Node) == _replace(new_nodes, Node)
    assert _replace(old_relationships, Relationship) == _replace(new_relationships, Relationship)


def _test_static(test_dir: Path, db: Memgraph):
    """
    Testing static modules.
    """
    input_cyphers = [
        _replace_filename(query, test_dir)
        for query in test_dir.joinpath(TestConstants.INPUT_FILE).open("r").readlines()
    ]
    _execute_cyphers(input_cyphers, db)

    tests_list_or_dict = _load_yaml(test_dir.joinpath(TestConstants.TEST_FILE))
    if isinstance(tests_list_or_dict, dict):
        # If the test file is a dict, meaning only one test is present -> just run it
        tests_list_or_dict[TestConstants.QUERY] = _replace_filename(tests_list_or_dict[TestConstants.QUERY], test_dir)
        _run_test(tests_list_or_dict, db)
    else:
        for test_dict in tests_list_or_dict:
            if TestConstants.QUERY not in test_dict:
                pytest.fail(f"Test file {test_dir} has no valid format.")
            # Replace filename placeholder in query
            test_dict[TestConstants.QUERY] = _replace_filename(test_dict[TestConstants.QUERY], test_dir)
            _run_test(test_dict, db)


def _test_online(test_dir: Path, db: Memgraph):
    """
    Testing online modules. Checkpoint testing
    """
    checkpoint_input = _load_yaml(test_dir.joinpath(TestConstants.INPUT_FILE))
    checkpoint_test_dicts = _load_yaml(test_dir.joinpath(TestConstants.TEST_FILE))

    setup_cyphers = checkpoint_input.get(TestConstants.ONLINE_TEST_E2E_SETUP, None)
    checkpoint_input_cyphers = checkpoint_input[TestConstants.ONLINE_TEST_E2E_INPUT_QUERIES]
    cleanup_cyphers = checkpoint_input.get(TestConstants.ONLINE_TEST_E2E_CLEANUP, None)

    # Run optional setup queries
    if setup_cyphers:
        _execute_cyphers(setup_cyphers.splitlines(), db)

    try:
        # Execute cypher queries and compare them with results
        for input_cyphers_raw, test_dict in zip(checkpoint_input_cyphers, checkpoint_test_dicts):
            input_cyphers = input_cyphers_raw.splitlines()
            _execute_cyphers(input_cyphers, db)
            _run_test(test_dict, db)
    finally:
        # Run optional cleanup queries
        if cleanup_cyphers:
            _execute_cyphers(cleanup_cyphers.splitlines(), db)


tests = prepare_tests()


@pytest.mark.parametrize("test_dir", tests)
def test_end2end(test_dir: Path, db: Memgraph):
    db.drop_database()

    if test_dir.name.startswith(TestConstants.EXPORT_TEST_SUBDIR_SUFFIX):
        _test_export(test_dir, db)
    elif test_dir.name.startswith(TestConstants.ONLINE_TEST_SUBDIR_SUFFIX):
        _test_online(test_dir, db)
    else:
        _test_static(test_dir, db)

    db.drop_database()
    db.drop_indexes()
    db.ensure_constraints([])
