# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Pytest configuration for parallel e2e tests.

This module provides fixtures and configuration for running e2e tests in parallel
using pytest-xdist. Each worker gets its own Memgraph instance on a unique port.
Instances are managed via the standard interactive_mg_runner module.
"""

import os
import sys
import time
from pathlib import Path
from typing import Generator, Optional

import pytest
from neo4j import GraphDatabase

# Add standard e2e directory to sys.path
E2E_ROOT = Path(__file__).resolve().parent.parent
if str(E2E_ROOT) not in sys.path:
    sys.path.insert(0, str(E2E_ROOT))

import interactive_mg_runner

from memgraph import connectable_port

# Base port for Memgraph instances
BASE_BOLT_PORT = 7687


def pytest_addoption(parser):
    """Add custom command line arguments for parallel tests."""
    parser.addoption(
        "--num-workers",
        action="store",
        default="8",
        help="Number of internal Memgraph worker threads (bolt-num-workers)",
    )


@pytest.fixture(scope="session")
def num_workers(request) -> int:
    """Get the number of internal worker threads from command line."""
    return int(request.config.getoption("--num-workers"))


@pytest.fixture(scope="session")
def memgraph_port() -> int:
    """Get the Memgraph port."""
    return BASE_BOLT_PORT


class MemgraphWrapper:
    """Wrapper that provides helper methods expected by tests."""

    def __init__(self, runner, port, auth: Optional[tuple] = None):
        self.runner = runner
        self.bolt_port = port
        self.uri = f"bolt://localhost:{port}"
        self.auth = auth

    def get_driver(self):
        """Get a neo4j driver for this instance."""
        return GraphDatabase.driver(self.uri, auth=self.auth)

    def with_auth(self, username, password):
        """Return a new wrapper instance with specified credentials."""
        return MemgraphWrapper(self.runner, self.bolt_port, auth=(username, password))

    def execute_query(self, query: str, params: Optional[dict] = None) -> None:
        """Execute a query without returning results."""
        with self.get_driver() as driver:
            with driver.session() as session:
                session.run(query, params or {})

    def fetch_all(self, query: str, params: Optional[dict] = None) -> list:
        """Execute a query and return all results as list of dicts."""
        with self.get_driver() as driver:
            with driver.session() as session:
                result = session.run(query, params or {})
                return [record.data() for record in result]

    def fetch_one(self, query: str, params: Optional[dict] = None):
        """Execute a query and return the first result."""
        results = self.fetch_all(query, params)
        return results[0] if results else None

    def clear_database(self) -> None:
        """Clear all data from the database."""
        self.execute_query("MATCH (n) DETACH DELETE n")


@pytest.fixture(scope="session")
def memgraph_instance(memgraph_port: int, num_workers: int) -> Generator[MemgraphWrapper, None, None]:
    """
    Session-scoped fixture that provides a Memgraph instance via interactive_mg_runner.
    """
    instance_name = "parallel_e2e_instance"

    # Configure runner paths for the current environment
    build_root = E2E_ROOT.parent.parent
    interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
    interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

    config = {
        instance_name: {
            "args": [
                f"--bolt-port={memgraph_port}",
                "--log-level=WARNING",
                "--storage-wal-enabled=true",
                "--storage-snapshot-interval-sec=300",
                "--storage-properties-on-edges=true",
                "--telemetry-enabled=false",
                f"--bolt-num-workers={num_workers}",
            ],
            # Runner prepends BUILD_DIR/e2e/logs/ and BUILD_DIR/e2e/data/
            "log_file": "parallel_e2e.log",
            "data_directory": "parallel_e2e_data",
        }
    }

    interactive_mg_runner.start(config, instance_name)
    runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

    wrapper = MemgraphWrapper(runner, memgraph_port)
    yield wrapper

    interactive_mg_runner.stop(config, instance_name, keep_directories=False)


@pytest.fixture(scope="session")
def memgraph_auth_instance(memgraph_instance: MemgraphWrapper) -> MemgraphWrapper:
    """
    Session-scoped fixture that provides an authenticated Memgraph wrapper.
    Creates an admin user if needed and returns a wrapper with auth credentials.
    Use this fixture for tests that require user authentication (e.g., test_fgas).
    """
    # Create admin user if it's a fresh instance (no users yet)
    try:
        memgraph_instance.execute_query("CREATE USER admin IDENTIFIED BY 'test'")
    except Exception:
        # If this fails, it's likely because the admin user already exists
        # or authentication is already required.
        pass

    return memgraph_instance.with_auth("admin", "test")


@pytest.fixture
def memgraph(memgraph_instance: MemgraphWrapper) -> Generator[MemgraphWrapper, None, None]:
    """
    Function-scoped fixture that provides a clean Memgraph instance.
    """
    memgraph_instance.clear_database()
    yield memgraph_instance


@pytest.fixture
def memgraph_auth(memgraph_auth_instance: MemgraphWrapper) -> Generator[MemgraphWrapper, None, None]:
    """
    Function-scoped fixture that provides a clean Memgraph instance.
    """
    memgraph_auth_instance.clear_database()
    yield memgraph_auth_instance


@pytest.fixture
def neo4j_driver(memgraph_instance: MemgraphWrapper):
    """
    Function-scoped fixture that provides a neo4j driver.
    """
    driver = memgraph_instance.get_driver()
    yield driver
    driver.close()


@pytest.fixture
def empty_db(memgraph: MemgraphWrapper) -> MemgraphWrapper:
    """Fixture providing a guaranteed empty database."""
    memgraph.clear_database()
    return memgraph


@pytest.fixture
def single_element_db(memgraph: MemgraphWrapper) -> MemgraphWrapper:
    """Fixture providing a database with a single element."""
    memgraph.clear_database()
    memgraph.execute_query("CREATE (:A{p:1})-[:E{p:1}]->(:B{p:1})")
    return memgraph


@pytest.fixture
def thread_count_db(memgraph: MemgraphWrapper, num_workers: int) -> MemgraphWrapper:
    """Fixture providing a database with some elements for parallel testing."""
    memgraph.clear_database()
    memgraph.execute_query(f"UNWIND range(1, {num_workers}) AS i CREATE (:A{{p:i}})-[:E{{p:i}}]->(:B{{p:i}})")
    return memgraph


@pytest.fixture
def large_db(memgraph: MemgraphWrapper) -> MemgraphWrapper:
    """Fixture providing a database with many elements."""
    memgraph.clear_database()
    memgraph.execute_query("UNWIND range(1, 10000) AS i CREATE (:A{p:i})-[:E{p:i}]->(:B{p:i})")
    return memgraph
