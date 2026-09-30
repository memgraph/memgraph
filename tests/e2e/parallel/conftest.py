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
    Uses the session-scoped instance but clears data between tests.
    """
    memgraph_instance.clear_database()
    yield memgraph_instance


@pytest.fixture
def fresh_memgraph(num_workers: int) -> Generator[MemgraphWrapper, None, None]:
    """
    Function-scoped fixture that provides a completely fresh Memgraph instance.

    Starts a new Memgraph process for each test and stops it afterward.
    Use this for tests that may leave the instance in a bad state (e.g., memory tests).

    This is slower than the shared `memgraph` fixture but provides full isolation.
    """
    import uuid

    instance_name = f"fresh_instance_{uuid.uuid4().hex[:8]}"
    port = 7690  # Use a different port to avoid conflicts

    # Configure runner paths
    build_root = E2E_ROOT.parent.parent
    interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
    interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

    config = {
        instance_name: {
            "args": [
                f"--bolt-port={port}",
                "--log-level=WARNING",
                "--storage-wal-enabled=true",
                "--storage-snapshot-interval-sec=300",
                "--storage-properties-on-edges=true",
                "--telemetry-enabled=false",
                f"--bolt-num-workers={num_workers}",
            ],
            "log_file": f"{instance_name}.log",
            "data_directory": f"{instance_name}_data",
        }
    }

    interactive_mg_runner.start(config, instance_name)
    runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

    wrapper = MemgraphWrapper(runner, port)
    yield wrapper

    interactive_mg_runner.stop(config, instance_name, keep_directories=False)


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


class MemgraphInstanceFactory:
    """
    Factory for creating Memgraph instances with custom configurations.

    Use this when you need instances with specific settings like memory limits.
    Instances are automatically cleaned up when the factory goes out of scope.
    """

    def __init__(self, num_workers: int = 8):
        self.num_workers = num_workers
        self.instances = {}
        self._instance_counter = 0

        # Configure runner paths
        build_root = E2E_ROOT.parent.parent
        interactive_mg_runner.MEMGRAPH_BINARY = str((build_root / "memgraph").resolve())
        interactive_mg_runner.BUILD_DIR = str(build_root.resolve())

    def create_instance(
        self,
        memory_limit_mb: Optional[int] = None,
        port: int = 7688,
        extra_args: Optional[list] = None,
    ) -> MemgraphWrapper:
        """
        Create a new Memgraph instance with the specified configuration.

        Args:
            memory_limit_mb: Global memory limit in MB (--memory-limit flag)
            port: Bolt port for this instance
            extra_args: Additional command line arguments

        Returns:
            MemgraphWrapper for the new instance
        """
        self._instance_counter += 1
        instance_name = f"custom_instance_{self._instance_counter}"

        args = [
            f"--bolt-port={port}",
            "--log-level=WARNING",
            "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=300",
            "--storage-properties-on-edges=true",
            "--telemetry-enabled=false",
            f"--bolt-num-workers={self.num_workers}",
        ]

        if memory_limit_mb is not None:
            args.append(f"--memory-limit={memory_limit_mb}")

        if extra_args:
            args.extend(extra_args)

        config = {
            instance_name: {
                "args": args,
                "log_file": f"{instance_name}.log",
                "data_directory": f"{instance_name}_data",
            }
        }

        interactive_mg_runner.start(config, instance_name)
        runner = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]

        wrapper = MemgraphWrapper(runner, port)
        self.instances[instance_name] = (config, wrapper)

        return wrapper

    def stop_instance(self, wrapper: MemgraphWrapper) -> None:
        """Stop a specific instance."""
        for name, (config, w) in list(self.instances.items()):
            if w is wrapper:
                interactive_mg_runner.stop(config, name, keep_directories=False)
                del self.instances[name]
                break

    def stop_all(self) -> None:
        """Stop all instances created by this factory."""
        for name, (config, wrapper) in list(self.instances.items()):
            try:
                interactive_mg_runner.stop(config, name, keep_directories=False)
            except Exception:
                pass
        self.instances.clear()


@pytest.fixture
def memgraph_factory(num_workers: int) -> Generator[MemgraphInstanceFactory, None, None]:
    """
    Fixture providing a factory for creating custom Memgraph instances.

    Use this when you need instances with specific configurations like memory limits.
    All instances are automatically cleaned up after the test.

    Example:
        def test_with_memory_limit(memgraph_factory):
            mg = memgraph_factory.create_instance(memory_limit_mb=100, port=7688)
            # ... test with memory-limited instance ...
    """
    factory = MemgraphInstanceFactory(num_workers)
    yield factory
    factory.stop_all()
