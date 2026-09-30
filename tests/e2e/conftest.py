# Copyright 2026 Memgraph Ltd.
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
Root conftest.py for E2E tests.

This module sets up the Python path and configures interactive_mg_runner
to work correctly whether tests are run from:
  - Source directory: tests/e2e/
  - Build directory: build/tests/e2e/
"""

import os
import sys
from pathlib import Path

import pytest

# Add tests/e2e to Python path for imports (common, mg_utils, etc.)
E2E_DIR = Path(__file__).parent.resolve()
if str(E2E_DIR) not in sys.path:
    sys.path.insert(0, str(E2E_DIR))


def _find_project_root(start_path: Path) -> Path | None:
    """Find the memgraph project root by looking for CMakeLists.txt and src/."""
    # First, try to find by marker files
    current = start_path
    for _ in range(10):  # Limit search depth
        if (current / "CMakeLists.txt").exists() and (current / "src").is_dir():
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent

    # Fallback: assume we're in tests/e2e or build/tests/e2e
    if start_path.name == "e2e" and start_path.parent.name == "tests":
        tests_parent = start_path.parent.parent
        if tests_parent.name == "build":
            # build/tests/e2e -> go up 3 levels
            return tests_parent.parent
        else:
            # tests/e2e -> go up 2 levels
            return tests_parent

    return None


def _find_build_dir(project_root: Path) -> Path:
    """Find the build directory."""
    # Standard build location
    build_dir = project_root / "build"
    if (build_dir / "memgraph").exists():
        return build_dir
    # Maybe we're already in the build tree
    if (project_root / "memgraph").exists():
        return project_root
    return build_dir


def _setup_interactive_mg_runner():
    """Configure interactive_mg_runner with correct paths if current config is invalid."""
    try:
        import interactive_mg_runner
    except ImportError:
        return  # Module not available, skip setup

    # Check if current MEMGRAPH_BINARY path is valid
    current_binary = getattr(interactive_mg_runner, "MEMGRAPH_BINARY", "")
    if os.path.exists(current_binary):
        return  # Current config is valid, don't override

    # Find correct paths
    project_root = _find_project_root(E2E_DIR)
    if project_root is None:
        return  # Could not find project root

    build_dir = _find_build_dir(project_root)
    memgraph_binary = build_dir / "memgraph"

    if memgraph_binary.exists():
        interactive_mg_runner.PROJECT_DIR = str(project_root)
        interactive_mg_runner.BUILD_DIR = str(build_dir)
        interactive_mg_runner.MEMGRAPH_BINARY = str(memgraph_binary)


@pytest.fixture(scope="session", autouse=True)
def configure_mg_runner_paths():
    """
    Session-scoped fixture that ensures interactive_mg_runner paths are correct.

    This runs after all test modules are imported but before any tests execute,
    allowing it to fix paths that were incorrectly set by test module imports.
    """
    _setup_interactive_mg_runner()
    yield
