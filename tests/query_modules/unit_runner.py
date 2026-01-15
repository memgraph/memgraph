"""
Unit test runner for query modules.
"""

import os
import sys

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
MAGE_PYTHON_PATH = os.path.join(PROJECT_ROOT, "mage", "python")
if MAGE_PYTHON_PATH not in sys.path:
    sys.path.insert(0, MAGE_PYTHON_PATH)

QUERY_MODULES_PATH = os.path.join(PROJECT_ROOT, "query_modules")
if QUERY_MODULES_PATH not in sys.path:
    sys.path.insert(0, QUERY_MODULES_PATH)

# modules shared between memgraph and mage that have unit tests
shared_modules = [
    "date",
]

if __name__ == "__main__":
    import glob
    import subprocess

    script_dir = os.path.dirname(os.path.abspath(__file__))
    unit_test_files = glob.glob(os.path.join(script_dir, "*/unit/test_*.py"))
    for module in shared_modules:
        unit_test_files.extend(glob.glob(os.path.join(MAGE_PYTHON_PATH, "tests", module, "test_*.py")))

    if not unit_test_files:
        print("No unit test files found!")
        sys.exit(1)

    cmd = ["python3", "-m", "pytest"] + unit_test_files + ["-v"]

    result = subprocess.run(cmd, cwd=script_dir)
    sys.exit(result.returncode)
