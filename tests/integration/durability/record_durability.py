#!/usr/bin/env python3
import argparse
import glob
import logging
import os
import shutil
import signal
import subprocess
import tempfile
from pathlib import Path

from memgraph_server_context import memgraph_server

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Get the absolute path to the directory containing this script
SCRIPT_DIR = Path(__file__).resolve().parent

# Build default paths relative to this script's location
DEFAULT_MEMGRAPH_PATH = SCRIPT_DIR / "../../../build/memgraph"


def find_latest_file(glob_pattern):
    """Find the latest file matching a glob pattern based on lexicographic order."""
    files = glob.glob(glob_pattern)
    return max(files, default=None)


def collect_snapshot_and_wal(test_dir, memgraph, mgconsole, port, force):
    cypher_file = Path(test_dir) / "create_dataset.cypher"
    snapshot_path = Path(test_dir) / "snapshot.bin"
    wal_path = Path(test_dir) / "wal.bin"

    # Skip if all required files are already present
    if not force and all(p.exists() for p in [snapshot_path, wal_path]):
        logger.info("Files already generated. Skipping.")
        return

    with tempfile.TemporaryDirectory(prefix="memgraph_data_", dir="/tmp") as data_dir:
        data_dir = Path(data_dir)
        logger.info(f"Using temporary data directory: {data_dir}")

        extra_args = [
            "--storage-snapshot-on-exit=true",
            "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=1000",
        ]

        with memgraph_server(memgraph, data_dir, port, logger, extra_args), open(cypher_file, "rb") as input_cypher:
            try:
                logger.info(f"Piping {cypher_file} into mgconsole...")
                subprocess.run(
                    [mgconsole],
                    stdin=input_cypher,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    f"mgconsole failed with return code {e.returncode}\n\nStandard output: {e.stdout}\n\nStandard error: {e.stderr}"
                )

        # After shutdown: copy latest snapshot
        if force or not snapshot_path.exists():
            latest_snapshot = find_latest_file(str(data_dir / "snapshots" / "*_timestamp_*"))
            if latest_snapshot:
                logger.info(f"Copying snapshot: {latest_snapshot} → {snapshot_path}")
                shutil.copy(latest_snapshot, snapshot_path)
            else:
                logger.warning("No snapshot file found.")
        else:
            logger.info("Snapshot already generated. Skipping.")

        # After shutdown: copy latest WAL
        if force or not wal_path.exists():
            latest_wal = find_latest_file(str(data_dir / "wal" / "*_from_*_to_*"))
            if latest_wal:
                logger.info(f"Copying WAL: {latest_wal} → {wal_path}")
                shutil.copy(latest_wal, wal_path)
            else:
                logger.warning("No WAL file found.")
        else:
            logger.info("WAL already generated. Skipping.")

        logger.info("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Run Memgraph, send Cypher input via mgconsole, and extract WAL/snapshot."
    )
    parser.add_argument("--memgraph", default=DEFAULT_MEMGRAPH_PATH, help="Path to the Memgraph binary.")
    parser.add_argument("--mgconsole", default="mgconsole", help="Path to mgconsole binary.")
    parser.add_argument("--port", type=int, default=7687, help="Port to wait for Memgraph to be ready on.")
    parser.add_argument("--tests_root", default="tests", help="Root for all the test data.")
    parser.add_argument("--force", action="store_true", help="Force creation of snapshot and wal data.")

    args = parser.parse_args()

    for tool in [args.memgraph, args.mgconsole]:
        if not shutil.which(str(tool)):
            logger.error(f"Required tool not found in PATH or not executable: {tool}")
            return

    pattern = str(Path(args.tests_root) / "*" / "*" / "create_dataset.cypher")
    cypher_files = glob.glob(pattern)
    if not cypher_files:
        logger.error("No create_dataset.cypher files found.")
        return

    for cypher_file in cypher_files:
        test_dir = os.path.dirname(cypher_file)
        logger.info(f"=== Running for: {cypher_file} ===")
        collect_snapshot_and_wal(test_dir, args.memgraph, args.mgconsole, args.port, args.force)


if __name__ == "__main__":
    main()
