# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os
import typing

import mgclient


def get_data_path(file: str, test: str):
    """
    Data is stored in durability folder.
    """
    return f"durability/{file}/{test}"


def get_logs_path(file: str, test: str):
    """
    Logs are stored in durability folder.
    """
    return f"durability/{file}/{test}"


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(**kwargs)
    connection.autocommit = True
    return connection


def corrupt_snapshots(full_data_directory):
    """Corrupt the data (vertex/edge/index) region of every snapshot while preserving the
    offsets block at the start and the metadata section at the end. This keeps the file
    readable by ReadSnapshotInfo (so it is selected for recovery) but makes LoadSnapshot
    fail, which is the "no usable snapshot" path that yields a broken database."""
    snapshot_dir = os.path.join(full_data_directory, "snapshots")
    files = [
        os.path.join(snapshot_dir, f) for f in os.listdir(snapshot_dir) if os.path.isfile(os.path.join(snapshot_dir, f))
    ]
    assert files, "Expected at least one snapshot to corrupt"
    head_keep = 1024  # magic + version + SECTION_OFFSETS
    tail_keep = 4096  # SECTION_METADATA (uuid/epoch/counts) lives near the end
    for path in files:
        size = os.path.getsize(path)
        start = min(head_keep, size)
        end = max(start, size - tail_keep)
        assert end > start, f"Snapshot {path} too small ({size} bytes) to corrupt safely"
        with open(path, "r+b") as fh:
            fh.seek(start)
            fh.write(b"\xff" * (end - start))
    return files
