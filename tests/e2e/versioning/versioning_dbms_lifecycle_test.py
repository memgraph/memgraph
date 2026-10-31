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

# DBMS lifecycle (RENAME, SUSPEND/RESUME, DROP ... FORCE) of a database whose branches have connected sessions. Needs
# --experimental-enabled=versioning, an enterprise license and a data directory the test can read the branch
# directories from (the server's own, taken from SHOW CONFIG).

import sys
import time
from pathlib import Path

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

URI = "bolt://localhost:7687"
NO_AUTH = ("", "")


def _open_driver():
    # One pooled connection, so the session below keeps the same server-side Bolt session.
    return GraphDatabase.driver(URI, auth=NO_AUTH, max_connection_pool_size=1)


def _use_branch(session, database: str) -> None:
    # USE BRANCH is refused on a session that names its database in the Bolt metadata, so switch with USE DATABASE.
    session.run(f"USE DATABASE {database}").consume()
    session.run("USE BRANCH b").consume()


@pytest.fixture
def admin():
    with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database="memgraph") as s:
        yield s
        for row in s.run("SHOW DATABASES").data():
            if row["Name"].startswith("lc_") and row["State"] != "DROPPING":
                if row["State"] == "COLD":
                    s.run(f"RESUME DATABASE {row['Name']}").consume()
                s.run(f"DROP DATABASE {row['Name']} FORCE").consume()


def _create_db_with_branch(admin, name: str) -> None:
    admin.run(f"CREATE DATABASE {name}").consume()
    with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database=name) as s:
        s.run("CREATE (:V {id: 1})").consume()
        s.run("CREATE BRANCH b").consume()


def _count(session, label: str) -> int:
    return session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]


def _branch_names(session) -> list[str]:
    return sorted(row["name"] for row in session.run("SHOW BRANCHES"))


def _branch_directories(admin) -> set[Path]:
    # The runner gives each worker its own data directory, so ask the server where it is.
    data_directory = next(r["current_value"] for r in admin.run("SHOW CONFIG") if r["name"] == "data_directory")
    return set(Path(data_directory).glob("databases/*/branches/*"))


def _wait_until_gone(directories: set[Path], timeout_s: float = 30.0) -> None:
    # A FORCE drop removes the database's files asynchronously, once the last session has let go.
    deadline = time.monotonic() + timeout_s
    while any(d.exists() for d in directories) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not any(d.exists() for d in directories)


def test_rename_keeps_branch_sessions_working(admin):
    _create_db_with_branch(admin, "lc_old")
    with _open_driver() as using, using.session() as u:
        _use_branch(u, "lc_old")
        u.run("CREATE (:OnBranch)").consume()

        admin.run("RENAME DATABASE lc_old TO lc_new").consume()

        assert _count(u, "OnBranch") == 1
        assert _count(u, "V") == 1
        with GraphDatabase.driver(URI, auth=NO_AUTH) as drv:
            with drv.session(database="lc_new") as s:
                assert _branch_names(s) == ["b", "main"]
            with drv.session(database="lc_new@b") as s:
                assert _count(s, "OnBranch") == 1
            with drv.session(database="lc_old@b") as s:
                with pytest.raises(ClientError, match='Tried to retrieve an unknown database "lc_old".'):
                    s.run("RETURN 1").consume()


def test_suspend_is_refused_with_a_branch_session_and_resume_restores_branches(admin):
    _create_db_with_branch(admin, "lc_susp")
    with _open_driver() as drv, drv.session() as u:
        _use_branch(u, "lc_susp")
        u.run("CREATE (:OnBranch)").consume()
        with pytest.raises(ClientError, match="Database lc_susp has active connections; cannot suspend while in use."):
            admin.run("SUSPEND DATABASE lc_susp").consume()
        assert _count(u, "OnBranch") == 1

    admin.run("SUSPEND DATABASE lc_susp").consume()
    admin.run("RESUME DATABASE lc_susp").consume()
    with _open_driver() as drv, drv.session() as s:
        _use_branch(s, "lc_susp")
        assert _branch_names(s) == ["b", "main"]
        assert _count(s, "OnBranch") == 1
        assert _count(s, "V") == 1


def test_drop_force_evicts_a_branch_session_and_removes_its_branches(admin):
    other_branch_directories = _branch_directories(admin)
    _create_db_with_branch(admin, "lc_drop")
    with _open_driver() as drv, drv.session() as u:
        _use_branch(u, "lc_drop")
        u.run("CREATE (:OnBranch)").consume()
        # USE BRANCH creates the branch's directory.
        branch_directories = _branch_directories(admin) - other_branch_directories
        assert len(branch_directories) == 1

        admin.run("DROP DATABASE lc_drop FORCE").consume()

        with pytest.raises(ClientError, match="Database required for the transaction setup."):
            u.run("MATCH (n:OnBranch) RETURN count(n)").consume()

    # Branches are keyed by the database uuid, so a re-created database of the same name starts with none.
    admin.run("CREATE DATABASE lc_drop").consume()
    with GraphDatabase.driver(URI, auth=NO_AUTH) as drv, drv.session(database="lc_drop") as s:
        assert _branch_names(s) == ["main"]
    _wait_until_gone(branch_directories)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
