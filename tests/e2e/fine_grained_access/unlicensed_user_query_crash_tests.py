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

# Regression test for a SIGSEGV in SessionHL::Pull (src/glue/SessionHL.cpp).
#
# Root cause: SessionHL::Pull builds a fine-grained auth checker whenever an
# authenticated user is present, but — unlike the three call sites in
# interpreter.cpp — did NOT first check that the enterprise license is valid.
# AuthChecker::GetFineGrainedAuthChecker returns a null unique_ptr as soon as the
# license is invalid, and the following `auth_checker->NeedsFineGrainedAuthChecker()`
# dereferenced null, crashing the whole server. The DMG_ASSERT guarding it is a
# no-op in release/RelWithDebInfo builds, so it never caught this.
#
# The crash needs three things, all present here:
#   * an enterprise binary (MG_ENTERPRISE) running WITHOUT a valid license,
#   * an authenticated user (QueryUserOrRole is truthy iff a username is set --
#     a role is NOT required; QA hit it via a role, but that was incidental),
#   * a data query, which sets up the execution db accessor.
# `CREATE USER` is not license-gated (unlicensed it just grants all privileges),
# so no license/role dance is needed to reproduce.
#
# Why this test manages its own instance instead of using a `cluster:` workload:
# CI feeds the license through the MEMGRAPH_ENTERPRISE_LICENSE /
# MEMGRAPH_ORGANIZATION_NAME env vars, and the license resolver picks the
# furthest-expiry valid candidate across DB-setting / ENV / CLI. So a valid ENV
# license cannot be turned off at runtime with `SET DATABASE SETTING
# 'enterprise.license' TO ''` (the ENV candidate still wins), and a
# runner-managed instance always inherits that ENV license. We therefore pop the
# license env vars *before* starting Memgraph: subprocess.Popen (no env=) hands
# the child our current os.environ, so the instance boots unlicensed.

import os
import sys

import interactive_mg_runner
import mgclient
import pytest

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

INSTANCE = "unlicensed"
LICENSE_ENV_VARS = ("MEMGRAPH_ENTERPRISE_LICENSE", "MEMGRAPH_ORGANIZATION_NAME")

MEMGRAPH_INSTANCE = {
    INSTANCE: {
        "args": [
            "--bolt-port=7687",
            "--log-level=TRACE",
            "--also-log-to-stderr",
            "--data-recovery-on-startup=false",
        ],
        "log_file": "unlicensed_user_query_crash.log",
    }
}


def connect(**kwargs):
    connection = mgclient.connect(host="localhost", port=7687, **kwargs)
    connection.autocommit = True
    return connection


def execute_and_fetch_all(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


@pytest.fixture
def unlicensed_instance():
    # Strip any license the CI/env provides so the instance boots unlicensed;
    # the child process inherits this (mutated) os.environ via Popen.
    saved = {var: os.environ.pop(var, None) for var in LICENSE_ENV_VARS}
    try:
        interactive_mg_runner.kill_all()
        interactive_mg_runner.start(MEMGRAPH_INSTANCE, INSTANCE)
        yield
    finally:
        interactive_mg_runner.kill_all()
        for var, value in saved.items():
            if value is not None:
                os.environ[var] = value


def test_authenticated_user_data_query_without_license_does_not_crash(unlicensed_instance):
    # Before any user exists the connection is unauthenticated and privileged;
    # use it to create data and a user. Pre-fix, the crash is triggered only once
    # we run a data query on an *authenticated* connection (below).
    admin = connect()
    admin_cursor = admin.cursor()
    execute_and_fetch_all(admin_cursor, "CREATE (:Person {name: 'x'});")
    # CREATE USER is not enterprise-gated; unlicensed it grants all privileges.
    execute_and_fetch_all(admin_cursor, "CREATE USER alice IDENTIFIED BY 'alice123';")

    # Reconnect as alice and run a data query. Pre-fix: SessionHL::Pull calls
    # GetFineGrainedAuthChecker (null, because unlicensed) and dereferences it ->
    # SIGSEGV takes down the whole server. Post-fix: the license guard skips that
    # block, no fine-grained filtering is applied, and the query returns cleanly.
    alice = connect(username="alice", password="alice123")
    alice_cursor = alice.cursor()
    result = execute_and_fetch_all(alice_cursor, "MATCH (n) RETURN count(n);")
    assert result == [(1,)]

    # The server must still be alive and serving -- a follow-up query proves the
    # process did not crash and the connection layer is intact.
    assert execute_and_fetch_all(alice_cursor, "RETURN 1;") == [(1,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
