# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# -*- coding: utf-8 -*-

import re

# --- Graph Versioning v1 --versioned-branch arm -----------------------------
#
# `CHECKOUT BRANCH` is per-Bolt-connection state on the server. The default
# path below opens a fresh session (and thus, in practice, a fresh physical
# connection) for every single query, so a checkout would never survive past
# the query that issued it. Under --versioned-branch we instead hold one
# session open for the whole scenario on `context.vbranch_session` (backed by
# the shared `context.driver`), so a CHECKOUT done during scenario setup is
# still in effect for the scenario's test query. See environment.py
# before_scenario/after_scenario for the per-scenario lifecycle and
# maybe_fork_to_branch() below for the fork point.
#
# close_vbranch_session() checks out back to `main` before dropping the
# branch and closing the session: `CHECKOUT BRANCH main` un-engages the
# connection's versioning state (fixed in ddf60d59f), so once the session
# closes and its connection is returned to the shared pool, the next
# scenario that reuses it starts on main, un-engaged and main-writable.


def get_vbranch_session(context):
    """
    Return the persistent Bolt session used by the --versioned-branch arm,
    opening it lazily on first use (so a scenario that errors out before
    executing any query never pays for a connection). Backed by the shared
    `context.driver` -- see the module docstring above for why a session
    held open for the whole scenario is still needed.
    """
    if getattr(context, "vbranch_session", None) is None:
        context.vbranch_session = context.driver.session()
    return context.vbranch_session


def close_vbranch_session(context):
    """
    Best-effort teardown of the persistent versioned-branch session: if the
    scenario forked onto a branch, checkout back to main (un-engaging the
    connection's versioning state, see ddf60d59f) and drop the branch (both
    swallowed on error -- this is cleanup, not the scenario under test), then
    close the session so its connection goes back to the shared pool clean
    for the next scenario. Safe to call even if no session was ever opened.
    """
    session = getattr(context, "vbranch_session", None)
    if session is None:
        return

    if getattr(context, "vbranch_forked", False):
        try:
            list(session.run("CHECKOUT BRANCH main"))
        except Exception:
            pass
        try:
            list(session.run(f"DROP BRANCH {context.vbranch_name}"))
        except Exception:
            pass

    session.close()
    context.vbranch_session = None
    context.vbranch_forked = False


def maybe_fork_to_branch(context):
    """
    The "partly versioned" fork point. Called from the `When executing
    query:` step, BEFORE the test query runs, and at most once per scenario.
    Everything before this point -- clear_graph's STORAGE MODE/DROP GRAPH
    admin ops, `Given graph "x"` setup, and `And having executed:` setup --
    has already committed to `main` on the persistent session (no checkout
    has happened yet), so it becomes the branch's inherited fork-state. The
    test query then runs on a fresh branch: reads of the setup data come
    through the branch/main union, and any writes land in the branch overlay
    only.
    """
    if not getattr(context.config, "versioned_branch", False):
        return
    if getattr(context, "vbranch_forked", False):
        return

    session = get_vbranch_session(context)
    branch_name = context.vbranch_name
    list(session.run(f"CREATE BRANCH {branch_name} FROM main"))
    list(session.run(f"CHECKOUT BRANCH {branch_name}"))
    context.vbranch_forked = True


# -----------------------------------------------------------------------------


def query(q, context, params={}):
    """
    Function used to execute query on database. Query results are
    set in context.result_list. If exception occurs, it is set on
    context.exception.

    @param q:
        String, database query.
    @param context:
        behave.runner.Context, context of all tests.
    @return:
        List of query results.
    """
    results_list = []

    parallel_execution = getattr(context.config, "parallel_execution", False)
    versioned_branch = getattr(context.config, "versioned_branch", False)
    storage_mode = getattr(context.config, "storage_mode", None)

    is_on_disk = storage_mode == "ON_DISK_TRANSACTIONAL"

    # Add USING PARALLEL EXECUTION to data queries (those with RETURN) when flag is set
    # and storage mode is not ON_DISK_TRANSACTIONAL.
    # Per the grammar, the directive lives inside cypherQuery, which EXPLAIN/PROFILE wrap
    # (explainQuery: EXPLAIN cypherQuery), so for those it must go *after* the keyword, e.g.
    # "EXPLAIN USING PARALLEL EXECUTION RETURN 1" -- not before, which is a parse error.
    if parallel_execution and not is_on_disk:
        if "RETURN" in q.upper():
            # Split off a leading EXPLAIN/PROFILE keyword (the directive goes after it).
            match = re.match(r"(?is)^(\s*(?:EXPLAIN|PROFILE)\s+)(.*)$", q)
            prefix, body = (match.group(1), match.group(2)) if match else ("", q)
            if body.strip().upper().startswith("USING"):
                body = re.sub(r"(?i)^(\s*USING\s+)", r"\1PARALLEL EXECUTION, ", body)
            else:
                body = "USING PARALLEL EXECUTION " + body
            q = prefix + body

    # Store the actual query being executed (for logging and validation purposes)
    context.last_executed_query = q

    if versioned_branch:
        # Reuse the scenario's persistent session instead of opening+closing
        # one per call -- CHECKOUT BRANCH state lives on the physical
        # connection and would not survive a close() here. See
        # get_vbranch_session()/close_vbranch_session() above; the session is
        # closed once, in after_scenario (environment.py), not per query.
        session = get_vbranch_session(context)
        try:
            results = session.run(q, params)
            results_list = list(results)
        except Exception as e:
            context.exception = e
            context.log.info("%s", str(e))
        return results_list

    session = context.driver.session()
    try:
        # executing query
        results = session.run(q, params)
        results_list = list(results)
        """
        This code snippet should replace code which is now
        executing queries when session.transactions will be supported.

        with session.begin_transaction() as tx:
            results = tx.run(q, params)
            summary = results.summary()
            results_list = list(results)
            tx.success = True
        """
    except Exception as e:
        # exception
        context.exception = e
        context.log.info("%s", str(e))
    finally:
        session.close()

    return results_list
