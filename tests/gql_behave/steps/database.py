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

# CHECKOUT BRANCH is per-Bolt-connection state, but the default path opens a fresh session per
# query, so under --versioned-branch one session is held open for the whole scenario instead.


def get_vbranch_session(context):
    """Persistent per-scenario session for --versioned-branch, opened lazily on first use."""
    if getattr(context, "vbranch_session", None) is None:
        context.vbranch_session = context.driver.session()
    return context.vbranch_session


def close_vbranch_session(context):
    """Best-effort cleanup: checkout main, drop the branch, close the session. Safe if never opened."""
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
    Runs once per scenario before the test query: setup already committed to `main` becomes the
    branch's inherited state, and the test query's writes land in the branch overlay only.
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
        # CHECKOUT state lives on the connection; closed once in after_scenario, not per query.
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
