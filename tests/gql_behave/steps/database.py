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
