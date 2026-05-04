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

"""Regression tests for label+property index cleanup and RSS reclamation.

The symptoms being guarded against:

1. After `SET` overwrites a property on an indexed vertex, the old entry in the
   label+property skiplist must be reclaimed. In IN_MEMORY_ANALYTICAL this
   previously never happened because the skiplist-cleanup pass in
   `CollectGarbage` was gated on vertex *deletions* — a pure property update
   never tripped the flag, so `FREE MEMORY` left the stale entry behind and
   `SHOW INDEX INFO` reported `2 * node_count`. Fixed by eagerly removing the
   old skiplist entry in `InMemoryLabelPropertyIndex::UpdateOnSetProperty`
   under analytical mode.

2. After bulk data churn (SET over N nodes, then DETACH DELETE), RSS retained
   by jemalloc used to stay hundreds of MiB (multi-GiB at 10M nodes) above
   baseline even after multiple `FREE MEMORY` calls. Root cause was an
   off-by-one in `SkipListGc::Collect` / `Run`: nodes were tagged with the
   next-to-allocate accessor id instead of the newest-alive id, so the latest
   batch of removed nodes needed two extra empty accessor cycles before the
   GC could free them. Fixed in `src/utils/skip_list.hpp` — Collect now tags
   with `counter - 1` and Run tracks `live_horizon` as the exclusive upper
   bound of the dead prefix.
"""

import os
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

BOLT_PORT = 7687
# 250k gives a ~100 MiB pre-fix retention gap on the RSS test (the symptom
# scaled linearly at ~460 MiB per 1M nodes), enough signal above jemalloc
# noise and the 32 MiB tolerance.
NODE_COUNT = 250_000
LONG_STRING = "abcdefghijklomnpqrstuvwxyz"

INSTANCE = {
    "index_cleanup_memory_test": {
        "args": [
            "--bolt-port",
            str(BOLT_PORT),
            "--storage-gc-cycle-sec=180",
            "--log-level=WARNING",
        ],
        "log_file": "index-cleanup-memory-e2e.log",
        "setup_queries": [],
        "validation_queries": [],
    }
}


@pytest.fixture(autouse=True)
def cleanup_after_test():
    yield
    interactive_mg_runner.kill_all(keep_directories=False)


def _start_and_connect():
    interactive_mg_runner.start_all(INSTANCE)
    return connect(host="localhost", port=BOLT_PORT).cursor()


def _find_label_property_row(index_info, label, props):
    return [row for row in index_info if row[0] == "label+property" and row[1] == label and row[2] == props]


def _seed_indexed_nodes(cursor, *, analytical: bool = False, with_label_index: bool = True):
    """Create NODE_COUNT :Node vertices with a string property, plus indices."""
    if analytical:
        execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")
    execute_and_fetch_all(cursor, f'UNWIND range(1, {NODE_COUNT}) AS x CREATE (:Node {{str: "{LONG_STRING}"}});')
    if with_label_index:
        execute_and_fetch_all(cursor, "CREATE INDEX ON :Node;")
    execute_and_fetch_all(cursor, "CREATE INDEX ON :Node(str);")


def _assert_node_str_index_count(cursor, expected):
    rows = execute_and_fetch_all(cursor, "SHOW INDEX INFO;")
    matches = _find_label_property_row(rows, "Node", ["str"])
    assert len(matches) == 1, f"Expected exactly one :Node(str) index row, got {rows}"
    assert matches[0][3] == expected, f"Expected {expected} entries in :Node(str) index, got {matches[0][3]}."


def _parse_mib(value: str) -> float:
    value = value.strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return 0.0


def _rss_mib(cursor) -> float:
    rows = execute_and_fetch_all(cursor, "SHOW STORAGE INFO")
    info = {row[0]: row[1] for row in rows}
    return _parse_mib(info["memory_res"])


def test_analytical_label_property_index_cleanup_on_set():
    """After SET overwrites an indexed string property in analytical mode, the
    old skiplist entry must be removed eagerly. Without the fix, the skiplist
    holds 2 * NODE_COUNT entries.

    No FREE MEMORY: the analytical erase calls `skiplist.remove`, which
    decrements `size_` synchronously, so `SHOW INDEX INFO` reflects it
    without a GC pass.
    """
    cursor = _start_and_connect()
    _seed_indexed_nodes(cursor, analytical=True)
    execute_and_fetch_all(cursor, "MATCH (n) SET n.str = 1;")

    _assert_node_str_index_count(cursor, NODE_COUNT)


def test_transactional_label_property_index_cleanup_on_set():
    """Same scenario as the analytical case, but in transactional mode. MVCC
    GC already reclaims stale entries after oldest_active_start_timestamp
    advances past them, so FREE MEMORY should bring the index count back to
    NODE_COUNT. This guards against a regression in the transactional path.
    """
    cursor = _start_and_connect()
    _seed_indexed_nodes(cursor)
    execute_and_fetch_all(cursor, "MATCH (n) SET n.str = 1;")
    # Two calls: the first drives CollectGarbage, the second runs once oldest
    # active has advanced, allowing RemoveObsoleteEntries to reclaim entries.
    execute_and_fetch_all(cursor, "FREE MEMORY;")
    execute_and_fetch_all(cursor, "FREE MEMORY;")

    _assert_node_str_index_count(cursor, NODE_COUNT)


def test_analytical_label_property_index_cleanup_on_remove_label():
    """In analytical mode, REMOVE :Node must immediately drop the vertex's
    label+property index entry — no MVCC reader could observe it. Without the
    fix, the entry persists since `CollectGarbage`'s skiplist sweep is gated
    on vertex deletions, and label removal alone never trips it.

    No FREE MEMORY: the analytical erase calls `skiplist.remove`, which
    decrements `size_` synchronously, so `SHOW INDEX INFO` reflects it
    without a GC pass.
    """
    cursor = _start_and_connect()
    _seed_indexed_nodes(cursor, analytical=True, with_label_index=False)
    execute_and_fetch_all(cursor, "MATCH (n:Node) REMOVE n:Node;")

    _assert_node_str_index_count(cursor, 0)


def test_analytical_label_property_index_cleanup_on_delete():
    """After DETACH DELETE in analytical mode, the label+property index must
    be empty. This path *does* set gc_full_scan_vertices_delete_, so it should
    already work today — the test guards against regression.

    One FREE MEMORY is enough: the analytical delete sets the flag,
    `CollectGarbage` calls `RemoveObsoleteVertexEntries`, and that sweep
    invokes `acc.remove(...)` which decrements `skiplist.size_` synchronously,
    so SHOW INDEX INFO reflects it without a second pass.
    """
    cursor = _start_and_connect()
    _seed_indexed_nodes(cursor, analytical=True, with_label_index=False)
    node_count_after_create = execute_and_fetch_all(cursor, "MATCH (n) RETURN count(n);")[0][0]
    assert node_count_after_create == NODE_COUNT, f"Expected {NODE_COUNT} nodes created, got {node_count_after_create}"
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "FREE MEMORY;")

    _assert_node_str_index_count(cursor, 0)


def test_transactional_free_memory_returns_rss_to_baseline():
    """After SET+DETACH DELETE churn, a single `FREE MEMORY` must bring RSS
    back close to the empty-storage baseline — on par with what `DROP GRAPH`
    achieves.

    Prior to the SkipListGc off-by-one fix, the most-recently-removed skiplist
    nodes were stamped with `accessor_id_.load()` (the next-to-allocate id)
    but the GC erased only when `tag < last_dead`. That required two extra
    accessor cycles before the latest batch could be freed, so single `FREE
    MEMORY` left hundreds of MiB (or multi-GiB at 10M nodes) pinned.

    With the fix, one `FREE MEMORY` reclaims ~the full churn; with a tolerance
    that catches any regression reintroducing the lag.
    """
    cursor = _start_and_connect()

    # Burn in to reach steady state before measuring baseline.
    execute_and_fetch_all(cursor, "FREE MEMORY;")
    baseline_mib = _rss_mib(cursor)

    _seed_indexed_nodes(cursor)
    execute_and_fetch_all(cursor, "MATCH (n) SET n.str = 1;")
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
    execute_and_fetch_all(cursor, "FREE MEMORY;")
    after_free_mib = _rss_mib(cursor)

    execute_and_fetch_all(cursor, "STORAGE MODE IN_MEMORY_ANALYTICAL;")
    execute_and_fetch_all(cursor, "DROP GRAPH;")
    execute_and_fetch_all(cursor, "FREE MEMORY;")
    after_drop_mib = _rss_mib(cursor)

    # DROP GRAPH must return RSS close to baseline. Generous ceiling — this
    # is the floor we can't do better than.
    assert after_drop_mib < max(200.0, baseline_mib * 4.0), (
        f"DROP GRAPH did not return RSS near baseline: baseline={baseline_mib:.1f} MiB, "
        f"after_drop={after_drop_mib:.1f} MiB."
    )
    # FREE MEMORY must get within 32 MiB of DROP GRAPH. Pre-fix the gap at
    # NODE_COUNT=250k is ~100 MiB (linear ~460 MiB at 1M), well above the
    # tolerance, so any regression fails loudly.
    assert after_free_mib - after_drop_mib < 32.0, (
        f"FREE MEMORY retained significantly more than DROP GRAPH — the SkipListGc "
        f"tag/horizon fix may have regressed. baseline={baseline_mib:.1f} MiB "
        f"after_free={after_free_mib:.1f} MiB after_drop={after_drop_mib:.1f} MiB."
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
