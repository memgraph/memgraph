// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// PARITY HARNESS — synchronous drive vs coroutine root-drive (coroutine cursors v2).
//
// Each cursor keeps its synchronous body (Pull(), unchanged from master); a converted cursor adds a
// coroutine body (DoPull, via MG_COROUTINE_CURSOR_PULLCO). This harness runs a corpus of queries
// SYNCHRONOUSLY and then with the coroutine root-drive forced ON (SetForceCoroRootDriveForTesting —
// PullPlan drives the plan via PullCo + ResumePullStep) and asserts the rendered results are IDENTICAL.
// Synchronous drive is byte-identical to master, so this is a mechanical proof that coroutine pull ==
// the current pull.
//
// The corpus GROWS as cursor families are converted: a query exercises the coroutine path only for the
// cursors that have a DoPull (unconverted cursors take the frame-less Immediate(Pull()) path, so they
// run synchronously even under the forced coroutine root-drive). Until the first cursor is converted,
// coroutine-drive == sync drive for every query — so the corpus also serves as a broad
// operator-surface robustness gate for the driver itself. Each cursor PR should add queries that route
// through its cursors. Keep corpus queries DETERMINISTIC (fixed seed data + stable ordering) so the two
// renders compare equal without order-normalization.
//
// NOTE: the force-coro-root hook is a throwaway test seam; it is replaced by the real per-cursor mode
// selection (MakeCursor) in a later PR, at which point this harness flips to driving via that selection.
// Parallel (enterprise) cursors are converted with the scheduler/park work in a later PR.

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "query/interpreter_context.hpp"
#include "query/plan/cursor_awaitable.hpp"
#include "replication/state.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "system/system.hpp"

namespace {

// Render a result stream (header + rows) to a stable string for equality comparison.
std::string Render(const ResultStreamFaker &stream) {
  std::ostringstream os;
  os << "HEADER:";
  for (const auto &col : stream.GetHeader()) os << ' ' << col;
  os << '\n';
  for (const auto &row : stream.GetResults()) {
    os << "ROW:";
    for (const auto &val : row) os << " | " << val;
    os << '\n';
  }
  return os.str();
}

class CursorParityTest : public ::testing::Test {
 protected:
  const std::string testSuite = "cursor_parity";
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_cursor_parity";

  memgraph::storage::Config config{[&]() {
    memgraph::storage::Config c{};
    c.durability.storage_directory = data_directory;
    c.disk.main_storage_directory = c.durability.storage_directory / "disk";
    return c;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&]() {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    return *db_acc_opt;
  }()};
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,  // DbmsHandler*
                                                          &repl_state,
                                                          system_state,
                                                          nullptr  // ServerContext*
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker interpreter{&interpreter_context, db};

  void TearDown() override {
    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
    std::filesystem::remove_all(data_directory);
  }

  // Run `query` synchronously and then with the coroutine root-drive forced ON; assert identical renders.
  void ExpectParity(const std::string &query) {
    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
    auto legacy = interpreter.Interpret(query);

    memgraph::query::plan::SetForceCoroRootDriveForTesting(true);
    auto coroutine = interpreter.Interpret(query);

    memgraph::query::plan::SetForceCoroRootDriveForTesting(false);

    EXPECT_EQ(Render(legacy), Render(coroutine)) << "PARITY MISMATCH for query: " << query;
  }

  // Parity for WRITE cursors. A mutation changes shared state, so we cannot run it twice on the same
  // data. Instead each drive mode gets its OWN fresh, equivalent sub-graph: run `setup` (sync),
  // run `mutation` under the drive mode and capture its RETURNED rows, then `cleanup` (sync). The two
  // runs are independent and start from identical input, so the returned rows must match. Mutation
  // queries must RETURN deterministic projections (properties / computed values, never internal ids).
  //
  // Optional `readback`: when non-empty, the mutation itself need not RETURN anything (EmptyResult
  // path). After the mutation runs, `readback` is executed sync to observe the written state;
  // its render is used for comparison instead of the mutation's (empty) render. The readback runs
  // before `cleanup` so it sees the mutated sub-graph.
  void ExpectMutationParity(const std::string &setup, const std::string &mutation, const std::string &cleanup,
                            const std::string &readback = "") {
    auto run = [&](bool coro) {
      memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
      if (!setup.empty()) interpreter.Interpret(setup);
      memgraph::query::plan::SetForceCoroRootDriveForTesting(coro);
      auto stream = interpreter.Interpret(mutation);
      memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
      std::string result = readback.empty() ? Render(stream) : Render(interpreter.Interpret(readback));
      if (!cleanup.empty()) interpreter.Interpret(cleanup);
      return result;
    };
    const auto legacy = run(false);
    const auto coroutine = run(true);
    EXPECT_EQ(legacy, coroutine) << "MUTATION PARITY MISMATCH for: " << mutation;
  }
};

// Read corpus. GROW this as cursor families are converted (each PR adds queries routing through its
// cursors). With no cursor converted yet, every query runs synchronously under both drive modes — this
// proves the harness + the coroutine root-drive handle the full operator surface end-to-end.
TEST_F(CursorParityTest, Corpus) {
  // Seed deterministic data once (sync). Reads below compare across both drive modes.
  memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
  interpreter.Interpret("CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3})");
  interpreter.Interpret("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E {w: 10}]->(b)");
  interpreter.Interpret("MATCH (b:N {id: 2}), (c:N {id: 3}) CREATE (b)-[:E {w: 20}]->(c)");
  // Indexed label :J + edges :JE for the IndexedJoin corpus query (the same multi-pattern equi-join
  // that yields a HashJoin on the un-indexed :N becomes an IndexedJoin once :J(id) is indexed).
  interpreter.Interpret("CREATE INDEX ON :J(id)");
  interpreter.Interpret("CREATE (:J {id: 1})-[:JE]->(:J {id: 2})");
  interpreter.Interpret("CREATE (:J {id: 1})-[:JE]->(:J {id: 3})");

  const std::vector<std::string> corpus = {
      // Once + Produce (P1.1/P1.2 dual-path).
      "RETURN 1",
      "RETURN 1 + 2 AS s, 'x' AS t",
      // ScanAll + Produce (P1.2 dual-path); OrderBy/Distinct now dual-path (P1.10).
      "MATCH (n:N) RETURN n.id AS id ORDER BY id",
      "MATCH (n:N) RETURN n.id AS id ORDER BY id LIMIT 2",
      "MATCH (n:N) RETURN n.id AS id ORDER BY id SKIP 1",
      "MATCH (n:N) WHERE n.id > 1 RETURN n.id AS id ORDER BY id",
      "MATCH (n:N) RETURN count(*) AS c",
      "MATCH (n:N) RETURN sum(n.id) AS total",
      // Edge / expand paths (exercise ScanAllByEdge or Expand depending on the chosen plan).
      "MATCH (a:N)-[r:E]->(b:N) RETURN a.id AS aid, b.id AS bid, r.w AS w ORDER BY aid, bid",
      "MATCH ()-[r:E]->() RETURN r.w AS w ORDER BY w",
      "MATCH (a:N)-[:E]->(b:N) RETURN count(*) AS edges",
      "UNWIND [1, 2, 3, 3] AS x RETURN DISTINCT x ORDER BY x",
      // Filter / Skip / Limit (P1.3 dual-path).
      "MATCH (n:N) WHERE n.id >= 2 RETURN n.id AS id ORDER BY id",
      "MATCH (n:N) WHERE n.id = 2 OR n.id = 3 RETURN n.id AS id ORDER BY id SKIP 1 LIMIT 1",
      // ConstructNamedPath (P1.3 dual-path).
      "MATCH p = (a:N {id: 1})-[:E]->(b:N) RETURN size(relationships(p)) AS hops ORDER BY hops",
      // EvaluatePatternFilter / EXISTS (P1.3 dual-path synchronous island).
      "MATCH (n:N) WHERE exists((n)-[:E]->(:N)) RETURN n.id AS id ORDER BY id",
      // Expand (P1.4a dual-path) — directed and both-direction (exercises the BOTH cycle path).
      "MATCH (a:N {id: 2})-[:E]->(b:N) RETURN b.id AS bid ORDER BY bid",
      "MATCH (a:N)-[:E]-(b:N) RETURN a.id AS aid, b.id AS bid ORDER BY aid, bid",
      // ExpandVariable + EdgeUniquenessFilter (P1.4b dual-path) — variable-length over the 1->2->3 chain.
      "MATCH (a:N {id: 1})-[:E*1..2]->(b:N) RETURN b.id AS bid ORDER BY bid",
      "MATCH (a:N {id: 1})-[r:E*1..3]->(b:N) RETURN size(r) AS hops, b.id AS bid ORDER BY hops, bid",
      // Shortest-path BFS (P1.5 dual-path): SingleSource (source bound) + ST (both endpoints bound).
      "MATCH (a:N {id: 1})-[r *BFS]->(b:N) RETURN b.id AS bid, size(r) AS hops ORDER BY bid",
      "MATCH (a:N {id: 1})-[r *BFS]->(b:N {id: 3}) RETURN size(r) AS hops",
      // Weighted / all-shortest (P1.6 dual-path) over weighted 1-(10)->2-(20)->3.
      "MATCH (a:N {id: 1})-[r *WSHORTEST (e, n | e.w) total]->(b:N) RETURN b.id AS bid, total AS cost ORDER BY bid",
      "MATCH (a:N {id: 1})-[r *ALLSHORTEST (e, n | e.w) total]->(b:N) RETURN b.id AS bid, total AS cost ORDER BY bid",
      // K-shortest (Yen's, P1.7 dual-path) over the 1->2->3 chain. KSHORTEST needs both endpoints
      // bound, so match the pair first.
      "MATCH (a:N {id: 1}), (b:N {id: 3}) WITH a, b MATCH (a)-[r:E *KSHORTEST]->(b) RETURN size(r) AS hops ORDER BY "
      "hops",
      // Aggregate (P1.9 dual-path): group-by, avg/min/max, DISTINCT-agg, no-input default aggregation.
      "MATCH (n:N) RETURN n.id % 2 AS parity, count(*) AS c ORDER BY parity",
      "MATCH (n:N) RETURN avg(n.id) AS a, min(n.id) AS mn, max(n.id) AS mx",
      "MATCH (n:N) RETURN count(DISTINCT (n.id % 2)) AS c",
      "MATCH (n:N) RETURN size(collect(n.id)) AS c",
      "MATCH (n:NoSuchLabel) RETURN count(*) AS c, sum(n.id) AS s",  // no-input -> DefaultAggregation
      // OrderBy (P1.10 dual-path): descending + multi-key + ORDER BY over an expression.
      "MATCH (n:N) RETURN n.id AS id ORDER BY id DESC",
      "MATCH (a:N)-[r:E]->(b:N) RETURN a.id AS aid, r.w AS w ORDER BY r.w DESC, a.id ASC",
      "MATCH (n:N) RETURN n.id AS id ORDER BY n.id % 2, n.id DESC",
      // Distinct (P1.10 dual-path): single-symbol, multi-symbol, and DISTINCT over a chained projection.
      "UNWIND [1, 1, 2, 2, 3] AS x RETURN DISTINCT x ORDER BY x",
      "UNWIND [[1, 2], [1, 2], [1, 3]] AS pair RETURN DISTINCT pair[0] AS a, pair[1] AS b ORDER BY a, b",
      "MATCH (n:N) WITH DISTINCT n.id % 2 AS parity RETURN parity ORDER BY parity",
      // Combiners (P1.11 dual-path).
      // Cartesian: comma-separated patterns with no shared symbol -> cross product.
      "MATCH (a:N), (b:N) RETURN a.id AS aid, b.id AS bid ORDER BY aid, bid",
      // Optional: left-outer with null-fill where no outgoing edge exists (node id 3).
      "MATCH (n:N) OPTIONAL MATCH (n)-[:E]->(m:N) RETURN n.id AS nid, m.id AS mid ORDER BY nid, mid",
      // Apply: correlated CALL subquery (the subquery cursor is driven per input row).
      "MATCH (n:N) CALL { WITH n MATCH (n)-[:E]->(m:N) RETURN m.id AS mid } RETURN n.id AS nid, mid ORDER BY nid, mid",
      // RollUpApply: pattern comprehension collects a per-row list (drives list_collection_cursor_).
      "MATCH (n:N) RETURN n.id AS id, [(n)-[:E]->(m:N) | m.id] AS outs ORDER BY id",
      // HashJoin: multi-pattern equi-join on the un-indexed :N (planner picks HashJoin, see query_plan
      // MatchMultiPatternWithHashJoin).
      "MATCH (a:N)-[:E]->(b:N), (c:N)-[:E]->(d:N) WHERE c.id = a.id "
      "RETURN a.id AS aid, b.id AS bid, d.id AS did ORDER BY aid, bid, did",
      // IndexedJoin: same equi-join shape over the indexed :J (planner picks IndexedJoin, see query_plan
      // MatchMultiPatternWithIndexJoin).
      "MATCH (a:J)-[:JE]->(b:J), (c:J)-[:JE]->(d:J) WHERE c.id = a.id "
      "RETURN a.id AS aid, b.id AS bid, d.id AS did ORDER BY aid, bid, did",
      // Sub-plan combiners (P1.12 dual-path).
      // Unwind: literal list + correlated list (one output row per element).
      "UNWIND [10, 20, 30] AS x RETURN x ORDER BY x",
      "MATCH (n:N) UNWIND [n.id, n.id * 10] AS v RETURN v ORDER BY v",
      // Union: DISTINCT concat (dedups) + UNION ALL concat (keeps dups).
      "RETURN 1 AS x UNION RETURN 2 AS x",
      "MATCH (n:N) RETURN n.id AS x UNION ALL RETURN 99 AS x",
      // CallProcedure (P1.13): a builtin procedure call (the registry is identical across both runs).
      "CALL mg.procedures() YIELD name RETURN count(name) AS c",
  };
  for (const auto &q : corpus) {
    ExpectParity(q);
  }

  // Coverage guard: HashJoin / IndexedJoin are planner-chosen, so confirm the corpus queries above
  // still route through them (otherwise the parity assertions would silently stop exercising those
  // DoPull paths). The planner is drive-mode-independent, so checking once (sync) is sufficient.
  memgraph::query::plan::SetForceCoroRootDriveForTesting(false);
  const auto hash_plan = Render(interpreter.Interpret(
      "EXPLAIN MATCH (a:N)-[:E]->(b:N), (c:N)-[:E]->(d:N) WHERE c.id = a.id RETURN a.id, b.id, d.id"));
  EXPECT_NE(hash_plan.find("HashJoin"), std::string::npos) << "HashJoin no longer planned:\n" << hash_plan;
  const auto index_plan = Render(interpreter.Interpret(
      "EXPLAIN MATCH (a:J)-[:JE]->(b:J), (c:J)-[:JE]->(d:J) WHERE c.id = a.id RETURN a.id, b.id, d.id"));
  EXPECT_NE(index_plan.find("IndexedJoin"), std::string::npos) << "IndexedJoin no longer planned:\n" << index_plan;
  const auto proc_plan = Render(interpreter.Interpret("EXPLAIN CALL mg.procedures() YIELD name RETURN count(name)"));
  EXPECT_NE(proc_plan.find("CallProcedure"), std::string::npos) << "CallProcedure no longer planned:\n" << proc_plan;

  // LoadCsv (P1.13): write a small CSV and read it back -- one output row per data line. The same file
  // is read under both drive modes (read-only), so the rendered rows must match.
  const auto csv_path = (data_directory / "parity.csv").string();
  {
    std::ofstream f(csv_path);
    f << "a,b\n1,x\n2,y\n3,z\n";
  }
  ExpectParity("LOAD CSV FROM \"" + csv_path + "\" WITH HEADER AS row RETURN row.a AS a, row.b AS b ORDER BY a");
  // FOLLOW-UP (tracked): OutputTable(Stream), LoadParquet/Jsonl and Periodic{Commit,Subquery} are NOT
  // parity-covered here -- they need binary fixtures / IN-TRANSACTIONS semantics that don't fit this
  // in-process harness. Today their safety rests on flag-OFF == master verbatim + the byte-identical
  // DoPull splice + the dedicated load_parquet / periodic-commit suites (which run flag-OFF). They are
  // to be exercised end-to-end under the flag in PHASE 3 (e2e + stress); a periodic-commit parity case
  // counting commits across both drive modes would be the strongest in-process guard if added earlier.
}

// Write-cursor parity (P1.8 MUTATE). Each case: {setup, mutation-with-RETURN, cleanup}. The mutation
// runs once sync and once coroutine-driven, each on its own fresh sub-graph; the RETURNED rows must match.
TEST_F(CursorParityTest, MutationCorpus) {
  const std::string cleanup = "MATCH (n:Tmp) DETACH DELETE n";

  struct Case {
    std::string setup;
    std::string mutation;
    std::string readback;  // optional: sync observation query run after the mutation (see ExpectMutationParity)
  };

  const std::vector<Case> cases = {
      // CreateNode
      {"", "CREATE (n:Tmp {v: 42}) RETURN n.v AS v"},
      // CreateExpand
      {"", "CREATE (a:Tmp {id: 1})-[r:R {w: 7}]->(b:Tmp {id: 2}) RETURN r.w AS w, a.id AS aid, b.id AS bid"},
      // SetProperty
      {"CREATE (:Tmp {id: 1})", "MATCH (n:Tmp) SET n.x = 9 RETURN n.x AS x"},
      // SetProperties (+=)
      {"CREATE (:Tmp {id: 1})", "MATCH (n:Tmp) SET n += {a: 1, b: 2} RETURN n.a AS a, n.b AS b"},
      // SetLabels
      {"CREATE (:Tmp {id: 1})", "MATCH (n:Tmp) SET n:Extra RETURN 'Extra' IN labels(n) AS has"},
      // RemoveProperty
      {"CREATE (:Tmp {id: 1, x: 5})", "MATCH (n:Tmp) REMOVE n.x RETURN n.x AS x"},
      // RemoveLabels
      {"CREATE (:Tmp:Extra {id: 1})", "MATCH (n:Tmp) REMOVE n:Extra RETURN 'Extra' IN labels(n) AS has"},
      // Delete (buffered passthrough; capture id BEFORE delete -- can't read a deleted object)
      {"CREATE (:Tmp {id: 1}), (:Tmp {id: 2})", "MATCH (n:Tmp) WITH n, n.id AS id DELETE n RETURN id ORDER BY id"},
      // EmptyResult sink (P1.9): a no-RETURN write drains through EmptyResult; the readback verifies the
      // write landed identically under both pull paths (the mutation itself returns nothing).
      {"CREATE (:Tmp {id: 1}), (:Tmp {id: 2})",
       "MATCH (n:Tmp) SET n.x = 1",
       "MATCH (n:Tmp) RETURN n.x AS x ORDER BY x"},
      // Accumulate (P1.9): WITH between MATCH and SET forces materialization of the read side.
      {"CREATE (:Tmp {id: 1}), (:Tmp {id: 2})", "MATCH (n:Tmp) WITH n ORDER BY n.id SET n.seq = 1 RETURN n.seq AS s"},
      // SetNestedProperty (P1.10): replace a leaf key inside an existing nested map.
      {"CREATE (:Tmp {id: 1, data: {x: 1, y: 2}})",
       "MATCH (n:Tmp) SET n.data.x = 99 RETURN n.data.x AS x, n.data.y AS y"},
      // SetNestedProperty APPEND (+=): merge a map into a nested key.
      {"CREATE (:Tmp {id: 1, data: {x: 1}})",
       "MATCH (n:Tmp) SET n.data += {y: 2, z: 3} RETURN n.data.x AS x, n.data.y AS y, n.data.z AS z"},
      // SetNestedProperty creating the nested map from scratch (lhs null -> new map).
      {"CREATE (:Tmp {id: 1})", "MATCH (n:Tmp) SET n.data.k = 7 RETURN n.data.k AS k"},
      // RemoveNestedProperty (P1.10): erase a leaf key from an existing nested map.
      {"CREATE (:Tmp {id: 1, data: {x: 1, y: 2}})",
       "MATCH (n:Tmp) REMOVE n.data.x RETURN n.data.x AS x, n.data.y AS y"},
      // Merge (P1.12): create branch (no match -> merge_create) returns the created node's id.
      {"", "MERGE (n:Tmp {id: 1}) RETURN n.id AS id"},
      // Merge (P1.12): match branch (existing node -> merge_match + ON MATCH SET).
      {"CREATE (:Tmp {id: 1})", "MERGE (n:Tmp {id: 1}) ON MATCH SET n.matched = true RETURN n.matched AS m"},
      // Foreach (P1.12): the updates sub-plan runs once per list element; RETURN the observable result.
      {"CREATE (:Tmp {id: 1})",
       "MATCH (n:Tmp) FOREACH (x IN [1, 2, 3] | SET n.cnt = coalesce(n.cnt, 0) + x) RETURN n.cnt AS c"},
  };
  for (const auto &c : cases) {
    ExpectMutationParity(c.setup, c.mutation, cleanup, c.readback);
  }
}

}  // namespace
