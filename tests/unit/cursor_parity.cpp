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

// DUAL-PATH PARITY HARNESS (Phase 1 gate).
//
// The coroutine-cursor conversion keeps each cursor's legacy synchronous body (PullLegacy) alongside
// its coroutine body (DoPull); the base Cursor::Pull router selects between them per the
// COROUTINE_CURSORS experiment flag, captured at cursor construction. This harness runs a corpus of
// queries with the flag OFF (legacy path) and ON (coroutine path) and asserts the rendered results
// are IDENTICAL. flag-OFF is byte-identical to the pre-coroutine engine, so this is a mechanical
// proof that the coroutine pull == the current pull.
//
// The corpus GROWS as cursor families are converted: a query only exercises the coroutine path for
// the cursors that have a DoPull, so each cursor PR should add queries that route through its
// cursors. Keep corpus queries DETERMINISTIC (fixed seed data + stable ordering) so the two renders
// compare equal without order-normalization.

#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "flags/experimental.hpp"
#include "glue/communication.hpp"
#include "interpreter_faker.hpp"
#include "query/interpreter_context.hpp"
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
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
    std::filesystem::remove_all(data_directory);
  }

  // Run `query` with the flag OFF (legacy pull) and ON (coroutine pull); assert identical renders.
  void ExpectParity(const std::string &query) {
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
    auto legacy = interpreter.Interpret(query);

    memgraph::flags::SetExperimental(memgraph::flags::Experiments::COROUTINE_CURSORS);
    auto coroutine = interpreter.Interpret(query);

    memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);

    EXPECT_EQ(Render(legacy), Render(coroutine)) << "PARITY MISMATCH for query: " << query;
  }
};

// Phase 1 corpus. GROW this as cursor families are converted (each PR adds queries routing through
// its cursors). At P1.1 only Once is dual-path; these prove the harness + routing end-to-end.
TEST_F(CursorParityTest, Corpus) {
  // Seed deterministic data once (flag OFF). Reads below compare across both flag states.
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::NONE);
  interpreter.Interpret("CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3})");
  interpreter.Interpret("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E {w: 10}]->(b)");
  interpreter.Interpret("MATCH (b:N {id: 2}), (c:N {id: 3}) CREATE (b)-[:E {w: 20}]->(c)");

  const std::vector<std::string> corpus = {
      // Once + Produce (P1.1/P1.2 dual-path).
      "RETURN 1",
      "RETURN 1 + 2 AS s, 'x' AS t",
      // ScanAll + Produce (P1.2 dual-path) + OrderBy/Limit/Aggregate/Distinct (legacy until their PRs).
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
  };
  for (const auto &q : corpus) {
    ExpectParity(q);
  }
}

}  // namespace
