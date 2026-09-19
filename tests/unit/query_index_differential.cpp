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

// No query may answer differently for having an index.
//
// An index is a performance decision, so a plan that swaps a filter for a scan
// has to hand back the rows the filter kept. The two sides read different
// relations to decide that, and each place they can disagree is a wrong answer
// rather than a slow one.
//
// The question is asked through the interpreter rather than through a plan built
// by hand, because what is under test is the plan the planner picks: building
// the scan directly would assume the swap this is meant to check.

#include <algorithm>
#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "interpreter_faker.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace {

/// One entry per stored type, with several values inside each so that a
/// value-level disagreement is reachable rather than only a type-level one.
///
/// The awkward values are deliberate: the ends of the integer range, an
/// infinity, a NaN, an empty string, an empty container, and a list holding more
/// than one type.
struct ValuesOfOneType {
  std::string_view name;
  std::vector<std::string> values;
};

std::vector<ValuesOfOneType> const kValuesByType = {
    {"boolean", {"true", "false"}},
    // The smallest integer is absent because it has no literal: the parser reads
    // the minus as an operator and the digits alone are out of range.
    {"integer", {"-9223372036854775807", "-1", "0", "1", "9007199254740993", "9223372036854775807"}},
    {"double", {"-1.0 / 0.0", "-1.5", "0.0", "1.5", "1.0 / 0.0", "sqrt(-1)"}},
    {"string", {"''", "'a'", "'ab'", "'b'"}},
    {"date", {"date('1970-01-01')", "date('2020-02-29')", "date('9999-12-31')"}},
    {"local time", {"localTime('00:00:00')", "localTime('12:34:56')", "localTime('23:59:59')"}},
    {"local date time", {"localDateTime('1970-01-01T00:00:00')", "localDateTime('2020-01-01T12:00:00')"}},
    {"duration", {"duration('PT1S')", "duration('P1D')", "duration('P100D')"}},
    {"zoned date time", {"datetime('1970-01-01T00:00:00+00:00')", "datetime('2020-01-01T12:00:00+02:00')"}},
    {"list", {"[]", "[1]", "[1, 2]", "[1, 'a']", "['a']"}},
    {"map", {"{}", "{a: 1}", "{a: 2}"}},
    {"point", {"point({x: 0, y: 0})", "point({x: 1, y: 2})", "point({x: sqrt(-1), y: 1})"}},
};

/// A column holding more than one type, which is where the two relations are
/// most likely to part: each has its own rule for a pair they cannot both place.
std::vector<std::string> const kMixedValues = {"1",
                                               "2.5",
                                               "'a'",
                                               "true",
                                               "[1]",
                                               "{a: 1}",
                                               "date('2020-01-01')",
                                               "duration('P1D')",
                                               "point({x: 1, y: 2})",
                                               "sqrt(-1)"};

}  // namespace

class IndexDifferentialTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_index_differential"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        return config;
      }()  // iile
  };

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        return *db_acc_opt;
      }()  // iile
  };

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  InterpreterFaker interpreter{&interpreter_context, db};

  void TearDown() override { std::filesystem::remove_all(data_directory); }

  void Run(std::string const &query) { interpreter.Interpret(query); }

  /// The one number a counting query hands back.
  int64_t CountOf(std::string const &query) {
    auto stream = interpreter.Interpret(query);
    auto const &rows = stream.GetResults();
    if (rows.empty()) return 0;
    return rows.front().front().ValueInt();
  }

  /// Every predicate worth asking of a column holding these values.
  static std::vector<std::string> Probes(std::vector<std::string> const &values) {
    auto probes = std::vector<std::string>{};
    for (auto const &value : values) {
      for (auto const *op : {"<", "<=", ">", ">="}) probes.push_back(std::string{op} + " " + value);
    }
    for (auto const &value : values) probes.push_back("= " + value);
    probes.emplace_back("IS NOT NULL");

    auto in_list = std::string{"IN ["};
    for (auto i = 0U; i != values.size(); ++i) {
      if (i != 0) in_list += ", ";
      in_list += values[i];
    }
    probes.push_back(in_list + "]");
    return probes;
  }

  /// The node count and the edge count for each probe, in one list.
  std::vector<int64_t> Counts(std::vector<std::string> const &probes) {
    auto answers = std::vector<int64_t>{};
    for (auto const &probe : probes) {
      answers.push_back(CountOf("MATCH (n:D) WHERE n.p " + probe + " RETURN count(n) AS c;"));
      answers.push_back(CountOf("MATCH ()-[r:D]->() WHERE r.p " + probe + " RETURN count(r) AS c;"));
    }
    return answers;
  }

  /// A node and an edge carrying each value, so both scans are asked.
  void Load(std::vector<std::string> const &values) {
    Run("MATCH (n) DETACH DELETE n;");
    for (auto const &value : values) Run("CREATE (:D {p: " + value + "});");
    Run("CREATE (:From), (:To);");
    for (auto const &value : values) {
      Run("MATCH (a:From), (b:To) CREATE (a)-[:D {p: " + value + "}]->(b);");
    }
  }

  /// Whether the plan for this query reads an index rather than filtering.
  ///
  /// Without this the comparison could hold by asking the same plan twice: a
  /// query the planner never swaps is a filter on both sides, and agreeing with
  /// itself says nothing about the relations the two sides read.
  bool PlanReadsAnIndex(std::string const &query) {
    auto stream = interpreter.Interpret("EXPLAIN " + query);
    for (auto const &row : stream.GetResults()) {
      auto const &line = row.front().ValueString();
      if (line.find("ScanAllByLabelProperties") != std::string::npos) return true;
      if (line.find("ScanAllByEdgeTypeProperty") != std::string::npos) return true;
    }
    return false;
  }

  /// A row as it reads, which is how two orders are compared.
  ///
  /// A NaN is not equal to itself, so comparing two orders that both hold one
  /// would fail on values that arrived in the same place. Reading them instead
  /// tells two NaNs apart from anything else but not from each other.
  static std::string AsItReads(memgraph::communication::bolt::Value const &value) {
    auto rendered = std::ostringstream{};
    rendered << value;
    return rendered.str();
  }

  /// The one column a query hands back, in the order it hands it back.
  std::vector<std::string> Ordered(std::string const &query) {
    auto stream = interpreter.Interpret(query);
    auto rows = std::vector<std::string>{};
    for (auto const &row : stream.GetResults()) rows.push_back(AsItReads(row.front()));
    return rows;
  }

  /// Runs each query with no index, then with one, and hands back both.
  std::pair<std::vector<std::vector<std::string>>, std::vector<std::vector<std::string>>> OrderAgrees(
      std::vector<std::string> const &values, std::vector<std::string> const &queries,
      std::vector<std::string> const &index_statements) {
    Run("MATCH (n) DETACH DELETE n;");
    for (auto const &value : values) Run("CREATE (:O {p: " + value + "});");

    auto without_index = std::vector<std::vector<std::string>>{};
    for (auto const &query : queries) without_index.push_back(Ordered(query));

    for (auto const &statement : index_statements) Run(statement);

    auto with_index = std::vector<std::vector<std::string>>{};
    for (auto const &query : queries) with_index.push_back(Ordered(query));

    return {std::move(without_index), std::move(with_index)};
  }

  /// Names the probe that disagreed rather than only the two lists.
  static std::string Report(std::vector<std::string> const &probes, std::vector<int64_t> const &without,
                            std::vector<int64_t> const &with) {
    auto lines = std::string{};
    for (auto i = 0U; i != probes.size(); ++i) {
      for (auto const &[offset, kind] : {std::pair{0U, "node"}, std::pair{1U, "edge"}}) {
        auto const at = i * 2 + offset;
        if (without[at] != with[at]) {
          lines += "\n  p " + probes[i] + " (" + kind + "): " + std::to_string(without[at]) + " without an index, " +
                   std::to_string(with[at]) + " with one";
        }
      }
    }
    return lines;
  }

  /// Asks every probe with no index, then with one, and hands back both.
  void AnswersAgree(std::vector<std::string> const &values, std::string_view what) {
    Load(values);
    auto const probes = Probes(values);

    auto const counting_query = "MATCH (n:D) WHERE n.p " + probes.front() + " RETURN count(n) AS c;";

    auto const filtered_before = PlanReadsAnIndex(counting_query);
    auto const without_index = Counts(probes);

    Run("CREATE INDEX ON :D(p);");
    Run("CREATE EDGE INDEX ON :D(p);");
    auto const reads_an_index = PlanReadsAnIndex(counting_query);
    auto const with_index = Counts(probes);
    Run("DROP INDEX ON :D(p);");
    Run("DROP EDGE INDEX ON :D(p);");

    // The two sides have to be two plans. Asked of the same query before and
    // after the index exists, so a planner that stopped choosing the scan would
    // show up here rather than as a silently passing comparison.
    EXPECT_FALSE(filtered_before) << "the plan for " << what << " read an index before one was created";
    EXPECT_TRUE(reads_an_index) << "the plan for " << what
                                << " filtered on both sides, so the comparison asked the same plan twice";

    EXPECT_EQ(with_index, without_index) << "an index changed the answer for " << what << ":"
                                         << Report(probes, without_index, with_index);

    // Non-vacuous: a run where every probe kept no row would pass while asking
    // nothing, and an empty column is exactly how that happens.
    EXPECT_TRUE(std::ranges::any_of(without_index, [](auto count) { return count > 0; }))
        << "no probe over " << what << " kept a row";
  }
};

TEST_F(IndexDifferentialTest, AnIndexOverOneTypeAnswersAsTheFilterDoes) {
  for (auto const &[name, values] : kValuesByType) {
    SCOPED_TRACE(name);
    AnswersAgree(values, name);
  }
}

TEST_F(IndexDifferentialTest, AnIndexOverAColumnOfManyTypesAnswersAsTheFilterDoes) {
  AnswersAgree(kMixedValues, "a column of many types");
}

TEST_F(IndexDifferentialTest, AnIndexOnTwoPropertiesAnswersAsTheFilterDoes) {
  // A composite index, with its trailing level left unbounded. The fence a scan
  // stops at when a level carries no bound of its own has to sit above every
  // value that level could hold, and the trailing values here run through every
  // type so that the fence is asked about each of them.
  Run("MATCH (n) DETACH DELETE n;");
  for (auto i = 0U; i != kMixedValues.size(); ++i) {
    Run("CREATE (:C {a: " + std::to_string(i) + ", b: " + kMixedValues[i] + "});");
  }

  auto const queries = std::vector<std::string>{
      "MATCH (n:C) WHERE n.a >= 0 RETURN count(n) AS c;",
      "MATCH (n:C) WHERE n.a > 2 RETURN count(n) AS c;",
      "MATCH (n:C) WHERE n.a >= 0 AND n.a < 100 RETURN count(n) AS c;",
      "MATCH (n:C) WHERE n.a >= 0 RETURN count(n.b) AS c;",
  };
  auto counts = [&] {
    auto answers = std::vector<int64_t>{};
    for (auto const &query : queries) answers.push_back(CountOf(query));
    return answers;
  };

  auto const without_index = counts();
  Run("CREATE INDEX ON :C(a, b);");
  auto const with_index = counts();
  Run("DROP INDEX ON :C(a, b);");

  EXPECT_EQ(with_index, without_index) << "a composite index changed the answer";
  EXPECT_EQ(without_index.front(), static_cast<int64_t>(kMixedValues.size()));
}

TEST_F(IndexDifferentialTest, AnIndexWalksAMixedColumnInTheOrderASortReadsIt) {
  // A plan drops a sort when the scan beneath it already walked the column. That
  // answers the sort only where an index walks a column of many types in the
  // order the sort would have put it in, so a column holding one of each is
  // where the two would part company if they ever did.
  auto const queries = std::vector<std::string>{
      "MATCH (n:O) WHERE n.p IS NOT NULL RETURN n.p AS v ORDER BY n.p;",
      "MATCH (n:O) WHERE n.p IS NOT NULL RETURN n.p AS v ORDER BY n.p DESC;",
  };

  auto const [without_index, with_index] = OrderAgrees(kMixedValues, queries, {"CREATE INDEX ON :O(p);"});

  EXPECT_EQ(with_index, without_index) << "an index changed the order rows come back in";
}

TEST_F(IndexDifferentialTest, AnIndexWalksTheTemporalKindsInTheOrderASortReadsThem) {
  // One stored type carries four of the date and time kinds and tells them apart
  // before anything else, so a column of all four is walked kind by kind. A sort
  // gives each kind its own place, and the two placements are the same one.
  auto const values = std::vector<std::string>{
      "duration('P1D')",
      "date('2020-01-01')",
      "localTime('12:00:00')",
      "localDateTime('2020-01-01T12:00:00')",
  };
  auto const queries = std::vector<std::string>{"MATCH (n:O) WHERE n.p IS NOT NULL RETURN n.p AS v ORDER BY n.p;"};

  auto const [without_index, with_index] = OrderAgrees(values, queries, {"CREATE INDEX ON :O(p);"});

  EXPECT_EQ(with_index, without_index) << "an index changed the order of a temporal column";
}

TEST_F(IndexDifferentialTest, AScanStandsInForTheSortItMatches) {
  // The other half, so that agreeing on an order is not paid for by sorting
  // anyway.
  //
  // A query is cached with its terms stripped out, so the type a bound will hold
  // is not settled when the plan is. The walk answers the sort whatever that
  // type turns out to be, which is what lets the sort go in every one of these.
  for (auto const *predicate : {"n.p = 2", "n.p > 1", "n.p >= 2 AND n.p < 100", "n.p IS NOT NULL"}) {
    SCOPED_TRACE(predicate);
    auto const query = "MATCH (n:O) WHERE " + std::string{predicate} + " RETURN n.p AS v ORDER BY n.p;";

    auto const [without_index, with_index] =
        OrderAgrees({"1", "2", "3", "10", "20"}, {query}, {"CREATE INDEX ON :O(p);"});
    EXPECT_EQ(with_index, without_index);

    auto plan = std::string{};
    for (auto const &row : interpreter.Interpret("EXPLAIN " + query).GetResults()) {
      plan += row.front().ValueString() + "\n";
    }
    EXPECT_EQ(plan.find("OrderBy"), std::string::npos) << "the sort was kept where the scan can stand in for it:\n"
                                                       << plan;

    Run("DROP INDEX ON :O(p);");
  }
}

TEST_F(IndexDifferentialTest, TheStringPredicatesAnswerAsTheFilterDoes) {
  // The three that read a search term rather than a bound, on both scans.
  auto const values = std::vector<std::string>{"''", "'a'", "'ab'", "'abc'", "'b'", "'zzz'"};
  Load(values);

  auto const probes = std::vector<std::string>{
      "STARTS WITH 'a'",
      "STARTS WITH ''",
      "CONTAINS 'b'",
      "CONTAINS ''",
      "ENDS WITH 'c'",
  };

  auto const without_index = Counts(probes);
  Run("CREATE INDEX ON :D(p);");
  Run("CREATE EDGE INDEX ON :D(p);");
  auto const with_index = Counts(probes);
  Run("DROP INDEX ON :D(p);");
  Run("DROP EDGE INDEX ON :D(p);");

  EXPECT_EQ(with_index, without_index) << "an index changed the answer for a string predicate:"
                                       << Report(probes, without_index, with_index);
  EXPECT_TRUE(std::ranges::any_of(without_index, [](auto count) { return count > 0; }));
}
