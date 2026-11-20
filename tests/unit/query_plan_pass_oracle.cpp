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

// Holds reading a set of properties in one pass to what reading them one at a time answers, over
// generated queries rather than over the shapes somebody thought to write down.

#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

#include "query_execution_fixture.hpp"

template <typename StorageType>
class PassOracle : public QueryExecutionFixture<StorageType> {
 protected:
  std::string SuiteName() const override { return "query_plan_pass_oracle"; }
};

TYPED_TEST_SUITE(PassOracle, StorageTypes);

namespace {

std::string ForRecord(std::string_view shape, std::string_view record) {
  auto filled = std::string{};
  for (size_t at = 0; at < shape.size();) {
    if (shape.compare(at, 3, "{r}") == 0) {
      filled += record;
      at += 3;
      continue;
    }
    filled += shape[at++];
  }
  return filled;
}

/// Every query shape along the axes a pass can be wrong on: where the record comes from, what the
/// predicate asks of it, whether a later clause reads a slot the predicate filled, what the query
/// writes to the property, and what the query does with the value.
///
/// A combination this spells wrongly is kept rather than filtered. Refusing is an answer, and both
/// plans for a query have to refuse it alike.
std::vector<std::string> ShapesAPassCouldBeWrongOn() {
  static constexpr std::string_view kPatterns[] = {"MATCH (a)-[:E]->(b)", "MATCH (a)"};
  static constexpr std::string_view kRecords[] = {"b", "a"};
  static constexpr std::string_view kPredicates[] = {
      "",
      "WHERE {r}.p > 1 AND {r}.p < 50",
      "WHERE {r}.p > 1 AND {r}.q < 50",
      // Starts the scan at a, which leaves the predicate on the record above the expansion.
      "WHERE a.id >= 1 AND {r}.p > 1 AND {r}.p < 50",
  };
  static constexpr std::string_view kMiddles[] = {
      "",
      "WITH {r}, {r}.p AS v",
      // Orders the rows, so what reaches the clause above was rebuilt from what the ordering kept
      // rather than handed up as the pass left it.
      "WITH {r}, {r}.p AS v ORDER BY v",
  };
  static constexpr std::string_view kWrites[] = {
      "",
      "SET {r}.p = 99",
      "SET {r}.p = {r}.p + 1",
      // Reads the property twice to write it once, which is the shape that earns a pass elsewhere.
      "SET {r}.p = {r}.p + {r}.p",
      "REMOVE {r}.p",
      // Writes a property no predicate here reads, so a slot holding another one stays good.
      "SET {r}.q = 99",
      // Which properties these write is a value rather than something the plan says: the first
      // replaces every property the record holds, the second updates the ones its map names.
      "SET {r} = {p: 99}",
      "SET {r} += {p: 99}",
  };
  static constexpr std::string_view kTails[] = {
      "RETURN {r}.p AS x",
      "RETURN count(*) AS x",
      "RETURN sum({r}.p) AS x",
      // Returns what the middle clause projected rather than reading the property again, which is
      // the only way to see a value a later write would have changed. Nothing projects it without
      // that clause, and both plans then refuse the query alike.
      "RETURN v AS x ORDER BY x",
      // Groups by the property, so the value a slot holds has to survive being a grouping key.
      "RETURN {r}.p AS x, count(*) AS c ORDER BY x",
      // Reads the property twice in one aggregation, which is what earns the aggregation a pass of
      // its own above whatever the clauses below it left.
      "RETURN sum({r}.p) AS s, max({r}.p) AS m",
  };

  auto shapes = std::vector<std::string>{};
  for (size_t which = 0; which != std::size(kPatterns); ++which) {
    for (const auto predicate : kPredicates) {
      for (const auto middle : kMiddles) {
        for (const auto write : kWrites) {
          for (const auto tail : kTails) {
            auto parts = std::vector<std::string_view>{kPatterns[which], predicate, middle, write, tail};
            auto query = std::string{};
            for (const auto part : parts) {
              if (part.empty()) continue;
              if (!query.empty()) query += ' ';
              query += part;
            }
            shapes.push_back(ForRecord(query, kRecords[which]));
          }
        }
      }
    }
  }
  return shapes;
}

}  // namespace

/// Reading a set of properties in one pass must answer what reading them one at a time answers, for
/// every shape of query rather than for the shapes somebody thought to write down.
TYPED_TEST(PassOracle, OnePassAnswersWhatOneLookupAtATimeAnswers) {
  const auto shapes = ShapesAPassCouldBeWrongOn();
  ASSERT_GT(shapes.size(), 100u) << "the shapes are generated, so a drop in their number is a defect in the generator";

  for (const auto &query : shapes) {
    const auto lookups = this->RunWith(false, query);
    const auto pass = this->RunWith(true, query);

    EXPECT_EQ(lookups.rows, pass.rows) << query;
    EXPECT_EQ(lookups.graph, pass.graph) << query;
    EXPECT_EQ(lookups.refusal, pass.refusal) << query;
  }
}
