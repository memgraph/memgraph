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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/property_writes.hpp"

#include "query_common.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

namespace {

/// An operator that holds a plan branch the walk does not follow has to answer for everything,
/// because what that branch writes is not reachable from the operator itself. A `static_assert`
/// makes an operator that says nothing a compile error; it cannot tell an operator that answers
/// too narrowly from one that is right to. These hold each such operator to answering for a
/// property nothing in the plan mentions.
class PropertyWritesOfAHiddenBranch : public ::testing::Test {
 protected:
  AstStorage storage;
  SymbolTable symbol_table;

  auto MakeSymbol(std::string name) -> Symbol { return symbol_table.CreateSymbol(std::move(name), true); }

  auto NoInput() -> std::shared_ptr<LogicalOperator> { return std::make_shared<Once>(); }

  /// A branch with nothing in it. The operator above must answer for everything even so: a branch
  /// the walk cannot see is unknown whatever it happens to hold.
  auto EmptyBranch() -> std::shared_ptr<LogicalOperator> { return NoInput(); }

  void ExpectAnswersForEverything(LogicalOperator const &root) {
    auto const writes = PropertyWritesOf(root);
    EXPECT_TRUE(writes.MayWrite(AstPropertyIndex{storage.GetPropertyIx("a_property_no_operator_here_names")}));
  }
};

TEST_F(PropertyWritesOfAHiddenBranch, Apply) {
  auto const op = Apply{NoInput(), EmptyBranch(), OnEmptyBranch::kDropRow};
  ExpectAnswersForEverything(op);
}

TEST_F(PropertyWritesOfAHiddenBranch, PeriodicSubquery) {
  auto const op = PeriodicSubquery{NoInput(), EmptyBranch(), LITERAL(1), OnEmptyBranch::kDropRow};
  ExpectAnswersForEverything(op);
}

TEST_F(PropertyWritesOfAHiddenBranch, Optional) {
  auto const op = Optional{NoInput(), EmptyBranch(), std::vector<Symbol>{MakeSymbol("n")}};
  ExpectAnswersForEverything(op);
}

TEST_F(PropertyWritesOfAHiddenBranch, Merge) {
  auto const op = plan::Merge{NoInput(), EmptyBranch(), EmptyBranch()};
  ExpectAnswersForEverything(op);
}

TEST_F(PropertyWritesOfAHiddenBranch, Foreach) {
  auto const op = plan::Foreach{NoInput(), EmptyBranch(), LIST(), MakeSymbol("x")};
  ExpectAnswersForEverything(op);
}

TEST_F(PropertyWritesOfAHiddenBranch, Conditional) {
  auto branches = std::vector<Conditional::Branch>{};
  branches.push_back(Conditional::Branch{.predicate = nullptr, .plan = EmptyBranch()});
  auto const op = Conditional{NoInput(), std::move(branches), std::vector<Symbol>{}};
  ExpectAnswersForEverything(op);
}

/// The counterpart, so the tests above are known to be testing something: an operator with one
/// input and nothing hidden behind it answers for nothing, and a property nobody writes is not
/// reported as written.
TEST_F(PropertyWritesOfAHiddenBranch, APlainProjectionAnswersForNothing) {
  auto const op = Produce{NoInput(), std::vector<NamedExpression *>{NEXPR("x", LITERAL(1))}};
  EXPECT_FALSE(PropertyWritesOf(op).MayWrite(AstPropertyIndex{storage.GetPropertyIx("p")}));
}

}  // namespace
