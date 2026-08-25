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

#include <vector>

#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/distinct_key.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

namespace {

class DistinctKeyTest : public ::testing::Test {
 protected:
  AstStorage storage;
  SymbolTable symbol_table;

  Symbol n = symbol_table.CreateSymbol("n", true);
  Symbol m = symbol_table.CreateSymbol("m", true);

  /// A projection of `expression`, bound to a fresh output symbol, as RETURN
  /// builds one per returned value.
  NamedExpression *Project(std::string name, Expression *expression) {
    auto *named = storage.Create<NamedExpression>(name, expression);
    named->MapTo(symbol_table.CreateSymbol(std::move(name), true));
    return named;
  }

  Expression *Read(Symbol const &symbol) { return storage.Create<Identifier>(symbol.name())->MapTo(symbol); }

  Expression *PropertyOf(Symbol const &symbol, std::string property) {
    return storage.Create<PropertyLookup>(Read(symbol), storage.GetPropertyIx(std::move(property)));
  }

  std::vector<std::string> KeptNames(std::vector<NamedExpression *> const &projections) {
    auto const kept = ReducedDistinctKey(projections, symbol_table);
    std::vector<std::string> names;
    names.reserve(kept.size());
    for (auto const &symbol : kept) names.push_back(symbol.name());
    return names;
  }
};

TEST_F(DistinctKeyTest, KeepsAKeyOfOneVertex) {
  EXPECT_EQ(KeptNames({Project("n", Read(n))}), (std::vector<std::string>{"n"}));
}

TEST_F(DistinctKeyTest, DropsAPropertyOfAVertexTheKeyAlreadyHolds) {
  auto *vertex = Project("n", Read(n));
  auto *property = Project("n.id", PropertyOf(n, "id"));
  EXPECT_EQ(KeptNames({property, vertex}), (std::vector<std::string>{"n"}));
}

TEST_F(DistinctKeyTest, KeepsAPropertyWhoseVertexTheKeyDoesNotHold) {
  // Two vertices may carry the same id, so the property separates rows the key
  // has nothing else to separate.
  EXPECT_EQ(KeptNames({Project("n.id", PropertyOf(n, "id"))}), (std::vector<std::string>{"n.id"}));
}

TEST_F(DistinctKeyTest, DropsEveryPropertyOfAVertexTheKeyHolds) {
  auto *vertex = Project("n", Read(n));
  auto *id = Project("n.id", PropertyOf(n, "id"));
  auto *age = Project("n.age", PropertyOf(n, "age"));
  EXPECT_EQ(KeptNames({id, vertex, age}), (std::vector<std::string>{"n"}));
}

TEST_F(DistinctKeyTest, DropsOnlyThePropertyWhoseVertexIsHeld) {
  auto *vertex = Project("n", Read(n));
  auto *held = Project("n.id", PropertyOf(n, "id"));
  auto *loose = Project("m.id", PropertyOf(m, "id"));
  EXPECT_EQ(KeptNames({vertex, held, loose}), (std::vector<std::string>{"n", "m.id"}));
}

TEST_F(DistinctKeyTest, DropsAnAliasOfAVertexTheKeyAlreadyHolds) {
  auto *vertex = Project("n", Read(n));
  auto *alias = Project("same", Read(n));
  EXPECT_EQ(KeptNames({vertex, alias}), (std::vector<std::string>{"n"}));
}

TEST_F(DistinctKeyTest, DropsAConstant) {
  // A column the same in every row cannot separate any two of them.
  auto *vertex = Project("n", Read(n));
  auto *constant = Project("one", storage.Create<PrimitiveLiteral>(1));
  EXPECT_EQ(KeptNames({vertex, constant}), (std::vector<std::string>{"n"}));
}

TEST_F(DistinctKeyTest, KeepsOneColumnWhereEveryColumnIsConstant) {
  // Deduplicating on a constant leaves one row; dropping the lot would leave
  // them all.
  auto *first = Project("one", storage.Create<PrimitiveLiteral>(1));
  auto *second = Project("two", storage.Create<PrimitiveLiteral>(2));
  EXPECT_EQ(KeptNames({first, second}).size(), 1U);
}

TEST_F(DistinctKeyTest, KeepsAnExpressionItCannotAccountFor) {
  auto *vertex = Project("n", Read(n));
  auto *call = Project("r", storage.Create<Function>("RAND", std::vector<Expression *>{}));
  EXPECT_EQ(KeptNames({vertex, call}), (std::vector<std::string>{"n", "r"}));
}

TEST_F(DistinctKeyTest, KeepsAPropertyOfAPropertyWhoseVertexIsLoose) {
  EXPECT_EQ(KeptNames({Project("nested", PropertyOf(n, "a"))}), (std::vector<std::string>{"nested"}));
}

}  // namespace
