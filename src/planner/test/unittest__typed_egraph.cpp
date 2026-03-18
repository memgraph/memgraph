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

// Tests for TypedEGraph, the typed Make<S>() wrapper over the core e-graph.
// Structural hash-consing (children, order, symbol distinctness) belongs to the
// core EGraph and is covered in unittest__egraph.cpp; here we test only what the
// typed layer itself owns:
//   - Make lowers to the same node a hand-written core emplace would produce.
//   - Make's disambiguator branch, reached via a stateful interning trait.
//   - storage<S>() recovers the per-symbol side-data Make wrote.
//   - SymbolMakeTraits rejects ill-formed traits at the constraint.

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "test_support/op_make_traits.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::planner::core {
namespace {

// A minimal symbol set whose only trait interns user data: make() records the
// name in per-symbol storage and returns the interned id as the disambiguator.
// This reaches the half of Make's dispatch that the production-like `Op` set
// (no storage, no disambiguator) never exercises.
enum class ToyOp : std::uint8_t { Symbol };

struct ToyAnalysis {};

template <ToyOp S>
struct toy_traits;

template <>
struct toy_traits<ToyOp::Symbol> {
  struct storage_type {
    std::map<std::string, std::uint64_t> by_name;
    std::uint64_t next_id = 0;
  };

  static auto make(storage_type &s, std::string_view name) -> LoweredNode {
    auto [it, inserted] = s.by_name.try_emplace(std::string{name}, s.next_id);
    if (inserted) {
      ++s.next_id;
    }
    return {.children = {}, .disambiguator = it->second};
  }
};

using ToyEGraph = TypedEGraph<ToyOp, ToyAnalysis, SymbolSequence<ToyOp, ToyOp::Symbol>, toy_traits>;

// A trait whose make() returns the wrong type, for the negative concept check.
struct BadTraits {
  struct storage_type {};

  static auto make(storage_type &) -> int { return 0; }
};

}  // namespace

// === Make: lowering parity with the core e-graph (S1) ===

TEST(TypedEGraph, MakeMatchesRawEmplace) {
  test::TypedTestEGraph eg;
  auto const x = eg.Make<test::Op::Var>();
  auto const y = eg.Make<test::Op::Const>();

  // Make<S>(args...) lowers to exactly the node a caller would emplace by hand.
  auto const sum = eg.Make<test::Op::Add>(x, y);
  auto const sum_raw = eg.core().emplace(test::Op::Add, utils::small_vector<EClassId>{x, y}).eclass_id;
  EXPECT_EQ(sum, sum_raw);

  // Children are positional: the wrapper does not canonicalise their order.
  EXPECT_NE(sum, eg.Make<test::Op::Add>(y, x));

  // A vector-argument trait forwards its children the same way, so identical
  // n-ary nodes hash-cons to one class.
  EXPECT_EQ(eg.Make<test::Op::F>(std::vector<EClassId>{x, y}), eg.Make<test::Op::F>(std::vector<EClassId>{x, y}));
}

// === Make: the disambiguator branch via interning (S2, S3) ===

TEST(TypedEGraph, InterningReusesClassForSameKey) {
  ToyEGraph eg;
  auto const x1 = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  auto const x2 = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  EXPECT_EQ(x1, x2);
  EXPECT_EQ(eg.core().num_classes(), 1u);
}

TEST(TypedEGraph, InterningSeparatesDistinctKeys) {
  ToyEGraph eg;
  auto const x = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  auto const y = eg.Make<ToyOp::Symbol>(std::string_view{"y"});
  EXPECT_NE(x, y);
  EXPECT_EQ(eg.core().num_classes(), 2u);
}

// === storage<S>(): per-symbol side-data recovery (S4) ===

TEST(TypedEGraph, StorageReflectsInternedEntries) {
  ToyEGraph eg;
  eg.Make<ToyOp::Symbol>(std::string_view{"alice"});
  eg.Make<ToyOp::Symbol>(std::string_view{"bob"});

  auto const &store = eg.storage<ToyOp::Symbol>();
  EXPECT_EQ(store.next_id, 2u);
  EXPECT_TRUE(store.by_name.contains("alice"));
  EXPECT_TRUE(store.by_name.contains("bob"));
}

// === SymbolMakeTraits: trait protocol enforcement (S5) ===

static_assert(SymbolMakeTraits<toy_traits<ToyOp::Symbol>, std::string_view>,
              "a well-formed interning trait satisfies the protocol");
static_assert(!SymbolMakeTraits<BadTraits>, "make() returning non-LoweredNode is rejected");

}  // namespace memgraph::planner::core
