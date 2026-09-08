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

// What a query value costs to copy, move, destroy, and compare.
//
// A row of expression evaluation builds and destroys a value per node it walks, and a filter, a
// DISTINCT and an ORDER BY each compare two of them per row, so these are among the most frequently
// executed instructions in the engine.
//
// Cypher compares two values by four different relations, and they are measured apart because they
// answer differently and are reached from different places: `=` and `IN` ask equality, DISTINCT and
// grouping ask equivalence, `<` and its three siblings ask comparability, and ORDER BY asks
// orderability.
//
// There is a case per type, so a change that costs something to tell types apart shows up on the
// types that gain nothing from it. What each operation costs depends on what the value holds: a
// type owning an allocation and a type owning nothing take different work.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "query/common.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace {

using memgraph::query::OrderedTypedValueCompare;
using memgraph::query::Ordering;
using memgraph::query::TypedValue;

memgraph::utils::MemoryResource *Mem() { return memgraph::utils::NewDeleteResource(); }

using Pair = std::pair<TypedValue, TypedValue>;

TypedValue ListOfInts(int64_t last) {
  auto items = std::vector<TypedValue>{};
  items.reserve(8);
  for (int64_t i = 0; i != 7; ++i) items.emplace_back(i, Mem());
  items.emplace_back(last, Mem());
  return TypedValue{std::move(items), Mem()};
}

/// One type per pair of values.
///
/// A shape is a type rather than a string, so a misspelled name fails to compile instead of
/// registering a benchmark that aborts when it runs. The two values in each pair differ, as two
/// values reaching a comparison usually do.
struct Int {
  static Pair Make() { return {TypedValue{int64_t{7'000'000}, Mem()}, TypedValue{int64_t{6'000'000}, Mem()}}; }
};

struct Double {
  static Pair Make() { return {TypedValue{33.14907, Mem()}, TypedValue{33.14908, Mem()}}; }
};

struct Bool {
  static Pair Make() { return {TypedValue{true, Mem()}, TypedValue{false, Mem()}}; }
};

struct String {
  static Pair Make() {
    return {TypedValue{std::string{"region_3"}, Mem()}, TypedValue{std::string{"region_4"}, Mem()}};
  }
};

struct Date {
  static Pair Make() {
    using memgraph::utils::Date;
    return {TypedValue{Date{std::chrono::microseconds{86'400'000'000}}, Mem()},
            TypedValue{Date{std::chrono::microseconds{172'800'000'000}}, Mem()}};
  }
};

/// Elements that differ at the end, so a comparison walks the whole list rather than settling on
/// the first pair it looks at.
struct List {
  static Pair Make() { return {ListOfInts(7), ListOfInts(8)}; }
};

/// Two numbers of unlike types, which the comparison widens before it can answer.
struct IntAgainstDouble {
  static Pair Make() { return {TypedValue{int64_t{499}, Mem()}, TypedValue{500.0, Mem()}}; }
};

/// A number that compares as unordered against every other, which is the one pair the ordering
/// answers differently from a plain three-way comparison.
struct NaN {
  static Pair Make() { return {TypedValue{std::numeric_limits<double>::quiet_NaN(), Mem()}, TypedValue{1.0, Mem()}}; }
};

TypedValue MapOfInts(bool with_null) {
  auto entries = std::map<std::string, TypedValue>{};
  for (int i = 0; i != 4; ++i) entries.emplace("k" + std::to_string(i), TypedValue{int64_t{i}, Mem()});
  if (with_null) entries.emplace("n", TypedValue{Mem()});
  return TypedValue{std::move(entries), Mem()};
}

struct Map {
  static Pair Make() { return {MapOfInts(false), MapOfInts(false)}; }
};

/// A container holding a null, where the two relations part company: equality cannot decide such a
/// pair while equivalence can.
struct MapHoldingNull {
  static Pair Make() { return {MapOfInts(true), MapOfInts(true)}; }
};

struct ListHoldingNull {
  static Pair Make() {
    auto with_null = [] {
      auto items = std::vector<TypedValue>{};
      for (int64_t i = 0; i != 8; ++i) items.emplace_back(i, Mem());
      items.emplace_back(Mem());
      return TypedValue{std::move(items), Mem()};
    };
    return {with_null(), with_null()};
  }
};

/// Lists inside a list, which is where a relation that recurses pays for the recursion.
struct NestedList {
  static Pair Make() {
    auto nested = [] {
      auto outer = std::vector<TypedValue>{};
      for (int i = 0; i != 4; ++i) outer.emplace_back(ListOfInts(7));
      return TypedValue{std::move(outer), Mem()};
    };
    return {nested(), nested()};
  }
};

// Copying is where a type that owns an allocation parts company with one that does not.
template <typename Shape>
void CopyConstruct(benchmark::State &state) {
  auto const source = Shape::Make().first;
  for (auto _ : state) {
    auto copy = TypedValue{source, Mem()};
    benchmark::DoNotOptimize(copy);
  }
  state.SetItemsProcessed(state.iterations());
}

// A frame write: move a value into a slot that already holds one. Both the type being overwritten
// and the type arriving decide whether this can be done where it stands.
template <typename Shape>
void MoveAssign(benchmark::State &state) {
  auto const source = Shape::Make().first;
  auto slot = TypedValue{Mem()};
  for (auto _ : state) {
    auto fresh = TypedValue{source, Mem()};
    slot = std::move(fresh);
    benchmark::DoNotOptimize(slot);
  }
  state.SetItemsProcessed(state.iterations());
}

// Building and destroying one value, which is what evaluating one node of an expression tree does.
//
// The value has to be made to escape before its scope closes. A type whose destructor does nothing
// and whose construction the compiler can see through is otherwise dead, and the loop measures an
// empty body - which reads as an enormous gain from any change that makes a destructor trivial.
template <typename Shape>
void ConstructDestroy(benchmark::State &state) {
  auto const source = Shape::Make().first;
  for (auto _ : state) {
    {
      auto copy = TypedValue{source, Mem()};
      benchmark::DoNotOptimize(copy);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}

// The four ordered comparisons, measured apart because they are not written alike: one of them is
// a single comparison, and the others reach their answer through two or three of it. A change to
// how they are written moves them by different amounts, and measuring them together would let a
// gain on one hide a loss on another.
template <typename Shape>
void Less(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  for (auto _ : state) benchmark::DoNotOptimize(lhs < rhs);
  state.SetItemsProcessed(state.iterations());
}

template <typename Shape>
void LessEqual(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  for (auto _ : state) benchmark::DoNotOptimize(lhs <= rhs);
  state.SetItemsProcessed(state.iterations());
}

template <typename Shape>
void Greater(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  for (auto _ : state) benchmark::DoNotOptimize(lhs > rhs);
  state.SetItemsProcessed(state.iterations());
}

template <typename Shape>
void GreaterEqual(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  for (auto _ : state) benchmark::DoNotOptimize(lhs >= rhs);
  state.SetItemsProcessed(state.iterations());
}

// Equality, the relation `=` and `IN` are answered by, asked of two values that are the same.
//
// Two values that differ are separated by the first element that differs, which for a container
// measures how quickly the walk gives up rather than what the walk costs. A probe reaching a
// populated bucket, or a row matching a filter, compares the whole of both values.
template <typename Shape>
void Equality(benchmark::State &state) {
  auto const lhs = Shape::Make().first;
  auto const rhs = TypedValue{lhs, Mem()};
  for (auto _ : state) benchmark::DoNotOptimize(lhs == rhs);
  state.SetItemsProcessed(state.iterations());
}

// Equivalence, the relation DISTINCT and grouping are answered by, and the hottest of the four: a
// hash set of query values calls it on every probe that reaches a populated bucket.
template <typename Shape>
void Equivalence(benchmark::State &state) {
  auto const lhs = Shape::Make().first;
  auto const rhs = TypedValue{lhs, Mem()};
  auto const equal = TypedValue::BoolEqual{};
  for (auto _ : state) benchmark::DoNotOptimize(equal(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// Orderability, the relation ORDER BY is answered by. A sort calls it once per comparison, which is
// the granularity that decides where it may be defined.
template <typename Shape>
void Orderability(benchmark::State &state) {
  auto const [lhs, rhs] = Shape::Make();
  auto const compare = OrderedTypedValueCompare{Ordering::ASC};
  for (auto _ : state) benchmark::DoNotOptimize(compare(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// What the relation exists for. A per-comparison figure alone hides how often a sort calls it, and
// a sort is where the cost is actually paid.
void SortColumn(benchmark::State &state) {
  auto source = std::vector<TypedValue>{};
  for (int i = 0; i != 4; ++i) {
    source.emplace_back(int64_t{7 - i}, Mem());
    source.emplace_back(0.5 * static_cast<double>(i), Mem());
    source.emplace_back(Mem());
  }
  auto const compare = OrderedTypedValueCompare{Ordering::ASC};
  for (auto _ : state) {
    auto values = source;
    std::ranges::sort(values,
                      [&compare](TypedValue const &a, TypedValue const &b) { return std::is_lt(compare(a, b)); });
    benchmark::DoNotOptimize(values);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(source.size()));
}

// The label comes from the same token that selects the shape, so the two cannot disagree.
#define SHAPE(bench, shape) BENCHMARK_TEMPLATE(bench, shape)->Name(#bench "/" #shape)->Unit(benchmark::kNanosecond)

#define FOR_EACH_TYPE(bench) \
  SHAPE(bench, Int);         \
  SHAPE(bench, Double);      \
  SHAPE(bench, Bool);        \
  SHAPE(bench, Date);        \
  SHAPE(bench, String);      \
  SHAPE(bench, List);

FOR_EACH_TYPE(CopyConstruct)
FOR_EACH_TYPE(MoveAssign)
FOR_EACH_TYPE(ConstructDestroy)
FOR_EACH_TYPE(Equality)
FOR_EACH_TYPE(Equivalence)
FOR_EACH_TYPE(Orderability)

// Only the types these four answer for. A pair they refuse throws out of the loop, which would
// abort the run rather than measure anything.
#define FOR_EACH_COMPARABLE_TYPE(bench) \
  SHAPE(bench, Int);                    \
  SHAPE(bench, Double);                 \
  SHAPE(bench, Date);                   \
  SHAPE(bench, String);                 \
  SHAPE(bench, IntAgainstDouble);

FOR_EACH_COMPARABLE_TYPE(Less)
FOR_EACH_COMPARABLE_TYPE(LessEqual)
FOR_EACH_COMPARABLE_TYPE(Greater)
FOR_EACH_COMPARABLE_TYPE(GreaterEqual)

// The container shapes the two sameness relations disagree about, which a sweep over scalars cannot
// reach.
#define FOR_EACH_CONTAINER(bench) \
  SHAPE(bench, Map);              \
  SHAPE(bench, MapHoldingNull);   \
  SHAPE(bench, ListHoldingNull);  \
  SHAPE(bench, NestedList);

FOR_EACH_CONTAINER(Equality)
FOR_EACH_CONTAINER(Equivalence)

SHAPE(Orderability, NaN);
SHAPE(Orderability, IntAgainstDouble);
BENCHMARK(SortColumn)->Unit(benchmark::kNanosecond);

#undef FOR_EACH_TYPE
#undef FOR_EACH_COMPARABLE_TYPE
#undef FOR_EACH_CONTAINER
#undef SHAPE

}  // namespace

BENCHMARK_MAIN();
