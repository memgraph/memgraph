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

// What a query value costs to copy, move, destroy and compare, one case per type it can hold.
//
// A row of expression evaluation builds and destroys a value per node it walks, so these are among
// the most frequently executed instructions in the engine. What each of them costs depends on what
// the value holds: a type owning an allocation and a type owning nothing take different work.
//
// Which is why every type gets a case, not only the ones expected to gain. A change that treats the
// types differently pays for telling them apart, and that cost lands on the types that do not gain.
// The measurement to protect is therefore not that one number falls, but that no other rises, and
// that is invisible unless a string is measured beside an integer.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "query/relations/equivalence.hpp"
#include "query/relations/orderability.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace {

using memgraph::query::TypedValue;
namespace relations = memgraph::query::relations;

memgraph::utils::MemoryResource *Mem() { return memgraph::utils::NewDeleteResource(); }

TypedValue Make(std::string_view what) {
  if (what == "Int") return TypedValue{int64_t{7'000'000}, Mem()};
  if (what == "Double") return TypedValue{33.14907, Mem()};
  if (what == "Bool") return TypedValue{true, Mem()};
  if (what == "String") return TypedValue{std::string{"region_3"}, Mem()};
  if (what == "Date") return TypedValue{memgraph::utils::Date{std::chrono::microseconds{86'400'000'000}}, Mem()};
  if (what == "List") {
    auto items = std::vector<TypedValue>{};
    for (int i = 0; i != 8; ++i) items.emplace_back(int64_t{i}, Mem());
    return TypedValue{std::move(items), Mem()};
  }
  MG_ASSERT(false, "unknown type name");
  return TypedValue{Mem()};
}

TypedValue MakeMap(bool with_null) {
  auto entries = std::map<std::string, TypedValue>{};
  for (int i = 0; i != 4; ++i) entries.emplace("k" + std::to_string(i), TypedValue{int64_t{i}, Mem()});
  if (with_null) entries.emplace("n", TypedValue{Mem()});
  return TypedValue{std::move(entries), Mem()};
}

TypedValue MakeList(bool with_null) {
  auto items = std::vector<TypedValue>{};
  for (int i = 0; i != 8; ++i) items.emplace_back(int64_t{i}, Mem());
  if (with_null) items.emplace_back(Mem());
  return TypedValue{std::move(items), Mem()};
}

TypedValue MakeNestedList() {
  auto outer = std::vector<TypedValue>{};
  for (int i = 0; i != 4; ++i) outer.emplace_back(MakeList(false));
  return TypedValue{std::move(outer), Mem()};
}

// Copying is where a type that owns an allocation parts company with one that does not.
void CopyConstruct(benchmark::State &state, std::string_view what) {
  auto const source = Make(what);
  for (auto _ : state) {
    auto copy = TypedValue{source, Mem()};
    benchmark::DoNotOptimize(copy);
  }
  state.SetItemsProcessed(state.iterations());
}

// A frame write: move a value into a slot that already holds one. Both the type being overwritten
// and the type arriving decide whether this can be done where it stands.
void MoveAssign(benchmark::State &state, std::string_view what) {
  auto const source = Make(what);
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
void ConstructDestroy(benchmark::State &state, std::string_view what) {
  auto const source = Make(what);
  for (auto _ : state) {
    {
      auto copy = TypedValue{source, Mem()};
      benchmark::DoNotOptimize(copy);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}

// `a > b` between two numbers, the comparison a filter runs once per row.
void CompareNumbers(benchmark::State &state) {
  auto const lhs = TypedValue{500.0, Mem()};
  auto const rhs = TypedValue{int64_t{499}, Mem()};
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs > rhs);
    benchmark::DoNotOptimize(lhs < rhs);
    benchmark::DoNotOptimize(lhs >= rhs);
    benchmark::DoNotOptimize(lhs <= rhs);
  }
  state.SetItemsProcessed(state.iterations() * 4);
}

// The same between two strings, which take the general path and must not be slowed by anything done
// for the numbers.
void CompareStrings(benchmark::State &state) {
  auto const lhs = TypedValue{std::string{"region_3"}, Mem()};
  auto const rhs = TypedValue{std::string{"region_4"}, Mem()};
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs > rhs);
    benchmark::DoNotOptimize(lhs < rhs);
    benchmark::DoNotOptimize(lhs >= rhs);
    benchmark::DoNotOptimize(lhs <= rhs);
  }
  state.SetItemsProcessed(state.iterations() * 4);
}

// `a > b` between two values of unlike types, which answers Null. Reaching that answer means
// establishing that each type belongs to the ordering at all, and a string is the type where asking
// that question the wrong way costs its whole length.
void CompareUnlikeTypes(benchmark::State &state) {
  auto const lhs = TypedValue{std::string{"a fairly long region name to compare against"}, Mem()};
  auto const rhs = TypedValue{int64_t{499}, Mem()};
  for (auto _ : state) {
    benchmark::DoNotOptimize(lhs > rhs);
    benchmark::DoNotOptimize(lhs < rhs);
    benchmark::DoNotOptimize(lhs >= rhs);
    benchmark::DoNotOptimize(lhs <= rhs);
  }
  state.SetItemsProcessed(state.iterations() * 4);
}

// The four ordered comparisons apart from one another. Measuring them together lets a change that
// speeds one up and slows another down read as no change at all.
void Greater(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs > rhs);
  state.SetItemsProcessed(state.iterations());
}

void GreaterEqual(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs >= rhs);
  state.SetItemsProcessed(state.iterations());
}

void Less(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs < rhs);
  state.SetItemsProcessed(state.iterations());
}

void LessEqual(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs <= rhs);
  state.SetItemsProcessed(state.iterations());
}

// Equality, the relation behind `=` and `IN`.
void Equality(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs == rhs);
  state.SetItemsProcessed(state.iterations());
}

// Equivalence, the relation behind DISTINCT and grouping, and the hottest of the three: a hash set
// of query values calls it on every probe that reaches a populated bucket.
void Equivalence(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  auto const eq = memgraph::query::relations::equivalence::KeyEqual{};
  for (auto _ : state) benchmark::DoNotOptimize(eq(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// The container shapes the two relations disagree about, which the scalar sweep above cannot reach.
void EquivalenceOfMaps(benchmark::State &state, bool with_null) {
  auto const lhs = MakeMap(with_null);
  auto const rhs = MakeMap(with_null);
  auto const eq = memgraph::query::relations::equivalence::KeyEqual{};
  for (auto _ : state) benchmark::DoNotOptimize(eq(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

void EquivalenceOfNestedLists(benchmark::State &state) {
  auto const lhs = MakeNestedList();
  auto const rhs = MakeNestedList();
  auto const eq = memgraph::query::relations::equivalence::KeyEqual{};
  for (auto _ : state) benchmark::DoNotOptimize(eq(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

void EqualityOfMaps(benchmark::State &state, bool with_null) {
  auto const lhs = MakeMap(with_null);
  auto const rhs = MakeMap(with_null);
  for (auto _ : state) benchmark::DoNotOptimize(lhs == rhs);
  state.SetItemsProcessed(state.iterations());
}

void EqualityOfListsWithNull(benchmark::State &state) {
  auto const lhs = MakeList(true);
  auto const rhs = MakeList(true);
  for (auto _ : state) benchmark::DoNotOptimize(lhs == rhs);
  state.SetItemsProcessed(state.iterations());
}

// Orderability, the relation ORDER BY, min and max read. A sort calls it once per comparison, which
// is the granularity that decides where it may be defined.
void Orderability(benchmark::State &state, std::string_view what) {
  auto const lhs = Make(what);
  auto const rhs = Make(what);
  for (auto _ : state) benchmark::DoNotOptimize(relations::orderability::Compare(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// The pair only this relation admits, where the answer comes from where the two types sit rather
// than from either value.
void OrderabilityOfUnlikeTypes(benchmark::State &state) {
  auto const lhs = Make("String");
  auto const rhs = Make("Int");
  for (auto _ : state) benchmark::DoNotOptimize(relations::orderability::Compare(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// The placement comparability leaves undone, and the one path where the ordering does more work
// than a plain three-way comparison.
void OrderabilityOfNaN(benchmark::State &state) {
  auto const lhs = TypedValue{std::numeric_limits<double>::quiet_NaN(), Mem()};
  auto const rhs = TypedValue{1.0, Mem()};
  for (auto _ : state) benchmark::DoNotOptimize(relations::orderability::Compare(lhs, rhs));
  state.SetItemsProcessed(state.iterations());
}

// What the relation exists for. Sorting is where its cost is actually paid, and a per-comparison
// figure alone hides how often a sort calls it.
void SortMixedColumn(benchmark::State &state) {
  auto const source = std::vector<TypedValue>{Make("Int"),
                                              Make("String"),
                                              Make("Bool"),
                                              Make("List"),
                                              Make("Double"),
                                              TypedValue{Mem()},
                                              Make("Date"),
                                              Make("Int"),
                                              Make("String"),
                                              Make("Double"),
                                              Make("Bool"),
                                              Make("List")};
  for (auto _ : state) {
    auto values = source;
    std::ranges::sort(values, [](TypedValue const &a, TypedValue const &b) {
      return std::is_lt(relations::orderability::Compare(a, b));
    });
    benchmark::DoNotOptimize(values);
  }
  state.SetItemsProcessed(state.iterations() * source.size());
}

#define FOR_EACH_TYPE(op)                                                \
  BENCHMARK_CAPTURE(op, Int, "Int")->Unit(benchmark::kNanosecond);       \
  BENCHMARK_CAPTURE(op, Double, "Double")->Unit(benchmark::kNanosecond); \
  BENCHMARK_CAPTURE(op, Bool, "Bool")->Unit(benchmark::kNanosecond);     \
  BENCHMARK_CAPTURE(op, Date, "Date")->Unit(benchmark::kNanosecond);     \
  BENCHMARK_CAPTURE(op, String, "String")->Unit(benchmark::kNanosecond); \
  BENCHMARK_CAPTURE(op, List, "List")->Unit(benchmark::kNanosecond);

FOR_EACH_TYPE(CopyConstruct)
FOR_EACH_TYPE(MoveAssign)
FOR_EACH_TYPE(ConstructDestroy)

// Only the types the ordering admits; the rest raise rather than compare.
#define FOR_EACH_ORDERED_TYPE(op)                                        \
  BENCHMARK_CAPTURE(op, Int, "Int")->Unit(benchmark::kNanosecond);       \
  BENCHMARK_CAPTURE(op, Double, "Double")->Unit(benchmark::kNanosecond); \
  BENCHMARK_CAPTURE(op, Date, "Date")->Unit(benchmark::kNanosecond);     \
  BENCHMARK_CAPTURE(op, String, "String")->Unit(benchmark::kNanosecond);

FOR_EACH_ORDERED_TYPE(Greater)
FOR_EACH_ORDERED_TYPE(GreaterEqual)
FOR_EACH_ORDERED_TYPE(Less)
FOR_EACH_ORDERED_TYPE(LessEqual)
FOR_EACH_TYPE(Equality)
FOR_EACH_TYPE(Equivalence)

BENCHMARK_CAPTURE(EquivalenceOfMaps, Plain, false)->Unit(benchmark::kNanosecond);
BENCHMARK_CAPTURE(EquivalenceOfMaps, WithNull, true)->Unit(benchmark::kNanosecond);
BENCHMARK(EquivalenceOfNestedLists)->Unit(benchmark::kNanosecond);
BENCHMARK_CAPTURE(EqualityOfMaps, Plain, false)->Unit(benchmark::kNanosecond);
BENCHMARK_CAPTURE(EqualityOfMaps, WithNull, true)->Unit(benchmark::kNanosecond);
BENCHMARK(EqualityOfListsWithNull)->Unit(benchmark::kNanosecond);
FOR_EACH_TYPE(Orderability)
BENCHMARK(OrderabilityOfUnlikeTypes)->Unit(benchmark::kNanosecond);
BENCHMARK(OrderabilityOfNaN)->Unit(benchmark::kNanosecond);
BENCHMARK(SortMixedColumn)->Unit(benchmark::kNanosecond);

#undef FOR_EACH_TYPE
#undef FOR_EACH_ORDERED_TYPE

BENCHMARK(CompareNumbers)->Unit(benchmark::kNanosecond);
BENCHMARK(CompareStrings)->Unit(benchmark::kNanosecond);
BENCHMARK(CompareUnlikeTypes)->Unit(benchmark::kNanosecond);

}  // namespace

BENCHMARK_MAIN();
