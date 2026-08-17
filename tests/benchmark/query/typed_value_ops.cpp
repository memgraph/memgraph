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
// the most frequently executed instructions in the engine. Copying and destroying are answered by
// asking what the value holds, and the types that answer "nothing to do" are grouped first so each
// question is one comparison.
//
// Which is why every type gets a case, not only the fast ones. The point of a benchmark here is not
// that a number gets faster: it is that making one type faster has not made another slower. A first
// attempt at this grouping tested membership of a bitmask, so a string paid for a test it always
// failed, and it cost several percent on exactly the queries that group by strings. That regression
// is invisible unless a string is measured beside an integer.

#include <benchmark/benchmark.h>

#include <string>
#include <vector>

#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace {

using memgraph::query::TypedValue;

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
void ConstructDestroy(benchmark::State &state, std::string_view what) {
  auto const source = Make(what);
  for (auto _ : state) {
    {
      auto copy = TypedValue{source, Mem()};
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

#undef FOR_EACH_TYPE

BENCHMARK(CompareNumbers)->Unit(benchmark::kNanosecond);
BENCHMARK(CompareStrings)->Unit(benchmark::kNanosecond);

}  // namespace

BENCHMARK_MAIN();
