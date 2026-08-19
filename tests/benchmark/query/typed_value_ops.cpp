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
