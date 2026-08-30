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

// The two readings of the order a stored value is kept in.
//
// An index descends a skip list by the decoded ordering and confirms the entry
// it lands on by the encoded one, which reads the bytes without decoding them.
// The two must answer alike, and neither had a benchmark: a change to the
// ordering that makes a comparison cost more is paid on every descent, and
// nothing measured it.
//
// Every type gets a case so that a change paying to tell types apart shows up
// on the types that do not gain by it. The pairs differ where a real key would:
// comparing a value against itself would let a length check or an early
// equality test stand in for the comparison being measured.

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

namespace {

using memgraph::storage::PropertyId;
using memgraph::storage::PropertyStore;
using memgraph::storage::PropertyValue;
using memgraph::storage::TemporalData;
using memgraph::storage::TemporalType;

PropertyValue ListOf(std::vector<int64_t> const &numbers) {
  auto elements = std::vector<PropertyValue>{};
  elements.reserve(numbers.size());
  for (auto const number : numbers) elements.emplace_back(number);
  return PropertyValue{elements};
}

PropertyValue ListOfStrings(std::vector<std::string> const &values) {
  auto elements = std::vector<PropertyValue>{};
  elements.reserve(values.size());
  for (auto const &value : values) elements.emplace_back(value);
  return PropertyValue{elements};
}

/// A pair of one type, differing in what they hold.
struct Pair {
  PropertyValue lhs;
  PropertyValue rhs;
};

Pair Made(std::string_view what) {
  if (what == "Int") return {PropertyValue{int64_t{7}}, PropertyValue{int64_t{9}}};
  if (what == "Double") return {PropertyValue{1.5}, PropertyValue{2.5}};
  if (what == "Bool") return {PropertyValue{false}, PropertyValue{true}};
  if (what == "String") return {PropertyValue{"alpha"}, PropertyValue{"bravo"}};
  if (what == "LongString") {
    return {PropertyValue{std::string(64, 'a') + "1"}, PropertyValue{std::string(64, 'a') + "2"}};
  }
  if (what == "Temporal") {
    return {PropertyValue{TemporalData{TemporalType::Date, 10}}, PropertyValue{TemporalData{TemporalType::Date, 20}}};
  }
  // Lists of one length, so the comparison has to walk to the element that
  // separates them rather than settling on the lengths.
  if (what == "List") return {ListOf({1, 2, 3, 4}), ListOf({1, 2, 3, 5})};
  // Lists of unlike lengths, which the ordering settles on the elements before
  // it reaches the lengths.
  if (what == "ListOfUnlikeLengths") return {ListOf({1, 2}), ListOf({1, 2, 3, 4})};
  if (what == "ListOfStrings") return {ListOfStrings({"a", "b"}), ListOfStrings({"a", "c"})};
  // Unlike types are separated by where the types sit, without either value
  // being looked at.
  if (what == "UnlikeTypes") return {PropertyValue{int64_t{7}}, PropertyValue{"seven"}};
  std::abort();
}

// The ordering an index keeps its entries in.
void Ordering(benchmark::State &state, std::string_view what) {
  auto const [lhs, rhs] = Made(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs <=> rhs);
  state.SetItemsProcessed(state.iterations());
}

// The sameness that ordering is read off, which a lookup asks of a candidate.
void Sameness(benchmark::State &state, std::string_view what) {
  auto const [lhs, rhs] = Made(what);
  for (auto _ : state) benchmark::DoNotOptimize(lhs == rhs);
  state.SetItemsProcessed(state.iterations());
}

// The same question asked of the bytes, without decoding them, which is what
// confirms the entry a scan has landed on.
void EncodedSameness(benchmark::State &state, std::string_view what) {
  auto const [lhs, rhs] = Made(what);
  auto const property = PropertyId::FromUint(1);
  auto store = PropertyStore{};
  store.SetProperty(property, lhs);
  for (auto _ : state) benchmark::DoNotOptimize(store.IsPropertyEqual(property, rhs));
  state.SetItemsProcessed(state.iterations());
}

// Answering about the value a store already holds, which is the case a lookup
// takes when the candidate is the one it wanted.
void EncodedSamenessHit(benchmark::State &state, std::string_view what) {
  auto const [lhs, rhs] = Made(what);
  auto const property = PropertyId::FromUint(1);
  auto store = PropertyStore{};
  store.SetProperty(property, lhs);
  for (auto _ : state) benchmark::DoNotOptimize(store.IsPropertyEqual(property, lhs));
  state.SetItemsProcessed(state.iterations());
}

#define FOR_EACH_SHAPE(bench)                                                                         \
  BENCHMARK_CAPTURE(bench, Int, "Int")->Unit(benchmark::kNanosecond);                                 \
  BENCHMARK_CAPTURE(bench, Double, "Double")->Unit(benchmark::kNanosecond);                           \
  BENCHMARK_CAPTURE(bench, Bool, "Bool")->Unit(benchmark::kNanosecond);                               \
  BENCHMARK_CAPTURE(bench, String, "String")->Unit(benchmark::kNanosecond);                           \
  BENCHMARK_CAPTURE(bench, LongString, "LongString")->Unit(benchmark::kNanosecond);                   \
  BENCHMARK_CAPTURE(bench, Temporal, "Temporal")->Unit(benchmark::kNanosecond);                       \
  BENCHMARK_CAPTURE(bench, List, "List")->Unit(benchmark::kNanosecond);                               \
  BENCHMARK_CAPTURE(bench, ListOfUnlikeLengths, "ListOfUnlikeLengths")->Unit(benchmark::kNanosecond); \
  BENCHMARK_CAPTURE(bench, ListOfStrings, "ListOfStrings")->Unit(benchmark::kNanosecond);             \
  BENCHMARK_CAPTURE(bench, UnlikeTypes, "UnlikeTypes")->Unit(benchmark::kNanosecond);

FOR_EACH_SHAPE(Ordering)
FOR_EACH_SHAPE(Sameness)
FOR_EACH_SHAPE(EncodedSameness)
FOR_EACH_SHAPE(EncodedSamenessHit)

}  // namespace

BENCHMARK_MAIN();
