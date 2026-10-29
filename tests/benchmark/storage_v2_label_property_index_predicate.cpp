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

// Checks that a scan skips a run of entries its value predicate rejects with one seek, instead of
// stepping through them.
//
// The column size is fixed; the argument is how many rows share each value. Items/s is per column
// row, so it stays flat if the scan steps through every entry and rises as runs lengthen if it
// seeks.
//
// The predicate rejects everything, so only rejection is measured, not vertex resolution.
//
// Argument 1 is the baseline: all values distinct, so no seek fires. Short runs can be slower than
// it, since a skip-list seek costs more than a few steps.

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"

namespace {

using memgraph::storage::Config;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyPath;
using memgraph::storage::PropertyValue;
using memgraph::storage::PropertyValueRange;
using memgraph::storage::View;

// Rows per column, the same at every sweep point.
constexpr int64_t kColumn = 1 << 14;

struct Indexed {
  std::unique_ptr<InMemoryStorage> storage;
  LabelId label;
  PropertyId leading;
  PropertyId trailing;
};

/// kColumn vertices; each run of @p shared vertices has one trailing value. @p properties is 2 for
/// an index on (leading, trailing), 1 for an index on trailing only. The leading value is the
/// same everywhere, so an equality on it matches the whole column.
Indexed MakeColumn(int64_t shared, std::size_t properties) {
  auto indexed = Indexed{.storage = std::make_unique<InMemoryStorage>(Config{})};

  {
    auto acc = indexed.storage->Access(memgraph::storage::WRITE);
    indexed.label = acc->NameToLabel("L");
    indexed.leading = acc->NameToProperty("a");
    indexed.trailing = acc->NameToProperty("b");

    for (int64_t at = 0; at != kColumn; ++at) {
      auto vertex = acc->CreateVertex();
      MG_ASSERT(vertex.AddLabel(indexed.label).has_value());
      MG_ASSERT(vertex.SetProperty(indexed.leading, PropertyValue(int64_t{1})).has_value());
      MG_ASSERT(vertex.SetProperty(indexed.trailing, PropertyValue(at / shared)).has_value());
    }
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = indexed.storage->ReadOnlyAccess();
    auto paths = properties == 2 ? std::vector{PropertyPath{indexed.leading}, PropertyPath{indexed.trailing}}
                                 : std::vector{PropertyPath{indexed.trailing}};
    MG_ASSERT(acc->CreateIndex(indexed.label, paths).has_value());
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  return indexed;
}

PropertyValueRange KeepsNothing() {
  auto range = PropertyValueRange::IsNotNull();
  range.SetValuePredicate(
      std::make_shared<PropertyValueRange::ValuePredicateFn const>([](PropertyValue const &) { return false; }));
  return range;
}

void Measure(benchmark::State &state, Indexed const &indexed, std::span<PropertyPath const> props,
             std::span<PropertyValueRange const> ranges) {
  auto acc = indexed.storage->Access(memgraph::storage::READ);

  for (auto _ : state) {
    auto found = int64_t{0};
    auto iterable = acc->Vertices(indexed.label, props, ranges, View::OLD);
    for (auto it = iterable.begin(); it != iterable.end(); ++it) ++found;
    benchmark::DoNotOptimize(found);
  }

  // Per column row, not per entry read, so sweep points are comparable.
  state.SetItemsProcessed(state.iterations() * kColumn);
  state.counters["distinct"] = static_cast<double>(kColumn / state.range(0));
}

// NOLINTNEXTLINE(google-runtime-references)
void TrailingPredicate(benchmark::State &state) {
  auto const shared = state.range(0);
  auto const indexed = MakeColumn(shared, 2);

  auto const props = std::array{PropertyPath{indexed.leading}, PropertyPath{indexed.trailing}};
  auto const ranges = std::array{PropertyValueRange::Bounded(memgraph::utils::MakeBoundInclusive(PropertyValue(1)),
                                                             memgraph::utils::MakeBoundInclusive(PropertyValue(1))),
                                 KeepsNothing()};
  Measure(state, indexed, props, ranges);
}

// The leading-property case, which seeked before this change: the reference for TrailingPredicate.
// NOLINTNEXTLINE(google-runtime-references)
void LeadingPredicate(benchmark::State &state) {
  auto const shared = state.range(0);
  auto const indexed = MakeColumn(shared, 1);

  auto const props = std::array{PropertyPath{indexed.trailing}};
  auto const ranges = std::array{KeepsNothing()};
  Measure(state, indexed, props, ranges);
}

BENCHMARK(TrailingPredicate)->RangeMultiplier(4)->Range(1, 1 << 10)->Unit(benchmark::kMicrosecond);

BENCHMARK(LeadingPredicate)->RangeMultiplier(4)->Range(1, 1 << 10)->Unit(benchmark::kMicrosecond);

}  // namespace

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::off);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
