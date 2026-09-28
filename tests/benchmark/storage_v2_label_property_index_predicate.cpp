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

// Guards that a scan reading a value predicate leaves a run of entries sharing the value the
// predicate read in one seek, rather than reading them one by one.
//
// Both sweeps hold the column size fixed and vary how many rows share a value, so the count of
// distinct values, and with it the count of entries a seeking scan reads, falls as the sweep
// advances. Every point reads the same column, so the rate reported against it is comparable
// across the sweep: a scan that reads every entry holds one rate throughout, and a scan that seeks
// past a run raises it as the runs lengthen.
//
// The predicate keeps nothing, so every point of the sweep hands back an empty result and the only
// work measured is rejecting. Keeping a value instead would hand back as many rows as share it,
// which grows as the sweep advances and buries what is being measured under the cost of resolving
// vertices.
//
// The first point of each sweep is the baseline to read the rest against. Every value there is
// distinct, so no run exists and no seek can fire, leaving the cost of reading the whole column
// entry by entry, with the one comparison the scan spends deciding not to seek. A point below that
// baseline is a seek that paid for itself, and a point above it is one that did not. Short runs sit
// above it: a seek descends the skip list, which buys nothing when the run it passes holds a
// handful of entries that a step would have walked more cheaply.

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

// The column every sweep point fills. Held fixed so that the rows sharing a value is the only thing
// that varies, and the scan has the same number of entries to get through each time.
constexpr int64_t kColumn = 1 << 14;

struct Indexed {
  std::unique_ptr<InMemoryStorage> storage;
  LabelId label;
  PropertyId leading;
  PropertyId trailing;
};

/// Fills a column of kColumn vertices where every @p shared of them carry the same trailing value,
/// under an index over @p properties.
///
/// The leading property holds one value throughout, so an equality bound on it admits the whole
/// column and the trailing property is what the scan ranges over. A single-property index ignores
/// the leading one and ranges over the trailing value directly.
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

  // The whole column, so that the rate is per row offered to the scan rather than per row it chose
  // to read, and points of the sweep can be read against one another.
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

// The predicate the index has always seeked on, measured the same way, so that the trailing sweep
// above has something to be read against rather than standing on its own.
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
