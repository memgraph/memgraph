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

// Guards that a bounded edge-index lookup stays O(log n) in the index size. The
// sweep fits per-lookup time against index size, so a regression to a full O(n)
// scan flips the reported complexity.

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"

namespace {

using memgraph::storage::Config;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::View;

// Builds a storage holding n edges of a single type, each with a distinct integer
// index property 0..n-1, and an edge-type+property index over them.
std::unique_ptr<InMemoryStorage> MakeIndexedEdges(int64_t n, EdgeTypeId &edge_type, PropertyId &prop) {
  Config config{};
  config.salient.items.properties_on_edges = true;
  auto storage = std::make_unique<InMemoryStorage>(config);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    edge_type = acc->NameToEdgeType("E");
    prop = acc->NameToProperty("id");
    for (int64_t i = 0; i < n; ++i) {
      auto from = acc->CreateVertex();
      auto to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&from, &to, edge_type);
      MG_ASSERT(edge.has_value());
      MG_ASSERT(edge->SetProperty(prop, PropertyValue(i)).has_value());
    }
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->ReadOnlyAccess();
    MG_ASSERT(acc->CreateIndex(edge_type, prop).has_value());
    MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  return storage;
}

// NOLINTNEXTLINE(google-runtime-references)
void EdgeTypePropertyIndexPointLookup(benchmark::State &state) {
  const int64_t n = state.range(0);
  EdgeTypeId edge_type;
  PropertyId prop;
  auto storage = MakeIndexedEdges(n, edge_type, prop);

  // Look up a value in the middle: reaching it needs the lower-bound seek (a head
  // scan would walk n/2 entries) and stopping after it needs the upper-bound
  // early exit (else the scan walks the other n/2). Either regression makes the
  // per-lookup cost O(n).
  const auto needle = PropertyValue(n / 2);
  auto acc = storage->Access(memgraph::storage::READ);

  for (auto _ : state) {
    int64_t found = 0;
    for (const auto &edge : acc->Edges(edge_type, prop, needle, View::OLD)) {
      found += static_cast<int64_t>(edge.Gid().AsUint());
    }
    benchmark::DoNotOptimize(found);
  }
  state.SetComplexityN(n);
}

BENCHMARK(EdgeTypePropertyIndexPointLookup)
    ->RangeMultiplier(2)
    ->Range(1 << 10, 1 << 18)
    ->Complexity(benchmark::oLogN)
    ->Unit(benchmark::kMicrosecond);

}  // namespace

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::off);
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
