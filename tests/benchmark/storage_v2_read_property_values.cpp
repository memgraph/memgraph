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

// Compares the ways a fixed set of properties can be read per row: one at a time, through the
// map-returning set read, and into a buffer the caller owns. The middle one is a regression:
// the containers it allocates per row cost more than the separate reads it replaces.

#include <benchmark/benchmark.h>

#include <array>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/logging.hpp"

namespace {

using memgraph::storage::Config;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::View;

constexpr int64_t kVertices = 200000;

struct Fixture {
  std::unique_ptr<InMemoryStorage> storage;
  std::array<PropertyId, 3> props{};
};

Fixture MakeVertices() {
  Fixture f;
  f.storage = std::make_unique<InMemoryStorage>(Config{});
  auto acc = f.storage->Access(memgraph::storage::WRITE);
  f.props = {acc->NameToProperty("category"), acc->NameToProperty("value"), acc->NameToProperty("quantity")};
  for (int64_t i = 0; i < kVertices; ++i) {
    auto v = acc->CreateVertex();
    MG_ASSERT(v.SetProperty(f.props[0], PropertyValue("category string")).has_value());
    MG_ASSERT(v.SetProperty(f.props[1], PropertyValue(1.5 * static_cast<double>(i))).has_value());
    MG_ASSERT(v.SetProperty(f.props[2], PropertyValue(i)).has_value());
  }
  MG_ASSERT(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return f;
}

Fixture &Data() {
  static Fixture f = MakeVertices();
  return f;
}

void OneAtATime(benchmark::State &state) {
  auto &f = Data();
  for (auto _ : state) {
    auto acc = f.storage->Access(memgraph::storage::READ);
    for (auto v : acc->Vertices(View::NEW)) {
      for (auto prop : f.props) {
        auto value = v.GetProperty(prop, View::NEW);
        benchmark::DoNotOptimize(value);
      }
    }
  }
}

void ByPropertyIdsMap(benchmark::State &state) {
  auto &f = Data();
  for (auto _ : state) {
    auto acc = f.storage->Access(memgraph::storage::READ);
    for (auto v : acc->Vertices(View::NEW)) {
      auto values = v.PropertiesByPropertyIds(f.props, View::NEW);
      benchmark::DoNotOptimize(values);
    }
  }
}

void IntoCallerBuffer(benchmark::State &state) {
  auto &f = Data();
  std::vector<PropertyValue> buffer(f.props.size());
  for (auto _ : state) {
    auto acc = f.storage->Access(memgraph::storage::READ);
    for (auto v : acc->Vertices(View::NEW)) {
      auto ok = v.ReadPropertyValues(f.props, View::NEW, buffer);
      benchmark::DoNotOptimize(ok);
      benchmark::DoNotOptimize(buffer);
    }
  }
}

BENCHMARK(OneAtATime)->Unit(benchmark::kMillisecond);
BENCHMARK(ByPropertyIdsMap)->Unit(benchmark::kMillisecond);
BENCHMARK(IntoCallerBuffer)->Unit(benchmark::kMillisecond);

}  // namespace

BENCHMARK_MAIN();
