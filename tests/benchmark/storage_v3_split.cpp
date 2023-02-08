// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <optional>
#include <vector>

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_id.hpp"

namespace memgraph::benchmark {

class ShardSplitBenchmark : public ::benchmark::Fixture {
 protected:
  using PrimaryKey = storage::v3::PrimaryKey;
  using PropertyId = storage::v3::PropertyId;
  using PropertyValue = storage::v3::PropertyValue;
  using LabelId = storage::v3::LabelId;
  using EdgeTypeId = storage::v3::EdgeTypeId;
  using Shard = storage::v3::Shard;
  using VertexId = storage::v3::VertexId;
  using Gid = storage::v3::Gid;

  void SetUp(const ::benchmark::State &state) override {
    storage.emplace(primary_label, min_pk, std::nullopt, schema_property_vector, last_hlc);
    storage->StoreMapping(
        {{1, "label"}, {2, "property"}, {3, "edge_property"}, {4, "secondary_label"}, {5, "secondary_prop"}});
  }

  void TearDown(const ::benchmark::State &) override { storage = std::nullopt; }

  const PropertyId primary_property{PropertyId::FromUint(2)};
  const PropertyId secondary_property{PropertyId::FromUint(5)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const LabelId secondary_label{LabelId::FromUint(4)};
  const EdgeTypeId edge_type_id{EdgeTypeId::FromUint(3)};
  std::optional<Shard> storage;

  coordinator::Hlc last_hlc{0, io::Time{}};

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(1);
    return last_hlc;
  }
};

BENCHMARK_DEFINE_F(ShardSplitBenchmark, BigDataSplit)(::benchmark::State &state) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, state.range(0));

  for (int64_t i{0}; i < state.range(0); ++i) {
    auto acc = storage->Access(GetNextHlc());
    MG_ASSERT(acc.CreateVertexAndValidate({secondary_label}, PrimaryKey{PropertyValue(i)},
                                          {{secondary_property, PropertyValue(i)}})
                  .HasValue(),
              "Failed creating with pk {}", i);
    if (i > 1) {
      const auto vtx1 = uniform_dist(e1) % i;
      const auto vtx2 = uniform_dist(e1) % i;

      MG_ASSERT(acc.CreateEdge(VertexId{primary_label, {PropertyValue(vtx1)}},
                               VertexId{primary_label, {PropertyValue(vtx2)}}, edge_type_id, Gid::FromUint(i))
                    .HasValue(),
                "Failed on {} and {}", vtx1, vtx2);
    }
    acc.Commit(GetNextHlc());
  }
  for (auto _ : state) {
    auto data = storage->PerformSplit(PrimaryKey{PropertyValue{state.range(0) / 2}}, last_hlc, GetNextHlc());
  }
}

BENCHMARK_DEFINE_F(ShardSplitBenchmark, BigDataSplitWithGc)(::benchmark::State &state) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, state.range(0));

  for (int64_t i{0}; i < state.range(0); ++i) {
    auto acc = storage->Access(GetNextHlc());
    MG_ASSERT(acc.CreateVertexAndValidate({secondary_label}, PrimaryKey{PropertyValue(i)},
                                          {{secondary_property, PropertyValue(i)}})
                  .HasValue(),
              "Failed creating with pk {}", i);
    if (i > 1) {
      const auto vtx1 = uniform_dist(e1) % i;
      const auto vtx2 = uniform_dist(e1) % i;

      MG_ASSERT(acc.CreateEdge(VertexId{primary_label, {PropertyValue(vtx1)}},
                               VertexId{primary_label, {PropertyValue(vtx2)}}, edge_type_id, Gid::FromUint(i))
                    .HasValue(),
                "Failed on {} and {}", vtx1, vtx2);
    }
    acc.Commit(GetNextHlc());
  }
  storage->CollectGarbage(GetNextHlc().coordinator_wall_clock);
  for (auto _ : state) {
    auto data = storage->PerformSplit(PrimaryKey{PropertyValue{state.range(0) / 2}}, last_hlc, GetNextHlc());
  }
}

BENCHMARK_DEFINE_F(ShardSplitBenchmark, BigDataSplitWithFewTransactions)(::benchmark::State &state) {
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, state.range(0));

  const auto max_transactions_needed = std::max(state.range(0), state.range(1));
  for (int64_t vertex_counter{state.range(0)}, edge_counter{state.range(1)}, i{0};
       vertex_counter > 0 || edge_counter > 0; --vertex_counter, --edge_counter, ++i) {
    auto acc = storage->Access(GetNextHlc());
    if (vertex_counter > 0) {
      MG_ASSERT(acc.CreateVertexAndValidate({secondary_label}, PrimaryKey{PropertyValue(i)},
                                            {{secondary_property, PropertyValue(i)}})
                    .HasValue(),
                "Failed creating with pk {}", i);
      ++i;
    }
    if (edge_counter > 0 && i > 1) {
      const auto vtx1 = uniform_dist(e1) % i;
      const auto vtx2 = uniform_dist(e1) % i;

      MG_ASSERT(acc.CreateEdge(VertexId{primary_label, {PropertyValue(vtx1)}},
                               VertexId{primary_label, {PropertyValue(vtx2)}}, edge_type_id, Gid::FromUint(i))
                    .HasValue(),
                "Failed on {} and {}", vtx1, vtx2);
    }

    acc.Commit(GetNextHlc());
    if (i >= max_transactions_needed - state.range(2)) {
      storage->CollectGarbage(GetNextHlc().coordinator_wall_clock);
    }
  }

  for (auto _ : state) {
    auto data = storage->PerformSplit(PrimaryKey{PropertyValue{state.range(0) / 2}}, last_hlc, GetNextHlc());
  }
}

// Range:
// Number of vertices
// This run is pessimistic, number of vertices corresponds with number if transactions
BENCHMARK_REGISTER_F(ShardSplitBenchmark, BigDataSplit)
    ->RangeMultiplier(10)
    ->Range(100'000, 1'000'000)
    ->Unit(::benchmark::kMillisecond);

// Range:
// Number of vertices
// This run is optimistic, in this run there are no transactions
BENCHMARK_REGISTER_F(ShardSplitBenchmark, BigDataSplitWithGc)
    ->RangeMultiplier(10)
    ->Range(100'000, 1'000'000)
    ->Unit(::benchmark::kMillisecond);

// Args:
// Number of vertices
// Number of edges
// Number of transaction
BENCHMARK_REGISTER_F(ShardSplitBenchmark, BigDataSplitWithFewTransactions)
    ->Args({100'000, 100'000, 1'000})
    ->Args({100'000, 100'000, 10'000})
    ->Args({1'000'000, 100'000, 1'000})
    ->Args({1'000'000, 100'000, 10'000})
    ->Args({100'000, 1'000'000, 1'000})
    ->Args({1'000'000, 1'00'000, 10'000})
    ->Unit(::benchmark::kMillisecond);

}  // namespace memgraph::benchmark

BENCHMARK_MAIN();
