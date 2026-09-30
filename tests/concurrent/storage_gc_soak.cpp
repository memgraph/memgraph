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

// Soak tests for the garbage collector: sustained traffic against the collector rather than a named
// ordering. They assert only that nothing drifts once the traffic stops, so what they mainly buy is
// a workload to run under a sanitizer. The orderings themselves are pinned by cheap unit tests.

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/test_run_until.hpp"

using memgraph::storage::Config;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::StorageMode;
using memgraph::storage::View;

namespace {

struct Params {
  StorageMode mode;
  bool light_edges;
};

auto ParamName(testing::TestParamInfo<Params> const &info) -> std::string {
  auto const *mode = info.param.mode == StorageMode::IN_MEMORY_TRANSACTIONAL ? "Transactional" : "Analytical";
  return std::string{mode} + (info.param.light_edges ? "LightEdges" : "HeavyEdges");
}

class IndexEntryChurn : public testing::TestWithParam<Params> {
 protected:
  void SetUp() override {
    Config config{};
    config.salient.items.properties_on_edges = true;
    config.salient.items.storage_light_edge = GetParam().light_edges;
    // GC is driven by a thread the test owns, so a pass is never in flight for reasons it cannot see.
    config.gc.type = Config::Gc::Type::NONE;

    storage = std::make_unique<InMemoryStorage>(config);
    edge_type = storage->NameToEdgeType("E");
    property = storage->NameToProperty("p");
    label = storage->NameToLabel("L");

    // One accessor per index: a read-only accessor is spent by the DDL it carries.
    {
      auto acc = storage->ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(edge_type, property).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    {
      auto acc = storage->ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(label).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // The mode is set once the indexes exist, so both modes index the same way.
    storage->SetStorageMode(GetParam().mode);
  }

  // Collects twice: the first pass unlinks and marks, the second removes what that freed up.
  void Collect() {
    storage->FreeMemory({}, false);
    storage->FreeMemory({}, false);
  }

  auto EdgeIndexEntries() -> uint64_t {
    auto acc = storage->Access(memgraph::storage::READ);
    auto const entries = acc->ApproximateEdgeCount(edge_type, property);
    acc->Abort();
    return entries;
  }

  auto VertexIndexEntries() -> uint64_t {
    auto acc = storage->Access(memgraph::storage::READ);
    auto const entries = acc->ApproximateVertexCount(label);
    acc->Abort();
    return entries;
  }

  std::unique_ptr<InMemoryStorage> storage;
  EdgeTypeId edge_type{};
  PropertyId property{};
  LabelId label{};
};

}  // namespace

// Churn, aborts, scans, mid-flight index DDL and collection, all at once. Once it quiesces every
// entry must name something a scan can still reach; anything more is either an entry that outlived
// its object or a superseded duplicate no sweep collapsed.
TEST_P(IndexEntryChurn, ChurnUnderScansAndGcLeavesOnlyReachableEntries) {
  // DDL rounds are the slowest of the concurrent activities, so requiring a number of them puts a
  // floor under all the others: each round spans hundreds of transactions and dozens of GC passes.
  constexpr uint64_t kDdlRoundsRequired = 60;

  std::atomic<bool> running{true};
  // Read-only access for DDL cannot be taken while writers hold accessors, and four writers in a
  // tight loop never leave a gap, so they stand aside between transactions when DDL is waiting.
  std::atomic<bool> ddl_waiting{false};
  std::atomic<uint64_t> created{0};
  std::atomic<uint64_t> deleted{0};
  std::atomic<uint64_t> aborted{0};
  std::atomic<uint64_t> ddl_rounds{0};

  auto const writer = [&](int seed) {
    std::mt19937 rng{static_cast<std::mt19937::result_type>(seed)};
    std::vector<Gid> pending;

    while (running.load(std::memory_order_relaxed)) {
      while (ddl_waiting.load(std::memory_order_acquire) && running.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
      }
      {
        auto acc = storage->Access(memgraph::storage::WRITE);
        auto from = acc->CreateVertex();
        auto to = acc->CreateVertex();
        ASSERT_TRUE(from.AddLabel(label).has_value());
        auto edge = acc->CreateEdge(&from, &to, edge_type);
        ASSERT_TRUE(edge.has_value());
        ASSERT_TRUE(edge->SetProperty(property, PropertyValue{static_cast<int64_t>(rng() % 32)}).has_value());
        // A second write to the same property supersedes the first entry rather than replacing it;
        // only a sweep collapses the pair.
        ASSERT_TRUE(edge->SetProperty(property, PropertyValue{static_cast<int64_t>(rng() % 32)}).has_value());

        // Aborts hand their objects to the collector without those objects ever being visible.
        if (rng() % 8 == 0) {
          acc->Abort();
          aborted.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
        pending.push_back(from.Gid());
        created.fetch_add(1, std::memory_order_relaxed);
      }

      if (pending.size() < 8) continue;

      auto const gid = pending.front();
      pending.erase(pending.begin());
      {
        auto acc = storage->Access(memgraph::storage::WRITE);
        auto from = acc->FindVertex(gid, View::NEW);
        if (!from) {
          acc->Abort();
          continue;
        }
        // DETACH DELETE takes the vertex and its edge together, so both indexes have entries to
        // collect in the same pass.
        if (!acc->DetachDelete({&*from}, {}, true).has_value()) {
          acc->Abort();
          continue;
        }
        if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
          deleted.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  };

  auto const scanner = [&]() {
    while (running.load(std::memory_order_relaxed)) {
      auto acc = storage->Access(memgraph::storage::READ);
      for (auto edge : acc->Edges(edge_type, property, View::NEW)) {
        // Read through the entry the way a Filter would.
        auto const value = edge.GetProperty(property, View::NEW);
        (void)value;
      }
      for (auto vertex : acc->Vertices(label, View::NEW)) {
        auto const labels = vertex.Labels(View::NEW);
        (void)labels;
      }
      acc->Abort();
    }
  };

  // Index DDL mid-flight: a second index appears and disappears while the churn runs, so entries
  // are written into an index that is itself being installed and dropped.
  auto const ddl = [&]() {
    auto const other = storage->NameToProperty("q");
    while (running.load(std::memory_order_relaxed)) {
      ddl_waiting.store(true, std::memory_order_release);
      std::this_thread::sleep_for(std::chrono::milliseconds{2});
      {
        auto acc = storage->ReadOnlyAccess();
        if (acc->CreateIndex(edge_type, other).has_value()) {
          (void)acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
        } else {
          acc->Abort();
        }
      }
      {
        // Dropping takes read access, unlike creating, which needs the read-only kind.
        auto acc = storage->Access(memgraph::storage::READ);
        if (acc->DropIndex(edge_type, other).has_value()) {
          (void)acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
        } else {
          acc->Abort();
        }
      }
      ddl_waiting.store(false, std::memory_order_release);
      ddl_rounds.fetch_add(1, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    ddl_waiting.store(false, std::memory_order_release);
  };

  // Holds the oldest active start timestamp back in bursts, which is what decides both what a pass
  // may unlink and which entries its sweep is allowed to skip.
  auto const long_reader = [&]() {
    while (running.load(std::memory_order_relaxed)) {
      auto acc = storage->Access(memgraph::storage::READ);
      std::this_thread::sleep_for(std::chrono::milliseconds{25});
      acc->Abort();
    }
  };

  auto const collector = [&]() {
    while (running.load(std::memory_order_relaxed)) {
      storage->FreeMemory({}, false);
      std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
  };

  {
    std::vector<std::jthread> threads;
    for (int i = 0; i != 4; ++i) threads.emplace_back(writer, i + 1);
    threads.emplace_back(scanner);
    threads.emplace_back(scanner);
    threads.emplace_back(ddl);
    threads.emplace_back(long_reader);
    threads.emplace_back(collector);
    memgraph::tests::RunUntil(ddl_rounds, kDdlRoundsRequired, running);
  }

  Collect();

  uint64_t visible_edges = 0;
  uint64_t visible_vertices = 0;
  {
    auto acc = storage->Access(memgraph::storage::READ);
    for ([[maybe_unused]] auto edge : acc->Edges(edge_type, property, View::NEW)) ++visible_edges;
    for ([[maybe_unused]] auto vertex : acc->Vertices(label, View::NEW)) ++visible_vertices;
    acc->Abort();
  }

  auto const edge_entries = EdgeIndexEntries();
  auto const vertex_entries = VertexIndexEntries();

  spdlog::info(
      "created={} deleted={} aborted={} ddl_rounds={} visible_edges={} edge_entries={} "
      "visible_vertices={} vertex_entries={}",
      created.load(),
      deleted.load(),
      aborted.load(),
      ddl_rounds.load(),
      visible_edges,
      edge_entries,
      visible_vertices,
      vertex_entries);

  EXPECT_GT(deleted.load(), 0U) << "no delete ever committed: the churn never ran";
  EXPECT_EQ(edge_entries, visible_edges) << "the edge index holds entries no scan can reach";
  EXPECT_EQ(vertex_entries, visible_vertices) << "the label index holds entries no scan can reach";
}

INSTANTIATE_TEST_SUITE_P(StorageModes, IndexEntryChurn,
                         testing::Values(Params{StorageMode::IN_MEMORY_TRANSACTIONAL, false},
                                         Params{StorageMode::IN_MEMORY_TRANSACTIONAL, true},
                                         Params{StorageMode::IN_MEMORY_ANALYTICAL, false},
                                         Params{StorageMode::IN_MEMORY_ANALYTICAL, true}),
                         ParamName);

// Aborts on client threads while the collector sweeps the indexes those objects are in and scans
// walk them. Nothing leaves storage off the collector's thread, so what is left to check is that
// deferring loses nothing: the stores must come back to where they started.
TEST(ObjectRemovalSoak, AbortHeavyTrafficUnderConcurrentSweepsAndScans) {
  // Sweeps are the scarce event here, and requiring a number of them puts a floor under the abort
  // traffic each one has to run against: the aborters produce thousands per sweep.
  constexpr uint64_t kSweepsRequired = 500;

  Config config{};
  config.salient.items.properties_on_edges = true;
  config.gc.type = Config::Gc::Type::NONE;

  auto storage = std::make_unique<InMemoryStorage>(config);
  auto const edge_type = storage->NameToEdgeType("E");
  auto const property = storage->NameToProperty("p");
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(edge_type, property).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto const vertices_before = storage->VertexStoreSize();
  auto const edges_before = storage->EdgeStoreSize();

  std::atomic<bool> running{true};
  std::atomic<uint64_t> aborts{0};
  std::atomic<uint64_t> sweeps{0};

  auto const aborter = [&](int seed) {
    while (running.load(std::memory_order_relaxed)) {
      auto acc = storage->Access(memgraph::storage::WRITE);
      auto from = acc->CreateVertex();
      auto to = acc->CreateVertex();
      auto edge = acc->CreateEdge(&from, &to, edge_type);
      ASSERT_TRUE(edge.has_value());
      ASSERT_TRUE(edge->SetProperty(property, PropertyValue{static_cast<int64_t>(seed)}).has_value());
      acc->Abort();
      aborts.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto const scanner = [&]() {
    while (running.load(std::memory_order_relaxed)) {
      auto acc = storage->Access(memgraph::storage::READ);
      for (auto edge : acc->Edges(edge_type, property, View::NEW)) {
        auto const value = edge.GetProperty(property, View::NEW);
        (void)value;
      }
      acc->Abort();
    }
  };

  auto const collector = [&]() {
    while (running.load(std::memory_order_relaxed)) {
      storage->FreeMemory({}, false);
      sweeps.fetch_add(1, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
  };

  {
    std::vector<std::jthread> threads;
    for (int i = 0; i != 4; ++i) threads.emplace_back(aborter, i + 1);
    threads.emplace_back(scanner);
    threads.emplace_back(scanner);
    threads.emplace_back(collector);
    memgraph::tests::RunUntil(sweeps, kSweepsRequired, running);
  }

  storage->FreeMemory({}, false);
  storage->FreeMemory({}, false);

  EXPECT_GT(aborts.load(), 0U) << "no abort ever ran: the traffic never started";
  EXPECT_EQ(storage->VertexStoreSize(), vertices_before) << "deferred removals must all be collected, not leaked";
  EXPECT_EQ(storage->EdgeStoreSize(), edges_before) << "deferred removals must all be collected, not leaked";

  auto acc = storage->Access(memgraph::storage::READ);
  EXPECT_EQ(acc->ApproximateEdgeCount(edge_type, property), 0U) << "no entry may outlive the edge it names";
  acc->Abort();
}
