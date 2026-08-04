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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <optional>
#include <set>
#include <string_view>
#include <thread>
#include <vector>

#include "flags/general.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/gc_status.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using testing::UnorderedElementsAre;

namespace ms = memgraph::storage;

// Forces a synchronous GC pass by handing FreeMemory a hold it must adopt rather than acquire.
inline auto UniqueGuard(memgraph::utils::ResourceLock &lock) {
  return memgraph::utils::ResourceLockGuard{lock, memgraph::utils::ResourceLockGuard::UNIQUE};
}

class StorageV2GcMetricsTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    db_name_ = testing::UnitTest::GetInstance()->current_test_info()->name();
    InitStorage(std::chrono::seconds(3600));
  }

  void TearDown() override {
    memgraph::metrics::Metrics().SetStorageSnapshotResolver({});
    storage.reset();
    memgraph::metrics::Metrics().RemoveDatabase(uuid_);
    handles_ = {};
    uuid_ = {};
    registered_ = false;
  }

  void InitStorage(std::chrono::milliseconds interval) {
    if (registered_) {
      memgraph::metrics::Metrics().SetStorageSnapshotResolver({});
      storage.reset();
      memgraph::metrics::Metrics().RemoveDatabase(uuid_);
      handles_ = {};
      uuid_ = {};
      registered_ = false;
    }
    memgraph::storage::Config config;
    config.salient.name = db_name_;
    config.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = interval};
    uuid_ = memgraph::utils::UUID{};
    handles_ = memgraph::metrics::Metrics().AddDatabase(uuid_, db_name_);
    registered_ = true;
    storage = std::make_unique<memgraph::storage::InMemoryStorage>(
        config, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(), handles_);
    memgraph::metrics::Metrics().SetStorageSnapshotResolver(
        [this](memgraph::utils::UUID const &uuid) -> std::optional<memgraph::metrics::StorageSnapshot> {
          if (uuid != uuid_ || !storage) return std::nullopt;
          auto const info = storage->GetBaseInfo();
          return memgraph::metrics::StorageSnapshot{
              .vertex_count = info.vertex_count,
              .edge_count = info.edge_count,
              .disk_usage = info.disk_usage,
          };
        });
  }

  std::unique_ptr<memgraph::storage::Storage> storage;
  memgraph::metrics::DatabaseMetricHandles handles_{};
  memgraph::utils::UUID uuid_{};
  bool registered_{false};

 private:
  std::string db_name_;
};

TEST(StorageV2GcStatus, PhaseToString) {
  using ms::GcPhase;
  EXPECT_EQ(ms::GcProgress::PhaseToString(GcPhase::IDLE), "idle");
  EXPECT_EQ(ms::GcProgress::PhaseToString(GcPhase::UNLINK), "unlink");
  EXPECT_EQ(ms::GcProgress::PhaseToString(GcPhase::INDEX_CLEANUP), "index_cleanup");
  EXPECT_EQ(ms::GcProgress::PhaseToString(GcPhase::DELETE), "delete");
}

// Start publishes run-state, SetPhase advances it, Reset clears every field.
// The Reset check is the regression guard: it once cleared only some fields.
TEST(StorageV2GcStatus, RunStateLifecycle) {
  ms::GcProgress gc;
  EXPECT_FALSE(gc.TryGetRunInfo().has_value());

  gc.Start(/*is_periodic=*/false, /*is_exclusive=*/true);
  auto info = gc.TryGetRunInfo();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->phase, ms::GcPhase::UNLINK);
  EXPECT_FALSE(info->periodic);
  EXPECT_TRUE(info->exclusive_lock);
  EXPECT_GT(info->start_time_us, 0);

  gc.SetPhase(ms::GcPhase::INDEX_CLEANUP);
  EXPECT_EQ(gc.TryGetRunInfo()->phase, ms::GcPhase::INDEX_CLEANUP);

  gc.Reset();
  EXPECT_FALSE(gc.TryGetRunInfo().has_value());
  EXPECT_EQ(gc.phase.load(), ms::GcPhase::IDLE);
  EXPECT_FALSE(gc.exclusive_lock.load());
  EXPECT_FALSE(gc.periodic.load());
  EXPECT_EQ(gc.start_time_us.load(), 0);
  EXPECT_EQ(gc.start_steady_ms.load(), 0);
}

// TODO: The point of these is not to test GC fully, these are just simple
// sanity checks. These will be superseded by a more sophisticated stress test
// which will verify that GC is working properly in a multithreaded environment.

// A simple test trying to get GC to run while a transaction is still alive and
// then verify that GC didn't delete anything it shouldn't have.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2Gc, Sanity) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}}));

  std::vector<memgraph::storage::Gid> vertices;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    // Create some vertices, but delete some of them immediately.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->CreateVertex();
      vertices.push_back(vertex.Gid());
    }

    acc->AdvanceCommand();

    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      if (i % 5 == 0) {
        EXPECT_FALSE(!acc->DeleteVertex(&vertex.value()).has_value());
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex_old = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      auto vertex_new = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_TRUE(vertex_old.has_value());
      EXPECT_EQ(vertex_new.has_value(), i % 5 != 0);
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Verify existing vertices and add labels to some of them.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);

      if (vertex.has_value()) {
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i)).has_value());
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i + 1)).has_value());
        EXPECT_FALSE(!vertex->AddLabel(memgraph::storage::LabelId::FromUint(3 * i + 2)).has_value());
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Verify labels.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);

      if (vertex.has_value()) {
        auto labels_old = vertex->Labels(memgraph::storage::View::OLD);
        EXPECT_TRUE(labels_old.has_value());
        EXPECT_TRUE(labels_old->empty());

        auto labels_new = vertex->Labels(memgraph::storage::View::NEW);
        EXPECT_TRUE(labels_new.has_value());
        EXPECT_THAT(labels_new.value(),
                    UnorderedElementsAre(memgraph::storage::LabelId::FromUint(3 * i),
                                         memgraph::storage::LabelId::FromUint(3 * i + 1),
                                         memgraph::storage::LabelId::FromUint(3 * i + 2)));
      }
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Add and remove some edges.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto from_vertex = acc->FindVertex(vertices[i], memgraph::storage::View::OLD);
      auto to_vertex = acc->FindVertex(vertices[(i + 1) % 1000], memgraph::storage::View::OLD);
      EXPECT_EQ(from_vertex.has_value(), i % 5 != 0);
      EXPECT_EQ(to_vertex.has_value(), (i + 1) % 5 != 0);

      if (from_vertex.has_value() && to_vertex.has_value()) {
        EXPECT_FALSE(
            !acc->CreateEdge(&from_vertex.value(), &to_vertex.value(), memgraph::storage::EdgeTypeId::FromUint(i))
                 .has_value());
      }
    }

    // Detach delete some vertices.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0);
      if (vertex.has_value()) {
        if (i % 3 == 0) {
          EXPECT_FALSE(!acc->DetachDeleteVertex(&vertex.value()).has_value());
        }
      }
    }

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Vertify edges.
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc->FindVertex(vertices[i], memgraph::storage::View::NEW);
      EXPECT_EQ(vertex.has_value(), i % 5 != 0 && i % 3 != 0);
      if (vertex.has_value()) {
        auto out_edges = vertex->OutEdges(memgraph::storage::View::NEW)->edges;
        if (i % 5 != 4 && i % 3 != 2) {
          EXPECT_EQ(out_edges.size(), 1);
          EXPECT_EQ(*vertex->OutDegree(memgraph::storage::View::NEW), 1);
          EXPECT_EQ(out_edges.at(0).EdgeType().AsUint(), i);
        } else {
          EXPECT_TRUE(out_edges.empty());
        }

        auto in_edges = vertex->InEdges(memgraph::storage::View::NEW)->edges;
        if (i % 5 != 1 && i % 3 != 1) {
          EXPECT_EQ(in_edges.size(), 1);
          EXPECT_EQ(*vertex->InDegree(memgraph::storage::View::NEW), 1);
          EXPECT_EQ(in_edges.at(0).EdgeType().AsUint(), (i + 999) % 1000);
        } else {
          EXPECT_TRUE(in_edges.empty());
        }
      }
    }

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// A simple sanity check for index GC:
// 1. Start transaction 0, create some vertices, add a label to them and
//    commit.
// 2. Start transaction 1.
// 3. Start transaction 2, remove the labels and commit;
// 4. Wait for GC. GC shouldn't remove the vertices from index because
//    transaction 1 can still see them with that label.
// NOLINTNEXTLINE(hicpp-special-member-functions)
// The index-cleanup sweep visits every entry of an index rather than only the stale ones, so
// what a collection cycle costs is governed by how many indexes it visits. Nothing else reports
// that: the sweep latency says how long it took, not how much of it was worth doing.
class StorageV2GcIndexSweepCountTest : public StorageV2GcMetricsTest {
 protected:
  // A pass that adopts a hold it is handed, so it runs here rather than on the collection
  // thread, and the count it produces belongs to a known set of writes.
  uint64_t SweptByOnePass() {
    auto const before = handles_.gc_index_sweeps.Value();
    auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(storage.get());
    mem_storage->FreeMemory(UniqueGuard(storage->main_lock_), false);
    return static_cast<uint64_t>(handles_.gc_index_sweeps.Value() - before);
  }

  void CreateLabelIndex(std::string_view name) {
    auto acc = storage->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(storage->NameToLabel(name)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  void CreateLabelPropertyIndex(std::string_view label, std::string_view property) {
    auto acc = storage->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(storage->NameToLabel(label),
                                 {memgraph::storage::PropertyPath{storage->NameToProperty(property)}})
                    .has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
};

TEST_F(StorageV2GcIndexSweepCountTest, IdleDatabaseSweepsNothing) {
  CreateLabelIndex("A");
  CreateLabelIndex("B");

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The write above says the vertex indexes may hold something to collect, so this pass sweeps.
  EXPECT_GT(SweptByOnePass(), 0);

  // Nothing has been written since, so there is nothing to look for.
  EXPECT_EQ(SweptByOnePass(), 0);
}

TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenLabelsIndexIsSwept) {
  CreateLabelIndex("A");
  CreateLabelIndex("B");

  memgraph::storage::Gid gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  // A write naming one label only. The index on the other label cannot have gained anything to
  // collect, and walking it would cost its whole size to find that out.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(*vertex->AddLabel(acc->NameToLabel("B")));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// A deleted vertex leaves index entries pointing at memory about to be freed, and no delta says
// which indexes hold them. Narrowing must not reach this case.
TEST_F(StorageV2GcIndexSweepCountTest, ADeletedVertexSweepsEveryVertexIndex) {
  CreateLabelIndex("A");
  CreateLabelIndex("B");

  memgraph::storage::Gid gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(acc->DeleteVertex(&*vertex).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 2);
}

// The saving must not come at the cost of leaving entries behind: an index whose label was
// written is swept, and what it holds afterwards is what it would have held before this change.
TEST_F(StorageV2GcIndexSweepCountTest, TheSweptIndexStillCollectsItsStaleEntries) {
  CreateLabelIndex("A");
  CreateLabelIndex("B");

  constexpr int kVertices = 100;
  std::vector<memgraph::storage::Gid> gids;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (int i = 0; i != kVertices; ++i) {
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("A")));
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("B")));
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  auto const indexed_count = [&](std::string_view name) {
    auto acc = storage->Access(memgraph::storage::READ);
    auto const count = acc->ApproximateVertexCount(storage->NameToLabel(name));
    acc->Abort();
    return count;
  };
  ASSERT_EQ(indexed_count("A"), kVertices);
  ASSERT_EQ(indexed_count("B"), kVertices);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (auto const gid : gids) {
      auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(*vertex->RemoveLabel(acc->NameToLabel("A")));
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_EQ(SweptByOnePass(), 1);
  EXPECT_EQ(indexed_count("A"), 0);
  EXPECT_EQ(indexed_count("B"), kVertices);
}

TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysIndexIsSwept) {
  CreateLabelPropertyIndex("L", "a");
  CreateLabelPropertyIndex("L", "b");

  memgraph::storage::Gid gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
    ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("b"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{2}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// A property write says the vertex indexes may hold something to collect but a delta cannot say
// which property it was without being read for it. Were the write to arm the family and name no
// property, every label-property index would be skipped and the entries it left would stay.
TEST_F(StorageV2GcIndexSweepCountTest, APropertyWriteStillCollectsTheEntriesItStaled) {
  CreateLabelPropertyIndex("L", "a");
  CreateLabelPropertyIndex("L", "b");

  constexpr int kVertices = 100;
  std::vector<memgraph::storage::Gid> gids;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (int i = 0; i != kVertices; ++i) {
      auto vertex = acc->CreateVertex();
      gids.push_back(vertex.Gid());
      ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
      ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{i}).has_value());
      ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("b"), memgraph::storage::PropertyValue{i}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  auto const indexed_count = [&](std::string_view property) {
    auto acc = storage->Access(memgraph::storage::READ);
    auto const count = acc->ApproximateVertexCount(
        storage->NameToLabel("L"), std::array{memgraph::storage::PropertyPath{storage->NameToProperty(property)}});
    acc->Abort();
    return count;
  };
  ASSERT_EQ(indexed_count("a"), kVertices);

  // Rewriting a property leaves the entry holding the old value behind for the sweep to find.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    for (auto const gid : gids) {
      auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(vertex->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{-1}).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_EQ(indexed_count("a"), 2 * kVertices);

  EXPECT_EQ(SweptByOnePass(), 1);
  EXPECT_EQ(indexed_count("a"), kVertices);
  EXPECT_EQ(indexed_count("b"), kVertices);
}

// A unique constraint keeps a skiplist keyed the way an index is and is swept the same way, so a
// write that cannot have staled it should not cost its whole size to walk either.
TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysConstraintIsSwept) {
  auto const create_constraint = [&](std::string_view property) {
    auto acc = storage->UniqueAccess();
    auto const result = acc->CreateUniqueConstraint(storage->NameToLabel("L"), {storage->NameToProperty(property)});
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  create_constraint("a");
  create_constraint("b");

  memgraph::storage::Gid gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    gid = vertex.Gid();
    ASSERT_TRUE(*vertex.AddLabel(acc->NameToLabel("L")));
    ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("b"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{2}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 1);

  // The constraint left unswept still holds: its entries were never stale, only unvisited.
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto other = acc->CreateVertex();
    ASSERT_TRUE(*other.AddLabel(acc->NameToLabel("L")));
    ASSERT_TRUE(other.SetProperty(acc->NameToProperty("b"), memgraph::storage::PropertyValue{1}).has_value());
    EXPECT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

TEST_F(StorageV2GcIndexSweepCountTest, OnlyTheWrittenPropertysEdgeIndexIsSwept) {
  auto const create_edge_index = [&](std::string_view property) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(storage->NameToEdgeType("E"), storage->NameToProperty(property)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  create_edge_index("a");
  create_edge_index("b");

  memgraph::storage::Gid edge_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("b"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{2}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 1);
}

// A removed edge leaves entries pointing at memory about to be freed, and its delta carries the
// edge type rather than the property any index is keyed on, so nothing names them.
TEST_F(StorageV2GcIndexSweepCountTest, ARemovedEdgeSweepsEveryEdgeIndex) {
  auto const create_edge_index = [&](std::string_view property) {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(storage->NameToEdgeType("E"), storage->NameToProperty(property)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  create_edge_index("a");
  create_edge_index("b");

  memgraph::storage::Gid edge_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("E"));
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("a"), memgraph::storage::PropertyValue{1}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(SweptByOnePass(), 0);

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto edge = acc->FindEdge(edge_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(acc->DeleteEdge(&*edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(SweptByOnePass(), 2);
}

TEST(StorageV2Gc, Indices) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}}));
  {
    auto unique_acc = storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(storage->NameToLabel("label")).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    for (uint64_t i = 0; i < 1000; ++i) {
      auto vertex = acc0->CreateVertex();
      ASSERT_TRUE(*vertex.AddLabel(acc0->NameToLabel("label")));
    }
    ASSERT_TRUE(acc0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc1 = storage->Access(memgraph::storage::WRITE);

    auto acc2 = storage->Access(memgraph::storage::WRITE);
    for (auto vertex : acc2->Vertices(memgraph::storage::View::OLD)) {
      ASSERT_TRUE(*vertex.RemoveLabel(acc2->NameToLabel("label")));
    }
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::set<memgraph::storage::Gid> gids;
    for (auto vertex : acc1->Vertices(acc1->NameToLabel("label"), memgraph::storage::View::OLD)) {
      gids.insert(vertex.Gid());
    }
    EXPECT_EQ(gids.size(), 1000);
  }
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithCommittedContributorsAreGarbagedCollected) {
  // Need a periodic garbage collector so that certain fast path optimisations
  // aren't taken when cleaning deltas, but with an inter-collection pause large
  // enough that we can step through when debugging, etc, without anything being
  // unexpectedly reclaimed from underneath us.

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    // The 6 unreleased deltas are:
    // - 2 x CREATE_OBJECT, to create the edges
    // - 2 x ADD_IN_EDGE
    // - 2 x ADD_OUT_EDGE
    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());

    // Commit in order `acc1` and `acc2`. This means that even though `acc2` does
    // have non-sequential deltas, everything downstream from them is committed
    // and so garbage collection can skip the waiting list.
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());
  }

  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithAbortedContributorsAreGarbagedCollected) {
  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());

    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();
    acc1->Abort();
    acc1.reset();
    acc0.reset();

    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());
  }

  // First GC: moves `waiting_gc_deltas_` to `aborted_transactions_` or
  // `committed_transactions_`
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Second GC: committed deltas are unlinked, and all deltas (be they committed
  // or aborted) move to `garbage_undo_buffers_` for reclamation.
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithMultipleAbortsAreGarbageCollected) {
  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());

    // Both transactions abort - all deltas should be cleaned up
    acc2->Abort();
    acc2.reset();
    acc1->Abort();

    acc1.reset();
    acc0.reset();

    ASSERT_EQ(6, handles_.unreleased_delta_objects.Value());
  }

  // First GC: moves from waiting_gc_deltas_ to aborted_transactions_
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Second GC: moves to garbage_undo_buffers_
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  // Third GC: frees the deltas
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, DownstreamDeltaChainsAreGarbageCollected) {
  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    // Create three edges to form downstream delta chain: TX1 -> TX2 -> TX3
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).has_value());

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());

    // Commit TX1, abort TX2 and TX3
    // TX3's deltas are downstream from TX2, which are downstream from TX1
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    acc3->Abort();
    acc3.reset();
    acc2->Abort();
    acc2.reset();
    acc0.reset();

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());
  }

  // Multiple GC cycles to process the downstream chain
  for (int i = 0; i < 4; ++i) {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, MixedCommitAbortCommitNonSequentialDeltasAreGarbageCollected) {
  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    // Create non-sequential deltas with mixed commit/abort pattern
    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3")).has_value());

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());

    // TX1 commits, TX2 aborts, TX3 commits
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    acc2->Abort();
    acc2.reset();
    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc3.reset();
    acc0.reset();

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());
  }

  // Multiple GC cycles to handle mixed commit/abort
  for (int i = 0; i < 4; ++i) {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithTwoContributorsAreGarbagedCollected) {
  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto acc3 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    auto edge1_result = acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1"));
    ASSERT_TRUE(edge1_result.has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    auto edge2_result = acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2"));
    ASSERT_TRUE(edge2_result.has_value());

    auto v1_t3 = acc3->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t3 = acc3->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    auto edge3_result = acc3->CreateEdge(&*v1_t3, &*v2_t3, acc3->NameToEdgeType("Edge3"));
    ASSERT_TRUE(edge3_result.has_value());

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());

    ASSERT_TRUE(acc3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc3.reset();
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    ASSERT_EQ(9, handles_.unreleased_delta_objects.Value());
  }

  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->FreeMemory(std::move(main_guard), false);
  }

  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithUncommittedContributorsAreGarbagedCollected) {
  // Use periodic GC with short interval to automatically test intermediate states
  InitStorage(std::chrono::milliseconds(100));

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).has_value());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, handles_.unreleased_delta_objects.Value());

    // When acc2 commits, its transaction has non-sequential deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc2 committed but acc1 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc1
    ASSERT_EQ(12, handles_.unreleased_delta_objects.Value());

    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST_F(StorageV2GcMetricsTest, NonSequentialDeltasWithUncommittedContributorsAreGarbagedCollected_SwapCommitOrder) {
  InitStorage(std::chrono::milliseconds(100));

  memgraph::storage::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc0 = storage->Access(memgraph::storage::WRITE);  // older accessor is just used to stop GC.
    auto acc1 = storage->Access(memgraph::storage::WRITE);
    auto acc2 = storage->Access(memgraph::storage::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge3")).has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc1->NameToEdgeType("Edge4")).has_value());

    // The 12 unreleased deltas are:
    // - 4 x CREATE_OBJECT, to create the edges
    // - 4 x ADD_IN_EDGE
    // - 4 x ADD_OUT_EDGE
    ASSERT_EQ(12, handles_.unreleased_delta_objects.Value());

    // When acc1 commits first, its transaction has non-sequential deltas which are
    // uncommitted, meaning these deltas must sit in the waiting list until
    // all contributors have committed.
    ASSERT_TRUE(acc1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc1.reset();

    // wait for GC to clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // At this point acc1 committed but acc2 hasn't - deltas should stay in waiting list
    // Wait for GC to run but deltas should remain due to uncommitted acc2
    ASSERT_EQ(12, handles_.unreleased_delta_objects.Value());

    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(0, handles_.unreleased_delta_objects.Value());
}

TEST(StorageV2Gc, ConcurrentEdgeOperationsAbortDeleteRepeat) {
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)}});

  memgraph::storage::Gid v1_gid, v2_gid;

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto tx2 = storage->Access(memgraph::storage::WRITE);

  {
    auto v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx1->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx1->CreateEdge(&v1, &v2, tx1->NameToEdgeType("Edge1")).has_value());
  }

  {
    auto v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::OLD).value();
    auto v2 = tx2->FindVertex(v2_gid, memgraph::storage::View::OLD).value();
    ASSERT_TRUE(tx2->CreateEdge(&v1, &v2, tx2->NameToEdgeType("Edge2")).has_value());
  }

  tx1->Abort();
  tx2->Abort();

  // Wait for GC to clean up
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  {
    auto reader = storage->Access(memgraph::storage::WRITE);
    auto v1 = reader->FindVertex(v1_gid, memgraph::storage::View::OLD);
    auto v2 = reader->FindVertex(v2_gid, memgraph::storage::View::OLD);

    if (v1.has_value()) {
      auto edges = v1->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.has_value());
    }
    if (v2.has_value()) {
      auto edges = v2->OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(edges.has_value());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2Gc, RapidGcOutOfOrderCommitTimestamps) {
  memgraph::storage::Gid vertex_gid;

  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{
          .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}}));

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    vertex_gid = vertex.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // blocker transaction prevents rapid GC
  auto blocker = storage->Access(memgraph::storage::WRITE);

  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_a;
  {
    accessor_a = storage->Access(memgraph::storage::WRITE);
    auto v = accessor_a->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(accessor_a->NameToLabel("LabelA")).has_value());
    ASSERT_TRUE(accessor_a->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto accessor_b = storage->Access(memgraph::storage::WRITE);
    auto v = accessor_b->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(accessor_b->NameToLabel("LabelB")).has_value());

    ASSERT_TRUE(accessor_b->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  accessor_a.reset();

  // Destroy blocker to allow rapid GC
  blocker.reset();

  {
    auto gc_trigger = storage->Access(memgraph::storage::WRITE);
    gc_trigger->CreateVertex();
    ASSERT_TRUE(gc_trigger->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto reader = storage->Access(memgraph::storage::WRITE);
    auto v = reader->FindVertex(vertex_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());

    auto labels = v->Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(labels.has_value());
    EXPECT_EQ(labels->size(), 2);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v1)-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithWithNoNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());

    ASSERT_TRUE(tx1->CreateEdge(&*v1, &*v2, tx1->NameToEdgeType("EDGE1")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1, &*v2, tx1->NameToEdgeType("EDGE2")).has_value());

    ASSERT_NE(v1->vertex_->delta(), nullptr);
    ASSERT_EQ(v1->vertex_->out_edges.size(), 2);

    tx1->Abort();
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 0);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 0);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

//==============================================================================
// Following tests all peek at internal state of vertices before, during, or
// after Abort(). As such, they "bypass" isolation and visibility to directly
// assert what we expect in terms of deltas and vertex state.

// Tests state after aborting tx2 when we have the following chain:
// (v1)-[tx2 intr]-[tx2 intr]-[tx1]
TEST(StorageV2Gc, AbortsWhenDeltasAreNonSequential) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto const *delta_v1_t1_head = v1_t1->vertex_->delta();

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE3")).has_value());

    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 3);

    tx2->Abort();

    ASSERT_EQ(v1_t1->vertex_->delta(), delta_v1_t1_head);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithUpstreamNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto *delta_v1_t2 = v1_t2->vertex_->delta();
    ASSERT_NE(delta_v1_t2, nullptr);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 3);

    tx1->Abort();

    EXPECT_EQ(v1_t2->vertex_->delta(), delta_v1_t2);
    EXPECT_EQ(v1_t2->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx2 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx2 intr]-[tx1]
TEST(StorageV2Gc, AbortsWithUpstreamAndDownstreamNonSequentialDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2A")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2B")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    auto *delta_v1_t3 = v1_t3->vertex_->delta();
    ASSERT_NE(delta_v1_t3, nullptr);
    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx2->Abort();

    EXPECT_EQ(v1_t3->vertex_->delta(), delta_v1_t3);
    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx2 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx2 intr]-[tx1 committed]
TEST(StorageV2Gc, AbortsWithCommittedDownstreamDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());
    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2A")).has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2B")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    auto *delta_v1_t3 = v1_t3->vertex_->delta();
    ASSERT_NE(delta_v1_t3, nullptr);
    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx2->Abort();

    EXPECT_EQ(v1_t3->vertex_->delta(), delta_v1_t3);
    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsAtEndOfNonSequentialChain) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto *delta_v1_t2 = v1_t2->vertex_->delta();
    ASSERT_NE(delta_v1_t2, nullptr);
    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 3);

    tx1->Abort();

    EXPECT_EQ(v1_t2->vertex_->delta(), delta_v1_t2);
    EXPECT_EQ(v1_t2->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    EXPECT_EQ(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 when we have the following chain:
// (v)-[tx3 intr]-[tx2 intr]-[tx1]-[tx1]
TEST(StorageV2Gc, AbortsWithMultipleTransactionsPrepending) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    ASSERT_EQ(v1_t1->vertex_->out_edges.size(), 4);

    tx1->Abort();

    EXPECT_EQ(v1_t3->vertex_->out_edges.size(), 2);

    ASSERT_TRUE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 2);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 2);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

// Tests state after aborting tx1 and tx2 when we have the following chain:
// (v)-[tx2 intr]-[tx1 intr]-[tx1 intr]-[tx0]
TEST(StorageV2Gc, AbortsTwoTransactions) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1_t0 = tx0->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t0 = tx0->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t0.has_value() && v2_t0.has_value());
    ASSERT_TRUE(tx0->CreateEdge(&*v1_t0, &*v2_t0, tx0->NameToEdgeType("EDGE0")).has_value());

    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1A")).has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1B")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    ASSERT_EQ(v1_t2->vertex_->out_edges.size(), 4);

    tx1->Abort();
    tx2->Abort();

    EXPECT_EQ(v1_t0->vertex_->out_edges.size(), 1);

    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    auto v2 = acc->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value() && v2.has_value());
    EXPECT_EQ(v1->vertex_->out_edges.size(), 1);
    EXPECT_EQ(v2->vertex_->in_edges.size(), 1);
    EXPECT_NE(v1->vertex_->delta(), nullptr);
    EXPECT_NE(v2->vertex_->delta(), nullptr);
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagRemainsAfterAbort) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagRemainsAfterPartialAbort) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    auto tx3 = storage->Access(memgraph::storage::WRITE);
    auto v1_t3 = tx3->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t3 = tx3->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t3.has_value() && v2_t3.has_value());
    ASSERT_TRUE(tx3->CreateEdge(&*v1_t3, &*v2_t3, tx3->NameToEdgeType("EDGE3")).has_value());

    {
      auto const guard = std::shared_lock{v1_t3->vertex_->lock};
      ASSERT_TRUE(v1_t3->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();

    {
      auto const guard = std::shared_lock{v1_t3->vertex_->lock};
      ASSERT_TRUE(v1_t3->vertex_->has_uncommitted_non_sequential_deltas());
    }

    ASSERT_TRUE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(tx3->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}

TEST(StorageV2Gc, HasNonSequentialDeltasFlagClearedWhenAllDeltasRemoved) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto tx0 = storage->Access(memgraph::storage::WRITE);
    auto v1 = tx0->CreateVertex();
    auto v2 = tx0->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(tx0->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto tx1 = storage->Access(memgraph::storage::WRITE);
    auto v1_t1 = tx1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = tx1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(tx1->CreateEdge(&*v1_t1, &*v2_t1, tx1->NameToEdgeType("EDGE1")).has_value());

    auto tx2 = storage->Access(memgraph::storage::WRITE);
    auto v1_t2 = tx2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = tx2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(tx2->CreateEdge(&*v1_t2, &*v2_t2, tx2->NameToEdgeType("EDGE2")).has_value());

    {
      auto const guard = std::shared_lock{v1_t2->vertex_->lock};
      ASSERT_TRUE(v1_t2->vertex_->has_uncommitted_non_sequential_deltas());
    }

    tx2->Abort();
    tx1->Abort();
  }

  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(v1->vertex_->delta(), nullptr);
    auto const guard = std::shared_lock{v1->vertex_->lock};
    ASSERT_FALSE(v1->vertex_->has_uncommitted_non_sequential_deltas());
  }
}

TEST(StorageV2Gc, ClearDrainsWaitingGcDeltas) {
  auto storage = std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)}});

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    v1_gid = acc->CreateVertex().Gid();
    v2_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    // Populate the garbage collector's waiting_gc_deltas_ by creating
    // non-sequential deltas, whilst also prohibiting fast GC collection.
    auto acc0 = storage->Access(ms::WRITE);
    auto acc1 = storage->Access(ms::WRITE);
    auto acc2 = storage->Access(ms::WRITE);

    auto v1_t1 = acc1->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t1 = acc1->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t1.has_value() && v2_t1.has_value());
    ASSERT_TRUE(acc1->CreateEdge(&*v1_t1, &*v2_t1, acc1->NameToEdgeType("Edge1")).has_value());

    auto v1_t2 = acc2->FindVertex(v1_gid, ms::View::OLD);
    auto v2_t2 = acc2->FindVertex(v2_gid, ms::View::OLD);
    ASSERT_TRUE(v1_t2.has_value() && v2_t2.has_value());
    ASSERT_TRUE(acc2->CreateEdge(&*v1_t2, &*v2_t2, acc2->NameToEdgeType("Edge2")).has_value());

    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    acc2.reset();
    acc1->Abort();
    acc1.reset();
  }

  // Clear should discard entire storage state, including pending
  // non-sequential deltas in the waiting_gc_deltas_ list.
  {
    auto main_guard = UniqueGuard(storage->main_lock_);
    storage->Clear();
  }

  // Subsequent commit triggers FastDiscardOfDeltas, which walks
  // waiting_gc_deltas_. If any uncleared state lingers after Clear(), this
  // will flag an error in ASAN.
  {
    auto acc = storage->Access(ms::WRITE);
    acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
}

// Light-edge GC routing tests (storage_light_edge=true).

namespace {
auto MakeLightEdgeGcStorage() {
  return std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(100)},
                 .salient = {.items = {.properties_on_edges = true, .storage_light_edge = true}}});
}
}  // namespace

// Mirrors StorageV2Gc/Sanity: GC running while a transaction is still alive must not free live data.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2GcLightEdge, Sanity) {
  auto storage = MakeLightEdgeGcStorage();

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("e")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Reader keeps an open transaction.
  auto reader = storage->Access(memgraph::storage::WRITE);

  {
    // Delete the edge in a second transaction while reader is alive.
    auto writer = storage->Access(memgraph::storage::WRITE);
    auto v1 = writer->FindVertex(v1_gid, ms::View::OLD).value();
    auto edges = v1.OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 1U);
    auto edge = edges->edges[0];
    ASSERT_TRUE(writer->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Let GC run while reader is still alive — it must NOT free the edge yet.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Reader still sees the edge via View::OLD.
  {
    auto v1 = reader->FindVertex(v1_gid, ms::View::OLD).value();
    auto edges = v1.OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 1U);
  }

  reader->Abort();

  // After the reader closes, GC eventually collects the graveyard. A fresh
  // transaction must no longer see the deleted edge.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  {
    auto checker = storage->Access(memgraph::storage::WRITE);
    auto v1 = checker->FindVertex(v1_gid, ms::View::NEW).value();
    auto edges = v1.OutEdges(ms::View::NEW);
    ASSERT_TRUE(edges.has_value());
    EXPECT_EQ(edges->edges.size(), 0U) << "deleted light edge must no longer be visible";
  }
}

// Mirrors StorageV2Gc/ConcurrentEdgeOperationsAbortDeleteRepeat:
// two transactions each create an edge, both abort; GC must not crash.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2GcLightEdge, ConcurrentEdgeOperationsAbortDeleteRepeat) {
  auto storage = MakeLightEdgeGcStorage();

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto tx1 = storage->Access(memgraph::storage::WRITE);
  auto tx2 = storage->Access(memgraph::storage::WRITE);

  {
    auto v1 = tx1->FindVertex(v1_gid, ms::View::OLD).value();
    auto v2 = tx1->FindVertex(v2_gid, ms::View::OLD).value();
    ASSERT_TRUE(tx1->CreateEdge(&v1, &v2, tx1->NameToEdgeType("Edge1")).has_value());
  }
  {
    auto v1 = tx2->FindVertex(v1_gid, ms::View::OLD).value();
    auto v2 = tx2->FindVertex(v2_gid, ms::View::OLD).value();
    ASSERT_TRUE(tx2->CreateEdge(&v1, &v2, tx2->NameToEdgeType("Edge2")).has_value());
  }

  tx1->Abort();
  tx2->Abort();

  // GC runs; aborted light-edge creations must have been freed inline (not via graveyard).
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  {
    auto reader = storage->Access(memgraph::storage::WRITE);
    auto v1 = reader->FindVertex(v1_gid, ms::View::OLD);
    if (v1.has_value()) {
      auto edges = v1->OutEdges(ms::View::OLD);
      ASSERT_TRUE(edges.has_value());
      ASSERT_EQ(edges->edges.size(), 0U);
    }
  }
}

// Mirrors StorageV2Gc/Indices: index GC correctness with light edges.
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2GcLightEdge, Indices) {
  auto storage = MakeLightEdgeGcStorage();

  {
    auto unique_acc = storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateIndex(storage->NameToLabel("label")).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    ASSERT_TRUE(*v1.AddLabel(acc->NameToLabel("label")));
    ASSERT_TRUE(*v2.AddLabel(acc->NameToLabel("label")));
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    ASSERT_TRUE(acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("e")).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc1 = storage->Access(memgraph::storage::WRITE);

    auto acc2 = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc2->FindVertex(v1_gid, ms::View::OLD).value();
    auto v2 = acc2->FindVertex(v2_gid, ms::View::OLD).value();
    auto edges = v1.OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 1U);
    auto edge = edges->edges[0];
    ASSERT_TRUE(acc2->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(*v1.RemoveLabel(acc2->NameToLabel("label")));
    ASSERT_TRUE(*v2.RemoveLabel(acc2->NameToLabel("label")));
    ASSERT_TRUE(acc2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    // GC runs while acc1 still holds a snapshot — must not collect index entries visible to acc1.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::set<ms::Gid> gids;
    for (auto vertex : acc1->Vertices(acc1->NameToLabel("label"), ms::View::OLD)) {
      gids.insert(vertex.Gid());
    }
    EXPECT_EQ(gids.size(), 2U);
  }
}

// Regression test: light edges deleted in analytical mode must be put in the graveyard
// and freed by GC, not leaked.  Verifies that:
//   - the storage destructor does not crash (no double-free / use-after-free)
//   - ~Edge() is called (catches PropertyStore heap-allocation leak under ASan)
//   - graveyard is drained correctly after mode switch back to transactional
// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(StorageV2GcLightEdge, AnalyticalModeDeleteGoesToGraveyard) {
  auto storage = MakeLightEdgeGcStorage();
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());

  ms::Gid v1_gid, v2_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    v1_gid = v1.Gid();
    v2_gid = v2.Gid();
    auto edge_res = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("e"));
    ASSERT_TRUE(edge_res.has_value());
    // Set a property so the PropertyStore has a heap allocation — caught by ASan if ~Edge() is skipped.
    ASSERT_TRUE(edge_res->SetProperty(acc->NameToProperty("key"), ms::PropertyValue{"value"}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Switch to analytical mode and delete the edge there.
  mem_storage->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto edges = v1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    ASSERT_EQ(edges->edges.size(), 1U);
    auto edge = edges->edges[0];
    ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Switch back — this triggers a FreeMemory/GC pass which must not crash.
  mem_storage->SetStorageMode(ms::StorageMode::IN_MEMORY_TRANSACTIONAL);

  // Run GC explicitly a second time to drain the graveyard.
  {
    auto main_guard = UniqueGuard(mem_storage->main_lock_);
    mem_storage->FreeMemory(std::move(main_guard), false);
  }

  // Verify the edge is gone and the two vertices are still intact.
  {
    auto acc = storage->Access(memgraph::storage::READ);
    auto v1 = acc->FindVertex(v1_gid, ms::View::OLD);
    ASSERT_TRUE(v1.has_value());
    auto edges = v1->OutEdges(ms::View::OLD);
    ASSERT_TRUE(edges.has_value());
    EXPECT_TRUE(edges->edges.empty());
    EXPECT_TRUE(acc->FindVertex(v2_gid, ms::View::OLD).has_value());
  }
  // storage destructor runs here — must not crash or report sanitizer errors.
}

// ASan smoke test for the FastDiscard-vs-GC delta-buffer use-after-free.
//
// Background: when a transaction commits it is marked finished (advancing
// commit_log_->OldestActive()) and only afterwards registers its deltas for GC.
// A concurrent transaction that commits sole-active in that window can
// fast-discard and free a delta a just-finished-but-unregistered transaction's
// chain still references via `prev`, which a later CollectGarbage dereferences.
//
// This exercises exactly that shape: several threads delete edges off a single
// shared vertex (so their commits chain deltas on the same object) while a GC
// thread hammers FreeMemory(). It is intentionally a NON-deterministic smoke
// test: the trigger window is a two-statement gap on the committing thread, so
// reproduction depends on scheduling pressure and the fault is only observable
// under a sanitizer (the free and the read are serialized by gc_lock_, so this
// is a use-after-free, not a data race -> ASan catches it, TSan does not). In
// CI it runs under the ASAN+UBSAN unit-coverage build (ctest -R memgraph__unit).
// It cannot guarantee a hit on every run; a deterministic reproduction would
// require a test-only hook in FinalizeTransaction, which we deliberately avoid.
TEST(StorageV2Gc, ConcurrentDeleteAndGcUAFSmoke) {
  constexpr int kRounds = 30;
  constexpr int kEdges = 100;
  constexpr int kDeleteThreads = 4;

  std::unique_ptr<ms::Storage> store(std::make_unique<ms::InMemoryStorage>(
      ms::Config{.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(1000)}}));

  ms::Gid gid_from{};
  ms::Gid gid_to{};
  {
    auto acc = store->Access(ms::WRITE);
    gid_from = acc->CreateVertex().Gid();
    gid_to = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  for (int round = 0; round < kRounds; ++round) {
    {
      auto acc = store->Access(ms::WRITE);
      auto vf = acc->FindVertex(gid_from, ms::View::NEW);
      auto vt = acc->FindVertex(gid_to, ms::View::NEW);
      auto et = acc->NameToEdgeType("CONC");
      for (int i = 0; i < kEdges; ++i) {
        ASSERT_TRUE(acc->CreateEdge(&*vf, &*vt, et).has_value());
      }
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    std::atomic<bool> gc_running{true};
    std::atomic<uint64_t> total_deleted{0};
    std::thread gc_thread([&]() {
      while (gc_running.load()) {
        store->FreeMemory();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      store->FreeMemory();
      store->FreeMemory();
    });

    std::vector<std::thread> deleters;
    deleters.reserve(kDeleteThreads);
    for (int t = 0; t < kDeleteThreads; ++t) {
      deleters.emplace_back([&]() {
        while (true) {
          auto acc = store->Access(ms::WRITE);
          auto vf = acc->FindVertex(gid_from, ms::View::NEW);
          if (!vf) break;
          auto out = vf->OutEdges(ms::View::NEW);
          if (!out.has_value() || out->edges.empty()) break;
          auto edge = out->edges[0];
          auto del = acc->DeleteEdge(&edge);
          if (!del.has_value()) continue;  // serialization conflict, retry
          if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
            total_deleted.fetch_add(1);
          }
        }
      });
    }

    for (auto &d : deleters) d.join();
    gc_running.store(false);
    gc_thread.join();

    // Functional check: every edge was deleted exactly once.
    EXPECT_EQ(total_deleted.load(), kEdges);
    auto acc = store->Access(ms::WRITE);
    auto vf = acc->FindVertex(gid_from, ms::View::OLD);
    ASSERT_TRUE(vf.has_value());
    EXPECT_EQ(vf->OutEdges(ms::View::OLD)->edges.size(), 0);
  }
}
