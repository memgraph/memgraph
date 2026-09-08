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

// Nothing keeps an object alive on its index entry's behalf, so an entry left behind names freed
// memory. ApproximateEdgeCount/ApproximateVertexCount report the entry count, which is what makes
// that visible. Both storage modes and both edge representations must hold the ordering, though the
// paths that deliver it differ: transactional collects deleted objects once their deltas unlink,
// analytical from a full scan, and light edges from a graveyard rather than the edge skip-list.

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

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

class IndexEntryLifetime : public testing::TestWithParam<Params> {
 protected:
  void SetUp() override {
    Config config{};
    config.salient.items.properties_on_edges = true;
    config.salient.items.storage_light_edge = GetParam().light_edges;
    // GC is driven explicitly, so a pass is never in flight for reasons a test cannot see.
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

  // One labelled vertex with one indexed out-edge; returns the from-vertex gid.
  auto CreateIndexedPair() -> Gid {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    EXPECT_TRUE(from.AddLabel(label).has_value());
    auto edge = acc->CreateEdge(&from, &to, edge_type);
    EXPECT_TRUE(edge.has_value());
    EXPECT_TRUE(edge->SetProperty(property, PropertyValue{1}).has_value());
    auto const gid = from.Gid();
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    return gid;
  }

  void DetachDeleteVertex(Gid gid) {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::NEW);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(acc->DetachDelete({&*vertex}, {}, true).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::unique_ptr<InMemoryStorage> storage;
  EdgeTypeId edge_type{};
  PropertyId property{};
  LabelId label{};
};

}  // namespace

// A transaction held open fixes the oldest active start timestamp below every entry written after
// it, which is the condition the sweeps use to skip an entry as too young to touch. Whether
// skipping is safe depends on whether the object can be collected meanwhile, and the two modes
// answer that differently.
TEST_P(IndexEntryLifetime, DeletedObjectsLeaveNoEntriesWhileATransactionIsOpen) {
  auto holder = storage->Access(memgraph::storage::READ);

  auto const gid = CreateIndexedPair();
  ASSERT_EQ(EdgeIndexEntries(), 1U);
  ASSERT_EQ(VertexIndexEntries(), 1U);

  DetachDeleteVertex(gid);
  Collect();

  if (GetParam().mode == StorageMode::IN_MEMORY_ANALYTICAL) {
    // The delete landed in place and this pass removed the objects from storage, so an entry left
    // behind names memory that is already free.
    EXPECT_EQ(EdgeIndexEntries(), 0U) << "an entry outlived the edge it names";
    EXPECT_EQ(VertexIndexEntries(), 0U) << "an entry outlived the vertex it names";
  } else {
    // MVCC keeps the deleted objects until the open transaction retires, so their entries still
    // name something live and holding them is correct rather than a leak.
    EXPECT_EQ(EdgeIndexEntries(), 1U);
    EXPECT_EQ(VertexIndexEntries(), 1U);
  }

  // With nothing holding the collector back, both modes must reach the same end state.
  holder->Abort();
  Collect();

  EXPECT_EQ(EdgeIndexEntries(), 0U);
  EXPECT_EQ(VertexIndexEntries(), 0U);
}

// Control for the test above: the same sequence with nothing held open, so the skip condition is
// false. It is the only difference between the two, which is what makes it the cause.
TEST_P(IndexEntryLifetime, DeletedObjectsLeaveNoEntriesWithNothingHeldOpen) {
  auto const gid = CreateIndexedPair();
  ASSERT_EQ(EdgeIndexEntries(), 1U);

  DetachDeleteVertex(gid);
  Collect();

  EXPECT_EQ(EdgeIndexEntries(), 0U);
  EXPECT_EQ(VertexIndexEntries(), 0U);
}

INSTANTIATE_TEST_SUITE_P(StorageModes, IndexEntryLifetime,
                         testing::Values(Params{StorageMode::IN_MEMORY_TRANSACTIONAL, false},
                                         Params{StorageMode::IN_MEMORY_TRANSACTIONAL, true},
                                         Params{StorageMode::IN_MEMORY_ANALYTICAL, false},
                                         Params{StorageMode::IN_MEMORY_ANALYTICAL, true}),
                         ParamName);
