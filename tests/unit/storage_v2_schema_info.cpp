// Copyright 2024 Memgraph Ltd.
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
#include <filesystem>

#include "dbms/constants.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;
constexpr auto testSuite = "storage_v2_schema_info";
const std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / testSuite};

struct InMemTransactional {};
struct InMemAnalytical {};

template <typename StorageType>
class SchemaInfoTest : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(storage_directory);
    config_.salient.name = memgraph::dbms::kDefaultDB;
    memgraph::storage::UpdatePaths(config_, storage_directory);
    config_.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;

    // TODO OnDisk no supported at this time
    this->storage = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
  }

  void TearDown() override {
    std::filesystem::remove_all(storage_directory);
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  StorageMode mode{std::is_same_v<StorageType, DiskStorage>
                       ? StorageMode::ON_DISK_TRANSACTIONAL
                       : (std::is_same_v<StorageType, InMemTransactional> ? StorageMode::IN_MEMORY_TRANSACTIONAL
                                                                          : StorageMode::IN_MEMORY_ANALYTICAL)};
};

template <typename StorageType>
class SchemaInfoTestWEdgeProp : public testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(storage_directory);
    config_.salient.name = memgraph::dbms::kDefaultDB;
    memgraph::storage::UpdatePaths(config_, storage_directory);
    config_.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config_.salient.items.properties_on_edges = true;

    // TODO OnDisk no supported at this time
    this->storage = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
  }

  void TearDown() override {
    std::filesystem::remove_all(storage_directory);
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  StorageMode mode{std::is_same_v<StorageType, DiskStorage>
                       ? StorageMode::ON_DISK_TRANSACTIONAL
                       : (std::is_same_v<StorageType, InMemTransactional> ? StorageMode::IN_MEMORY_TRANSACTIONAL
                                                                          : StorageMode::IN_MEMORY_ANALYTICAL)};
};

using StorageTypes = ::testing::Types<InMemTransactional /*, InMemAnalytical*/>;

TYPED_TEST_SUITE(SchemaInfoTest, StorageTypes);
TYPED_TEST_SUITE(SchemaInfoTestWEdgeProp, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, SingleVertex) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto p3 = in_memory->NameToProperty("p3");

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create empty vertex
  {
    auto acc = in_memory->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create vertex with label
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create vertex with label and property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["count"], 1);
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create vertex
  // add labels
  // add property
  // remove property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.AddLabel(l3).HasError());
    ASSERT_FALSE(v.RemoveLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.ClearProperties().HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create vertex
  // add property
  // remove property
  // add property
  // modify property
  // change property type
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    const auto gid = v.Gid();
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.ClearProperties().HasError());
    ASSERT_FALSE(
        v.SetProperty(p3, memgraph::storage::PropertyValue{memgraph::storage::PropertyValue::list_t{}}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    acc = in_memory->Access();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD)
                     ->SetProperty(p2, memgraph::storage::PropertyValue{false})
                     .HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][0]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][0]["properties"][1]["types"][0]["count"], 1);
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create - delete - commit
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->DeleteVertex(&v).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create - rollback
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    acc->Abort();
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, MultipleVertices) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto p3 = in_memory->NameToProperty("p3");

  Gid tmp_gid;

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create empty vertex
  {
    auto acc = in_memory->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
  }

  // create vertex with label
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 0);
  }

  // create vertex with label and property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 3);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][0]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
  }

  // create vertex
  // add labels
  // add property
  // remove property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.AddLabel(l3).HasError());
    ASSERT_FALSE(v.RemoveLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.ClearProperties().HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 0);
  }

  // create vertex
  // add property
  // remove property
  // add property
  // modify property
  // change property type
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    tmp_gid = v.Gid();
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.ClearProperties().HasError());
    ASSERT_FALSE(
        v.SetProperty(p3, memgraph::storage::PropertyValue{memgraph::storage::PropertyValue::list_t{}}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    acc = in_memory->Access();
    ASSERT_FALSE(acc->FindVertex(tmp_gid, memgraph::storage::View::OLD)
                     ->SetProperty(p2, memgraph::storage::PropertyValue{false})
                     .HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 2);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["count"], 1);
  }

  // create - delete - commit
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->DeleteVertex(&v).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 2);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["count"], 1);
  }

  // create - rollback
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    acc->Abort();
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 2);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["count"], 1);
  }

  // change property type
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(v->SetProperty(p2, memgraph::storage::PropertyValue{"string"}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 2);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "string");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["count"], 1);
  }

  // add multiple vertices with same property id, different types
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    auto v4 = acc->CreateVertex();
    ASSERT_FALSE(v1.SetProperty(p2, memgraph::storage::PropertyValue{"string"}).HasError());
    ASSERT_FALSE(v3.SetProperty(p2, memgraph::storage::PropertyValue{123}).HasError());
    ASSERT_FALSE(v4.SetProperty(p2, memgraph::storage::PropertyValue{true}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 6);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 4);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 3);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["type"], "int");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["type"], "string");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["count"], 2);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][3]["properties"][1]["types"][0]["count"], 1);
  }

  // change label
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(v->AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 2);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 2);
    ASSERT_EQ(json["nodes"][2]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][2]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["properties"][0]["types"][0]["type"], "string");
    ASSERT_EQ(json["nodes"][2]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["properties"][1]["key"], "p3");
    ASSERT_EQ(json["nodes"][2]["properties"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["properties"][1]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["properties"][1]["types"][0]["type"], "list");
    ASSERT_EQ(json["nodes"][2]["properties"][1]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["count"], 5);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 3);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 3);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["type"], "int");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["type"], "string");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["count"], 1);
  }

  // remove vertex with properties
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->DeleteVertex(&*v).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][0]["labels"][1], "L3");
    ASSERT_EQ(json["nodes"][0]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["labels"][0], "L2");
    ASSERT_EQ(json["nodes"][1]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["type"], "int");
    ASSERT_EQ(json["nodes"][1]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["count"], 1);
    ASSERT_EQ(json["nodes"][2]["labels"].size(), 1);
    ASSERT_EQ(json["nodes"][2]["labels"][0], "L1");
    ASSERT_EQ(json["nodes"][2]["properties"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["count"], 5);
    ASSERT_EQ(json["nodes"][3]["labels"].size(), 0);
    ASSERT_EQ(json["nodes"][3]["properties"].size(), 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["key"], "p2");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["count"], 3);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"].size(), 3);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["type"], "bool");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["type"], "int");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["type"], "string");
    ASSERT_EQ(json["nodes"][3]["properties"][0]["types"][2]["count"], 1);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, SingleEdge) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid tmp_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto e = in_memory->NameToEdgeType("E");
  auto e2 = in_memory->NameToEdgeType("E2");
  auto e3 = in_memory->NameToEdgeType("E3");

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create simple edge
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_FALSE(edge.HasError());
    tmp_gid = edge->Gid();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // delete edge
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(acc->DeleteEdge(&*e).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 2);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // change from label
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge = acc->CreateEdge(&*v1, &*v2, e2);
    ASSERT_FALSE(edge.HasError());
    tmp_gid = edge->Gid();
    ASSERT_FALSE(acc->Commit().HasError());
    acc = in_memory->Access();
    v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // delete edge - rollback
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(acc->DeleteEdge(&*e).HasError());
    acc->Abort();
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // change to label
  {
    auto acc = in_memory->Access();
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v2);
    ASSERT_FALSE(v2->AddLabel(l2).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // change to and from label
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());
    ASSERT_TRUE(v2);
    ASSERT_FALSE(v2->AddLabel(l3).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // create delete commit
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge = acc->CreateEdge(&*v1, &*v2, e3);
    ASSERT_FALSE(edge.HasError());
    ASSERT_FALSE(acc->DeleteEdge(&edge.GetValue()).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // delete vertices
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, MultipleEdges) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid e1_gid, e2_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto e = in_memory->NameToEdgeType("E");
  auto e2 = in_memory->NameToEdgeType("E2");
  auto e3 = in_memory->NameToEdgeType("E3");

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create multiple edges
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_FALSE(edge.HasError());
    e1_gid = edge->Gid();
    auto edge2 = acc->CreateEdge(&v2, &v1, e2);
    ASSERT_FALSE(edge2.HasError());
    e2_gid = edge2->Gid();
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, e3).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 3);
    const auto &json_edges = json["edges"];

    const auto e1 = nlohmann::json::object({{"type", "E"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e2 = nlohmann::json::object({{"type", "E2"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3 = nlohmann::json::object({{"type", "E3"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});

    ASSERT_EQ(json_edges.size(), 3);
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e1; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3; }));
  }

  // delete edge
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(e1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(acc->DeleteEdge(&*e).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 3);
    const auto &json_edges = json["edges"];

    const auto e2 = nlohmann::json::object({{"type", "E2"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3 = nlohmann::json::object({{"type", "E3"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});

    ASSERT_EQ(json_edges.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3; }));
  }

  // change vertex label
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["count"], 2);
    const auto &json_edges = json["edges"];

    const auto e2 = nlohmann::json::object({{"type", "E2"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({"L1"})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3 = nlohmann::json::object({{"type", "E3"},
                                            {"start_node_labels", nlohmann::json::array({"L1"})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});

    ASSERT_EQ(json_edges.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3; }));
  }

  // delete edge - rollback
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(e2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(acc->DeleteEdge(&*e).HasError());
    acc->Abort();
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["count"], 2);
    const auto &json_edges = json["edges"];

    const auto e2 = nlohmann::json::object({{"type", "E2"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({"L1"})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3 = nlohmann::json::object({{"type", "E3"},
                                            {"start_node_labels", nlohmann::json::array({"L1"})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});

    ASSERT_EQ(json_edges.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3; }));
  }

  // create multiple edges
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    ASSERT_FALSE(v1.AddLabel(l3).HasError());
    ASSERT_FALSE(v1.AddLabel(l2).HasError());
    ASSERT_FALSE(v2.AddLabel(l).HasError());
    ASSERT_FALSE(v2.AddLabel(l2).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, e).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v2, &v1, e2).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, e3).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, e).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 4);
    const auto &json_edges = json["edges"];

    const auto e2 = nlohmann::json::object({{"type", "E2"},
                                            {"start_node_labels", nlohmann::json::array({/* empty */})},
                                            {"end_node_labels", nlohmann::json::array({"L1"})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3 = nlohmann::json::object({{"type", "E3"},
                                            {"start_node_labels", nlohmann::json::array({"L1"})},
                                            {"end_node_labels", nlohmann::json::array({/* empty */})},
                                            {"count", 1},
                                            {"properties", nlohmann::json::array({/* empty */})}});
    const auto e_new = nlohmann::json::object({{"type", "E"},
                                               {"start_node_labels", nlohmann::json::array({"L2", "L3"})},
                                               {"end_node_labels", nlohmann::json::array({"L1", "L2"})},
                                               {"count", 2},
                                               {"properties", nlohmann::json::array({/* empty */})}});
    const auto e2_new = nlohmann::json::object({{"type", "E2"},
                                                {"start_node_labels", nlohmann::json::array({"L1", "L2"})},
                                                {"end_node_labels", nlohmann::json::array({"L2", "L3"})},
                                                {"count", 1},
                                                {"properties", nlohmann::json::array({/* empty */})}});
    const auto e3_new = nlohmann::json::object({{"type", "E3"},
                                                {"start_node_labels", nlohmann::json::array({"L2", "L3"})},
                                                {"end_node_labels", nlohmann::json::array({"L1", "L2"})},
                                                {"count", 1},
                                                {"properties", nlohmann::json::array({/* empty */})}});

    ASSERT_EQ(json_edges.size(), 5);
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e_new; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e2_new; }));
    ASSERT_TRUE(std::any_of(json_edges.begin(), json_edges.end(), [&](const auto &in) { return in == e3_new; }));
  }

  // delete vertices
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, SingleEdge) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid tmp_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto e = in_memory->NameToEdgeType("E");

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create simple edge
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    v1_gid = v1.Gid();
    auto v2 = acc->CreateVertex();
    v2_gid = v2.Gid();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_FALSE(edge.HasError());
    tmp_gid = edge->Gid();
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // add edge properties
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(e->SetProperty(p2, PropertyValue{"a"}).HasError());
    ASSERT_FALSE(e->SetProperty(p, PropertyValue{true}).HasError());
    ASSERT_FALSE(e->SetProperty(p, PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object(
        {{"key", "p2"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "string"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // change from label
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object(
        {{"key", "p2"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "string"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // delete edge - rollback
  {
    auto acc = in_memory->Access();
    auto e = acc->FindEdge(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(e);
    ASSERT_FALSE(acc->DeleteEdge(&*e).HasError());
    acc->Abort();
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object(
        {{"key", "p2"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "string"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // change to label
  {
    auto acc = in_memory->Access();
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v2);
    ASSERT_FALSE(v2->AddLabel(l2).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object(
        {{"key", "p2"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "string"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // change to and from label->edges
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());
    ASSERT_TRUE(v2);
    ASSERT_FALSE(v2->AddLabel(l3).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object(
        {{"key", "p2"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "string"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // delete edge property
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    auto edges = v1->OutEdges(memgraph::storage::View::NEW);
    ASSERT_EQ(edges->edges.size(), 1);
    ASSERT_FALSE(edges->edges[0].SetProperty(p2, PropertyValue{}).HasError());
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object(
        {{"key", "p1"}, {"count", 1}, {"types", nlohmann::json::array({{{"type", "int"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 1);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
  }

  // delete vertices
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, ConcurrentEdges) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid e_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto e = in_memory->NameToEdgeType("E");

  // Empty
  {
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // Change out/in vertex labels in parallel
  {
    // Setup
    // CREATE (:A)-[:E{p:1}]->(:B);
    {
      auto acc = in_memory->Access();
      auto v1 = acc->CreateVertex();
      ASSERT_FALSE(v1.AddLabel(l).HasError());
      v1_gid = v1.Gid();
      auto v2 = acc->CreateVertex();
      ASSERT_FALSE(v2.AddLabel(l2).HasError());
      v2_gid = v2.Gid();
      auto edge = acc->CreateEdge(&v1, &v2, e);
      ASSERT_FALSE(edge.HasError());
      ASSERT_FALSE(edge->SetProperty(p, PropertyValue{1}).HasError());
      e_gid = edge->Gid();
      ASSERT_FALSE(acc->Commit().HasError());
    }

    // Data manipulations
    // TX1
    // 1 BEGIN;
    // 4 MATCH(n:A) SET n:L;
    // 5 COMMIT;
    //
    // TX2
    // 2 BEGIN;
    // 3 MATCH(n:B) SET n:L;
    // 6 COMMIT;
    auto tx1 = in_memory->Access();
    auto tx2 = in_memory->Access();
    auto v2 = tx2->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(v2->AddLabel(l3).HasError());
    auto v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());
    ASSERT_FALSE(tx1->Commit().HasError());

    // Check
    const auto json_mid =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json_mid["nodes"].size(), 2);
    ASSERT_TRUE(std::any_of(json_mid["nodes"].begin(), json_mid["nodes"].end(), [&](const auto &in) {
      return in == nlohmann::json::object({{"count", 1},
                                           {"labels", nlohmann::json::array({"L1", "L3"})},
                                           {"properties", nlohmann::json::array({/* empty */})}});
    }));
    ASSERT_TRUE(std::any_of(json_mid["nodes"].begin(), json_mid["nodes"].end(), [&](const auto &in) {
      return in == nlohmann::json::object({{"count", 1},
                                           {"labels", nlohmann::json::array({"L2"})},
                                           {"properties", nlohmann::json::array({/* empty */})}});
    }));
    ASSERT_EQ(json_mid["edges"].size(), 1);
    EXPECT_EQ(json_mid["edges"][0]["type"], "E");
    EXPECT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    EXPECT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    EXPECT_EQ(json_mid["edges"][0]["count"], 1);
    EXPECT_EQ(json_mid["edges"][0]["properties"].size(), 1);
    EXPECT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
    EXPECT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
    EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
    EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "int");
    EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);

    // Commit tx2
    ASSERT_FALSE(tx2->Commit().HasError());

    // Check
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_TRUE(std::any_of(json["nodes"].begin(), json["nodes"].end(), [&](const auto &in) {
      return in == nlohmann::json::object({{"count", 1},
                                           {"labels", nlohmann::json::array({"L1", "L3"})},
                                           {"properties", nlohmann::json::array({/* empty */})}});
    }));
    ASSERT_TRUE(std::any_of(json["nodes"].begin(), json["nodes"].end(), [&](const auto &in) {
      return in == nlohmann::json::object({{"count", 1},
                                           {"labels", nlohmann::json::array({"L2", "L3"})},
                                           {"properties", nlohmann::json::array({/* empty */})}});
    }));
    ASSERT_EQ(json["edges"].size(), 1);
    EXPECT_EQ(json["edges"][0]["type"], "E");
    EXPECT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    EXPECT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    EXPECT_EQ(json["edges"][0]["count"], 1);
    EXPECT_EQ(json["edges"][0]["properties"].size(), 1);
    EXPECT_EQ(json["edges"][0]["properties"][0]["key"], "p1");
    EXPECT_EQ(json["edges"][0]["properties"][0]["count"], 1);
    EXPECT_EQ(json["edges"][0]["properties"][0]["types"].size(), 1);
    EXPECT_EQ(json["edges"][0]["properties"][0]["types"][0]["type"], "int");
    EXPECT_EQ(json["edges"][0]["properties"][0]["types"][0]["count"], 1);
  }

  // Clear
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->Commit().HasError());
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // Change in vertex label and edge property in parallel
  {
    // Setup
    // CREATE (:A)-[:E{p:1}]->(:B);
    {
      auto acc = in_memory->Access();
      auto v1 = acc->CreateVertex();
      ASSERT_FALSE(v1.AddLabel(l).HasError());
      v1_gid = v1.Gid();
      auto v2 = acc->CreateVertex();
      ASSERT_FALSE(v2.AddLabel(l2).HasError());
      v2_gid = v2.Gid();
      auto edge = acc->CreateEdge(&v1, &v2, e);
      ASSERT_FALSE(edge.HasError());
      ASSERT_FALSE(edge->SetProperty(p, PropertyValue{1}).HasError());
      e_gid = edge->Gid();
      ASSERT_FALSE(acc->Commit().HasError());
    }

    // Data manipulations
    // TX1
    // 1 BEGIN;
    // 4 MATCH (:A)-[e:E{p:1}]->(:B) SET e.p="";
    // 5 COMMIT;
    //
    // TX2
    // 2 BEGIN;
    // 3 MATCH(n:B) SET n:L;
    // 6 ROLLBACK;
    auto tx1 = in_memory->Access();
    auto tx2 = in_memory->Access();
    auto v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());
    auto edge = tx1->FindEdge(e_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(edge->SetProperty(p, PropertyValue{""}).HasError());
    ASSERT_FALSE(tx1->Commit().HasError());

    // Check
    const auto json_mid =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json_mid["nodes"].size(), 2);
    ASSERT_EQ(json_mid["nodes"][0]["count"], 1);
    ASSERT_EQ(json_mid["nodes"][0]["labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json_mid["nodes"][1]["count"], 1);
    ASSERT_EQ(json_mid["nodes"][1]["labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json_mid["edges"].size(), 1);
    ASSERT_EQ(json_mid["edges"][0]["type"], "E");
    ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json_mid["edges"][0]["count"], 1);
    ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
    ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
    ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "string");
    ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);

    // Abort tx2
    tx2->Abort();

    // Check
    const auto json =
        schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->config_.salient.items.properties_on_edges);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["nodes"][0]["count"], 1);
    ASSERT_EQ(json["nodes"][0]["labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json["nodes"][1]["count"], 1);
    ASSERT_EQ(json["nodes"][1]["labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 1);
    ASSERT_EQ(json["edges"][0]["properties"][0]["key"], "p1");
    ASSERT_EQ(json["edges"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"][0]["types"].size(), 1);
    ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["type"], "string");
    ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["count"], 1);
  }
}