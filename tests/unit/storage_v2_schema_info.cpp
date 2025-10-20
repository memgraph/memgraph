// Copyright 2025 Memgraph Ltd.
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
#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>

#include "dbms/constants.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/temporal.hpp"
#include "storage/v2/view.hpp"
#include "storage_test_utils.hpp"

#include <nlohmann/json.hpp>
#include <stdexcept>
#include <thread>
#include "tests/test_commit_args_helper.hpp"

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
    config_.salient.items.properties_on_edges = false;
    config_.salient.storage_mode = mode;
    config_.salient.items.enable_schema_info = true;

    // TODO OnDisk no supported at this time
    this->storage = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
  }

  void TearDown() override {
    std::filesystem::remove_all(storage_directory);
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  StorageMode mode{(std::is_same_v<StorageType, InMemTransactional> ? StorageMode::IN_MEMORY_TRANSACTIONAL
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
    config_.salient.storage_mode = mode;
    config_.salient.items.enable_schema_info = true;

    // TODO OnDisk not supported at this time
    this->storage = std::make_unique<memgraph::storage::InMemoryStorage>(config_);
  }

  void TearDown() override {
    std::filesystem::remove_all(storage_directory);
    this->storage.reset(nullptr);
  }

  memgraph::storage::Config config_;
  std::unique_ptr<memgraph::storage::Storage> storage;
  StorageMode mode{(std::is_same_v<StorageType, InMemTransactional> ? StorageMode::IN_MEMORY_TRANSACTIONAL
                                                                    : StorageMode::IN_MEMORY_ANALYTICAL)};
};

using StorageTypes = ::testing::Types<InMemTransactional, InMemAnalytical>;

TEST(SchemaInfoContext, ConfrontJSON) {
  {
    nlohmann::json json1{};
    nlohmann::json json2{};
    ASSERT_TRUE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1{1, 2, 3};
    nlohmann::json json2{2, 3, 1};
    ASSERT_TRUE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1{1, 2, 3};
    nlohmann::json json2{2, 3};
    ASSERT_FALSE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1{1, 2, 3};
    nlohmann::json json2{"1", "2", "3"};
    ASSERT_FALSE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1{{}};
    nlohmann::json json2{{}};
    ASSERT_TRUE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1;
    nlohmann::json json2;
    json1.emplace("test", nlohmann::json::array({1, 2, 3}));
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2.emplace("test", nlohmann::json::array({1, 2, 3}));
    ASSERT_TRUE(ConfrontJSON(json1, json2));
    json1.emplace("test2", "a");
    ASSERT_FALSE(ConfrontJSON(json1, json2));
  }
  {
    nlohmann::json json1;
    nlohmann::json json2;
    const auto node = nlohmann::json::object(
        {{"count", 1},
         {"labels", nlohmann::json::array({"a", "b", "c"})},
         {"properties",
          nlohmann::json::array(
              {nlohmann::json::object(
                   {{"count", 2},
                    {"ration", 35.0},
                    {"types", nlohmann::json::array({{{"type", "1"}, {"count", 1}}, {{"type", "2"}, {"count", 2}}})}}),
               nlohmann::json::object({{"count", 1},
                                       {"ration", 25.0},
                                       {"types", nlohmann::json::array({{{"type", "1"}, {"count", 1}},
                                                                        {{"type", "5"}, {"count", 5}}})}})})}});
    json1.emplace("nodes", node);
    json2.emplace("nodes", node);
    ASSERT_TRUE(ConfrontJSON(json1, json2));
    json2["nodes"]["count"] = 2;
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2["nodes"]["count"] = 1;
    json2["nodes"]["labels"].erase(1);
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2["nodes"]["labels"].push_back("b");
    ASSERT_TRUE(ConfrontJSON(json1, json2));
    json2["nodes"]["properties"][0]["ration"] = 10.0;
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2["nodes"]["properties"][0]["ration"] = 35.0;
    json2["nodes"]["properties"][0]["types"].erase(0);
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2["nodes"]["properties"][0]["types"].push_back({{"type", "1"}, {"count", 1}});
    ASSERT_TRUE(ConfrontJSON(json1, json2));
    json2["nodes"]["properties"].erase(0);
    ASSERT_FALSE(ConfrontJSON(json1, json2));
    json2["nodes"]["properties"].push_back(nlohmann::json::object(
        {{"count", 2},
         {"ration", 35.0},
         {"types", nlohmann::json::array({{{"type", "1"}, {"count", 1}}, {{"type", "2"}, {"count", 2}}})}}));
    ASSERT_TRUE(ConfrontJSON(json1, json2));
  }
}

TYPED_TEST_SUITE(SchemaInfoTest, StorageTypes);
TYPED_TEST_SUITE(SchemaInfoTestWEdgeProp, StorageTypes);

auto &&jarray = nlohmann::json::array;

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

  auto expected_result = nlohmann::json::object({{"nodes", jarray({})}, {"edges", jarray({})}});

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // create empty vertex
  {
    auto acc = in_memory->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].push_back({{"count", 1}, {"labels", jarray({})}, {"properties", jarray({})}});
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // create vertex with label
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].push_back({{"count", 1}, {"labels", jarray({"L1"})}, {"properties", jarray({})}});
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // create vertex with label and property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{"12"}).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{false}).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].push_back(
        {{"count", 1},
         {"labels", jarray({"L2"})},
         {"properties", jarray({{{"key", "p1"},
                                 {"count", 1},
                                 {"filling_factor", 100.0},
                                 {"types", jarray({{{"type", "Integer"}, {"count", 1}}})}}})}});
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].push_back({{"count", 1}, {"labels", jarray({"L2", "L3"})}, {"properties", jarray({})}});
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].push_back(
        {{"count", 1},
         {"labels", jarray({})},
         {"properties", jarray({{{"key", "p2"},
                                 {"count", 1},
                                 {"filling_factor", 100.0},
                                 {"types", jarray({{{"type", "String"}, {"count", 1}}})}},
                                {{"key", "p3"},
                                 {"count", 1},
                                 {"filling_factor", 100.0},
                                 {"types", jarray({{{"type", "List"}, {"count", 1}}})}}})}});
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
    acc = in_memory->Access();
    ASSERT_FALSE(acc->FindVertex(gid, memgraph::storage::View::OLD)
                     ->SetProperty(p2, memgraph::storage::PropertyValue{false})
                     .HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    auto &prop = FindProp(expected_result["nodes"][0], "p2");
    prop["types"][0]["type"] = "Boolean";
    const auto json2 = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json2, expected_result));
  }

  // delete vertex
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // create - delete - commit
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->DeleteVertex(&v).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    expected_result["nodes"].clear();
    expected_result["edges"].clear();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
  }

  // create - rollback
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    acc->Abort();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      // No changes to result
    } else {
      expected_result["nodes"].push_back(
          {{"count", 1},
           {"labels", jarray({"L1"})},
           {"properties", jarray({{{"key", "p1"},
                                   {"count", 1},
                                   {"filling_factor", 100.0},
                                   {"types", jarray({{{"type", "Integer"}, {"count", 1}}})}},
                                  {{"key", "p2"},
                                   {"count", 1},
                                   {"filling_factor", 100.0},
                                   {"types", jarray({{{"type", "String"}, {"count", 1}}})}}})}});
    }
    ASSERT_TRUE(ConfrontJSON(json, expected_result));
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
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create empty vertex
  {
    auto acc = in_memory->Access();
    acc->CreateVertex();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // create vertex with label and property
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l2).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 3);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else {
        ASSERT_TRUE(false);
      }
    }
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc = in_memory->Access();
    ASSERT_FALSE(acc->FindVertex(tmp_gid, memgraph::storage::View::OLD)
                     ->SetProperty(p2, memgraph::storage::PropertyValue{false})
                     .HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 2);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 1);
        const auto &json_p2_types = (*json_p2)["types"];
        ASSERT_EQ(json_p2_types.size(), 1);
        EXPECT_EQ(json_p2_types[0], nlohmann::json::object({{"type", "Boolean"}, {"count", 1}}));

        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        const auto &json_p3_types = (*json_p3)["types"];
        ASSERT_EQ(json_p3_types.size(), 1);
        EXPECT_EQ(json_p3_types[0], nlohmann::json::object({{"type", "List"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // create - delete - commit
  {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(acc->DeleteVertex(&v).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 2);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 1);
        const auto &json_p2_types = (*json_p2)["types"];
        ASSERT_EQ(json_p2_types.size(), 1);
        EXPECT_EQ(json_p2_types[0], nlohmann::json::object({{"type", "Boolean"}, {"count", 1}}));

        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        const auto &json_p3_types = (*json_p3)["types"];
        ASSERT_EQ(json_p3_types.size(), 1);
        EXPECT_EQ(json_p3_types[0], nlohmann::json::object({{"type", "List"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // create - rollback
  if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto acc = in_memory->Access();
    auto v = acc->CreateVertex();
    ASSERT_FALSE(v.AddLabel(l).HasError());
    ASSERT_FALSE(v.SetProperty(p, memgraph::storage::PropertyValue{12}).HasError());
    ASSERT_FALSE(v.SetProperty(p2, memgraph::storage::PropertyValue{"abc"}).HasError());
    acc->Abort();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 2);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 1);
        const auto &json_p2_types = (*json_p2)["types"];
        ASSERT_EQ(json_p2_types.size(), 1);
        EXPECT_EQ(json_p2_types[0], nlohmann::json::object({{"type", "Boolean"}, {"count", 1}}));

        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        const auto &json_p3_types = (*json_p3)["types"];
        ASSERT_EQ(json_p3_types.size(), 1);
        EXPECT_EQ(json_p3_types[0], nlohmann::json::object({{"type", "List"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // change property type
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(v->SetProperty(p2, memgraph::storage::PropertyValue{"String"}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 2);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 1);
        const auto &json_p2_types = (*json_p2)["types"];
        ASSERT_EQ(json_p2_types.size(), 1);
        EXPECT_EQ(json_p2_types[0], nlohmann::json::object({{"type", "String"}, {"count", 1}}));

        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        const auto &json_p3_types = (*json_p3)["types"];
        ASSERT_EQ(json_p3_types.size(), 1);
        EXPECT_EQ(json_p3_types[0], nlohmann::json::object({{"type", "List"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // add multiple vertices with same property id, different types
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    acc->CreateVertex();
    auto v3 = acc->CreateVertex();
    auto v4 = acc->CreateVertex();
    ASSERT_FALSE(v1.SetProperty(p2, memgraph::storage::PropertyValue{"String"}).HasError());
    ASSERT_FALSE(v3.SetProperty(p2, memgraph::storage::PropertyValue{123}).HasError());
    ASSERT_FALSE(v4.SetProperty(p2, memgraph::storage::PropertyValue{true}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 6);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 4);
        const auto &json_p2_types = (*json_p2)["types"];
        ASSERT_EQ(json_p2_types.size(), 3);
        const auto prop1 = nlohmann::json::object({{"type", "Boolean"}, {"count", 1}});
        const auto prop2 = nlohmann::json::object({{"type", "Integer"}, {"count", 1}});
        const auto prop3 = nlohmann::json::object({{"type", "String"}, {"count", 2}});
        EXPECT_TRUE(
            std::any_of(json_p2_types.begin(), json_p2_types.end(), [&](const auto &in) { return in == prop1; }));
        EXPECT_TRUE(
            std::any_of(json_p2_types.begin(), json_p2_types.end(), [&](const auto &in) { return in == prop2; }));
        EXPECT_TRUE(
            std::any_of(json_p2_types.begin(), json_p2_types.end(), [&](const auto &in) { return in == prop3; }));

        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        const auto &json_p3_types = (*json_p3)["types"];
        ASSERT_EQ(json_p3_types.size(), 1);
        EXPECT_EQ(json_p3_types[0], nlohmann::json::object({{"type", "List"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // change label
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(v->AddLabel(l).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 5);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p2");
        EXPECT_EQ(json_prop[0]["count"], 3);
        const auto &json_prop_types = json_prop[0]["types"];
        ASSERT_EQ(json_prop_types.size(), 3);
        const auto prop1 = nlohmann::json::object({{"type", "Boolean"}, {"count", 1}});
        const auto prop2 = nlohmann::json::object({{"type", "Integer"}, {"count", 1}});
        const auto prop3 = nlohmann::json::object({{"type", "String"}, {"count", 1}});
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop1; }));
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop2; }));
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop3; }));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 2);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 2);

        const auto json_p2 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p2"; });
        ASSERT_NE(json_p2, json_prop.end());
        EXPECT_EQ((*json_p2)["count"], 1);
        ASSERT_EQ((*json_p2)["types"].size(), 1);
        EXPECT_EQ((*json_p2)["types"][0]["count"], 1);
        EXPECT_EQ((*json_p2)["types"][0]["type"], "String");
        const auto json_p3 =
            std::find_if(json_prop.begin(), json_prop.end(), [&](const auto &in) { return in["key"] == "p3"; });
        ASSERT_NE(json_p3, json_prop.end());
        EXPECT_EQ((*json_p3)["count"], 1);
        ASSERT_EQ((*json_p3)["types"].size(), 1);
        EXPECT_EQ((*json_p3)["types"][0]["count"], 1);
        EXPECT_EQ((*json_p3)["types"][0]["type"], "List");
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  // remove vertex with properties
  {
    auto acc = in_memory->Access();
    auto v = acc->FindVertex(tmp_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v);
    ASSERT_FALSE(acc->DeleteVertex(&*v).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 4);
    ASSERT_EQ(json["edges"].size(), 0);

    const auto &json_nodes = json["nodes"];
    for (const auto &json_node : json_nodes) {
      if (json_node["labels"] == nlohmann::json::array({/* empty */})) {
        EXPECT_EQ(json_node["count"], 5);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p2");
        EXPECT_EQ(json_prop[0]["count"], 3);
        const auto &json_prop_types = json_prop[0]["types"];
        ASSERT_EQ(json_prop_types.size(), 3);
        const auto prop1 = nlohmann::json::object({{"type", "Boolean"}, {"count", 1}});
        const auto prop2 = nlohmann::json::object({{"type", "Integer"}, {"count", 1}});
        const auto prop3 = nlohmann::json::object({{"type", "String"}, {"count", 1}});
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop1; }));
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop2; }));
        EXPECT_TRUE(
            std::any_of(json_prop_types.begin(), json_prop_types.end(), [&](const auto &in) { return in == prop3; }));
      } else if (json_node["labels"] == nlohmann::json::array({"L1"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else if (json_node["labels"] == nlohmann::json::array({"L2"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 1);
        EXPECT_EQ(json_prop[0]["key"], "p1");
        EXPECT_EQ(json_prop[0]["count"], 1);
        ASSERT_EQ(json_prop[0]["types"].size(), 1);
        EXPECT_EQ(json_prop[0]["types"][0], nlohmann::json::object({{"type", "Integer"}, {"count", 1}}));
      } else if (json_node["labels"] == nlohmann::json::array({"L2", "L3"})) {
        EXPECT_EQ(json_node["count"], 1);
        const auto &json_prop = json_node["properties"];
        ASSERT_EQ(json_prop.size(), 0);
      } else {
        ASSERT_TRUE(false);
      }
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, SingleEdge) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid edge_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto e = in_memory->NameToEdgeType("E");
  auto e2 = in_memory->NameToEdgeType("E2");
  auto e3 = in_memory->NameToEdgeType("E3");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    edge_gid = edge->Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    edge_gid = edge->Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    acc = in_memory->Access();
    v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->AddLabel(l).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    EXPECT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    EXPECT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    EXPECT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // delete edge - rollback
  if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e2, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    acc->Abort();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E2");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
  }

  // delete change labels commit
  {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e2, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    ASSERT_TRUE(v1);
    ASSERT_FALSE(v1->RemoveLabel(l3).HasError());
    ASSERT_TRUE(v2);
    ASSERT_FALSE(v2->RemoveLabel(l3).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // delete vertices
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // create edge change to and from label
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    ASSERT_FALSE(v1.AddLabel(l).HasError());
    ASSERT_FALSE(acc->CreateEdge(&v1, &v2, e).HasError());
    ASSERT_FALSE(v2.AddLabel(l3).HasError());
    ASSERT_FALSE(v1.AddLabel(l2).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L2"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"].size(), 0);
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
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(e1_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
  if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(e2_gid, memgraph::storage::View::NEW, e2, &*v2, &*v1);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    acc->Abort();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, SingleEdge) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;
  Gid edge_gid, v1_gid, v2_gid;

  auto l = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto e = in_memory->NameToEdgeType("E");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    edge_gid = edge->Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(edge_acc->SetProperty(p2, PropertyValue{"a"}).HasError());
    ASSERT_FALSE(edge_acc->SetProperty(p, PropertyValue{true}).HasError());
    ASSERT_FALSE(edge_acc->SetProperty(p, PropertyValue{12}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 1);
    ASSERT_EQ(json["nodes"][0]["count"], 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p2"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});

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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p2"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 2);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p2; }));
  }

  // delete edge - rollback
  if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    acc->Abort();
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({/* empty */}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p2"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});

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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p2"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});

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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
    const auto p2 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p2"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});

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
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 2);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 1);
    ASSERT_TRUE(std::any_of(json_edges_properties.begin(), json_edges_properties.end(),
                            [&](const auto &in) { return in == p1; }));
  }

  // delete edge
  {
    {
      auto acc = in_memory->Access();
      auto v1 = acc->CreateVertex();
      v1_gid = v1.Gid();
      auto v2 = acc->CreateVertex();
      v2_gid = v2.Gid();
      auto edge = acc->CreateEdge(&v1, &v2, e);
      ASSERT_FALSE(edge.HasError());
      edge_gid = edge->Gid();
      ASSERT_FALSE(edge->SetProperty(p2, PropertyValue{"a"}).HasError());
      ASSERT_FALSE(edge->SetProperty(p, PropertyValue{true}).HasError());
      ASSERT_FALSE(edge->SetProperty(p, PropertyValue{12}).HasError());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

      const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
      ASSERT_EQ(json["nodes"].size(), 3);
      ASSERT_EQ(json["edges"].size(), 2);
    }

    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);

    ASSERT_EQ(json["nodes"].size(), 3);
    ASSERT_EQ(json["edges"].size(), 1);
    ASSERT_EQ(json["edges"][0]["type"], "E");
    ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
    ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
    ASSERT_EQ(json["edges"][0]["count"], 1);
    const auto &json_edges_properties = json["edges"][0]["properties"];

    const auto p1 = nlohmann::json::object({{"filling_factor", 100.0},
                                            {"key", "p1"},
                                            {"count", 1},
                                            {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});

    ASSERT_EQ(json_edges_properties.size(), 1);
    EXPECT_EQ(json_edges_properties[0], p1);
  }

  // set property delete edge
  {
    {
      auto acc = in_memory->Access();
      auto v1 = acc->CreateVertex();
      v1_gid = v1.Gid();
      auto v2 = acc->CreateVertex();
      v2_gid = v2.Gid();
      auto edge = acc->CreateEdge(&v1, &v2, e);
      auto edge2 = acc->CreateEdge(&v1, &v2, e);
      ASSERT_FALSE(edge.HasError());
      ASSERT_FALSE(edge2.HasError());
      edge_gid = edge->Gid();
      ASSERT_FALSE(edge->SetProperty(p2, PropertyValue{"a"}).HasError());
      ASSERT_FALSE(edge->SetProperty(p, PropertyValue{true}).HasError());
      ASSERT_FALSE(edge2->SetProperty(p2, PropertyValue{"a"}).HasError());
      ASSERT_FALSE(edge2->SetProperty(p, PropertyValue{true}).HasError());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

      const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
      ASSERT_EQ(json["nodes"].size(), 3);
      ASSERT_EQ(json["edges"].size(), 2);
    }

    auto acc = in_memory->Access();
    auto v1 = acc->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto v2 = acc->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(v1);
    ASSERT_TRUE(v2);
    auto edge_acc = acc->FindEdge(edge_gid, memgraph::storage::View::NEW, e, &*v1, &*v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(edge_acc->SetProperty(p, PropertyValue{123}).HasError());
    ASSERT_FALSE(acc->DeleteEdge(&*edge_acc).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 3);
    ASSERT_EQ(json["edges"].size(), 2);
    for (const auto &edge : json["edges"]) {
      if (edge["start_node_labels"] == nlohmann::json::array({"L1", "L3"})) {
        ASSERT_EQ(edge["type"], "E");
        ASSERT_EQ(edge["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
        ASSERT_EQ(edge["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
        ASSERT_EQ(edge["count"], 1);
        const auto &json_edges_properties = edge["properties"];
        const auto p1 =
            nlohmann::json::object({{"filling_factor", 100.0},
                                    {"key", "p1"},
                                    {"count", 1},
                                    {"types", nlohmann::json::array({{{"type", "Integer"}, {"count", 1}}})}});
        ASSERT_EQ(json_edges_properties.size(), 1);
        EXPECT_EQ(json_edges_properties[0], p1);
      } else {
        ASSERT_EQ(edge["type"], "E");
        ASSERT_EQ(edge["start_node_labels"], nlohmann::json::array());
        ASSERT_EQ(edge["end_node_labels"], nlohmann::json::array());
        ASSERT_EQ(edge["count"], 1);
        const auto &json_edges_properties = edge["properties"];
        ASSERT_EQ(json_edges_properties.size(), 2);
        const auto p1 =
            nlohmann::json::object({{"filling_factor", 100.0},
                                    {"key", "p1"},
                                    {"count", 1},
                                    {"types", nlohmann::json::array({{{"type", "Boolean"}, {"count", 1}}})}});
        const auto p2 =
            nlohmann::json::object({{"filling_factor", 100.0},
                                    {"key", "p2"},
                                    {"count", 1},
                                    {"types", nlohmann::json::array({{{"type", "String"}, {"count", 1}}})}});
        EXPECT_TRUE(json_edges_properties[0] == p1 || json_edges_properties[1] == p1);
        EXPECT_TRUE(json_edges_properties[0] == p2 || json_edges_properties[1] == p2);
      }
    }
  }

  // delete vertices
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
  auto e = in_memory->NameToEdgeType("E");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
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
    ASSERT_FALSE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    // Check
    const auto json_mid = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);

    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
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
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "Integer");
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    } else {
      // Analytical sees the changes before committing
      ASSERT_EQ(json_mid["nodes"].size(), 2);
      ASSERT_TRUE(std::any_of(json_mid["nodes"].begin(), json_mid["nodes"].end(), [&](const auto &in) {
        return in == nlohmann::json::object({{"count", 1},
                                             {"labels", nlohmann::json::array({"L1", "L3"})},
                                             {"properties", nlohmann::json::array({/* empty */})}});
      }));
      ASSERT_TRUE(std::any_of(json_mid["nodes"].begin(), json_mid["nodes"].end(), [&](const auto &in) {
        return in == nlohmann::json::object({{"count", 1},
                                             {"labels", nlohmann::json::array({"L2", "L3"})},
                                             {"properties", nlohmann::json::array({/* empty */})}});
      }));
      ASSERT_EQ(json_mid["edges"].size(), 1);
      EXPECT_EQ(json_mid["edges"][0]["type"], "E");
      EXPECT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      EXPECT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2", "L3"}));
      EXPECT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "Integer");
      EXPECT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    }

    // Commit tx2
    ASSERT_FALSE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    // Check
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
    ASSERT_EQ(json["edges"][0]["properties"].size(), 1);
    EXPECT_EQ(json["edges"][0]["properties"][0]["key"], "p1");
    EXPECT_EQ(json["edges"][0]["properties"][0]["count"], 1);
    ASSERT_EQ(json["edges"][0]["properties"][0]["types"].size(), 1);
    EXPECT_EQ(json["edges"][0]["properties"][0]["types"][0]["type"], "Integer");
    EXPECT_EQ(json["edges"][0]["properties"][0]["types"][0]["count"], 1);
  }

  // Clear
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
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
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }

    // Data manipulations
    // TX1
    // 1 BEGIN;
    // 4 MATCH (:A)-[e:E{p:1}]->(:B) SET e.p="";
    // 5 COMMIT;
    //
    // TX2
    // 2 BEGIN;
    // 3 MATCH(n:A) SET n:L;
    // 6 ROLLBACK;
    auto tx1 = in_memory->Access();
    auto tx2 = in_memory->Access();
    auto v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());

    auto tx1_v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto tx1_v2 = tx1->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(tx1_v1);
    ASSERT_TRUE(tx1_v2);
    auto edge_acc = tx1->FindEdge(e_gid, memgraph::storage::View::NEW, e, &*tx1_v1, &*tx1_v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(edge_acc->SetProperty(p, PropertyValue{""}).HasError());
    ASSERT_FALSE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    // Check
    const auto json_mid = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L1"}); });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["type"], "E");
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    } else {
      // Analytical sees changes before committing
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["type"], "E");
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    }

    // Abort tx2
    tx2->Abort();

    // Check
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      const auto json_nodes = json["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L1"}); });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json["edges"].size(), 1);
      ASSERT_EQ(json["edges"][0]["type"], "E");
      ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1"}));
      ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json["edges"][0]["count"], 1);
      ASSERT_EQ(json["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["count"], 1);
    } else {
      // There is no aborting in analytical
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["type"], "E");
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    }
  }

  // Clear
  {
    auto acc = in_memory->Access();
    for (auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      ASSERT_FALSE(acc->DetachDelete({&v}, {}, true).HasError());
    }
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_EQ(json["nodes"].size(), 0);
    ASSERT_EQ(json["edges"].size(), 0);
  }

  // Change in vertex label and edge property in parallel
  {
    // Setup
    // CREATE (:A)-[:E]->(:B);
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
      e_gid = edge->Gid();
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
    }

    // Data manipulations
    // TX1
    // 1 BEGIN;
    // 4 MATCH(n:A) SET n:L;
    // 5 COMMIT;
    //
    // TX2
    // 2 BEGIN;
    // 3 MATCH (:A)-[e:E]->(:B) SET e.p="";
    // 6 COMMIT;
    auto tx1 = in_memory->Access();
    auto tx2 = in_memory->Access();
    auto v1 = tx1->FindVertex(v1_gid, memgraph::storage::View::NEW);
    ASSERT_FALSE(v1->AddLabel(l3).HasError());

    auto tx2_v1 = tx2->FindVertex(v1_gid, memgraph::storage::View::NEW);
    auto tx2_v2 = tx2->FindVertex(v2_gid, memgraph::storage::View::NEW);
    ASSERT_TRUE(tx2_v1);
    ASSERT_TRUE(tx2_v2);
    auto edge_acc = tx2->FindEdge(e_gid, memgraph::storage::View::NEW, e, &*tx2_v1, &*tx2_v2);
    ASSERT_TRUE(edge_acc);
    ASSERT_FALSE(edge_acc->SetProperty(p, PropertyValue{""}).HasError());
    ASSERT_FALSE(tx1->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    // Check
    const auto json_mid = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 0);
    } else {
      // Analytical sees changes before committing
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["type"], "E");
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    }

    // Commit tx2
    ASSERT_FALSE(tx2->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    // Check
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    if (in_memory->storage_mode_ == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      const auto json_nodes = json["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json["edges"].size(), 1);
      ASSERT_EQ(json["edges"][0]["type"], "E");
      ASSERT_EQ(json["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json["edges"][0]["count"], 1);
      ASSERT_EQ(json["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json["edges"][0]["properties"][0]["types"][0]["count"], 1);
    } else {
      // There is no aborting in analytical
      const auto json_nodes = json_mid["nodes"];
      ASSERT_EQ(json_nodes.size(), 2);
      auto json_l1 = std::find_if(json_nodes.begin(), json_nodes.end(), [](const auto &in) {
        return in["labels"] == nlohmann::json::array({"L1", "L3"});
      });
      ASSERT_NE(json_l1, json_nodes.end());
      ASSERT_EQ((*json_l1)["count"], 1);
      auto json_l2 = std::find_if(json_nodes.begin(), json_nodes.end(),
                                  [](const auto &in) { return in["labels"] == nlohmann::json::array({"L2"}); });
      ASSERT_NE(json_l2, json_nodes.end());
      ASSERT_EQ((*json_l2)["count"], 1);
      ASSERT_EQ(json_mid["edges"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["type"], "E");
      ASSERT_EQ(json_mid["edges"][0]["start_node_labels"], nlohmann::json::array({"L1", "L3"}));
      ASSERT_EQ(json_mid["edges"][0]["end_node_labels"], nlohmann::json::array({"L2"}));
      ASSERT_EQ(json_mid["edges"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["key"], "p1");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["count"], 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"].size(), 1);
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["type"], "String");
      ASSERT_EQ(json_mid["edges"][0]["properties"][0]["types"][0]["count"], 1);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, BigCommit) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto l1 = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto l3 = in_memory->NameToLabel("L3");
  auto p1 = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto p3 = in_memory->NameToProperty("p3");
  auto e1 = in_memory->NameToEdgeType("E1");
  auto e2 = in_memory->NameToEdgeType("E2");
  auto e3 = in_memory->NameToEdgeType("E3");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  {
    // Setup
    // CREATE (:L1);
    // CREATE (:L2);
    // MATCH (v1:L1), (v2:L2) CREATE (v1)-[:E1{p1:1}]->(v2);
    // MATCH (v1:L1) SET v1.p2 = "";
    // MATCH (v1:L1) SET v1.p2 = 1;
    // MATCH (v1:L1) SET v1.p2 = NULL;
    // MATCH (v1:L1) SET v1.p1 = 3;
    // MATCH (v1:L1) SET v1.p3 = true;
    // CREATE (:L1:L2)-[:E2]->(:L3:L2);
    // MATCH (:L1:L2)-[e:E2]->(:L3:L2) SET e.p1=4;
    // MATCH (v:L1:L2) DETACH DELETE v;
    // MATCH (v4:L2:L3) CREATE (v4)<-[:E3{p3:0.0}]-();
    {
      auto acc = in_memory->Access();
      auto v1 = acc->CreateVertex();
      ASSERT_FALSE(v1.AddLabel(l1).HasError());
      auto v2 = acc->CreateVertex();
      ASSERT_FALSE(v2.AddLabel(l2).HasError());
      auto edge1 = acc->CreateEdge(&v1, &v2, e1);
      ASSERT_FALSE(edge1->SetProperty(p1, PropertyValue{1}).HasError());
      ASSERT_FALSE(v1.SetProperty(p2, PropertyValue{""}).HasError());
      ASSERT_FALSE(v1.SetProperty(p2, PropertyValue{1}).HasError());
      ASSERT_FALSE(v1.SetProperty(p2, PropertyValue{}).HasError());
      ASSERT_FALSE(v1.SetProperty(p1, PropertyValue{3}).HasError());
      ASSERT_FALSE(v1.SetProperty(p1, PropertyValue{true}).HasError());
      auto v3 = acc->CreateVertex();
      ASSERT_FALSE(v3.AddLabel(l1).HasError());
      ASSERT_FALSE(v3.AddLabel(l2).HasError());
      ASSERT_FALSE(v3.AddLabel(l3).HasError());
      ASSERT_FALSE(v3.RemoveLabel(l3).HasError());
      auto v4 = acc->CreateVertex();
      ASSERT_FALSE(v4.AddLabel(l3).HasError());
      ASSERT_FALSE(v4.AddLabel(l2).HasError());
      auto edge2 = acc->CreateEdge(&v3, &v4, e2);
      ASSERT_FALSE(edge2->SetProperty(p1, PropertyValue{4}).HasError());
      ASSERT_FALSE(acc->DetachDelete({&v3}, {}, true).HasError());
      auto v5 = acc->CreateVertex();
      auto edge3 = acc->CreateEdge(&v5, &v4, e3);
      ASSERT_FALSE(edge3->SetProperty(p3, PropertyValue{0.0}).HasError());
      ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

      // Check
      const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
      ASSERT_EQ(json["nodes"].size(), 4);
      ASSERT_EQ(json["edges"].size(), 2);
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, EdgePropertyStressTest) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto l1 = in_memory->NameToLabel("L1");
  auto l2 = in_memory->NameToLabel("L2");
  auto p1 = in_memory->NameToProperty("p1");
  auto e1 = in_memory->NameToEdgeType("E1");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // setup CREATE ()-[:E]->();
  // Running 2 write threads 1 read thread
  // t1 modify from and/or to labels
  // t2 modify edge property
  // t3 read schema

  Gid from_gid;
  Gid to_gid;
  std::atomic<Gid> edge_gid;

  // SETUP
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e1);
    ASSERT_TRUE(edge.HasValue());
    from_gid = v1.Gid();
    to_gid = v2.Gid();
    edge_gid = edge->Gid();
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  std::atomic_bool running = true;

  auto modify_labels = [&]() {
    uint8_t i = 0;
    while (running) {
      ++i;
      auto acc = in_memory->Access();
      bool can_commit = true;
      if (i % 2 == 0) {
        auto v = acc->FindVertex(from_gid, View::NEW);
        ASSERT_TRUE(v);
        const auto labels = v->Labels(View::NEW);
        if (labels.HasError()) continue;
        if (labels->empty()) {
          can_commit &= v->AddLabel(l1).HasValue();
        } else {
          can_commit &= v->RemoveLabel(l1).HasValue();
        }
      }
      if (i % 3 == 0) {
        auto v = acc->FindVertex(to_gid, View::NEW);
        const auto labels = v->Labels(View::NEW);
        if (labels.HasError()) continue;
        if (labels->empty()) {
          can_commit &= v->AddLabel(l2).HasValue();
        } else {
          can_commit &= v->RemoveLabel(l2).HasValue();
        }
      }
      if (can_commit) ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };

  auto modify_edge = [&]() {
    uint8_t i = 0;
    while (running) {
      ++i;
      if (i % 5 == 0) {
        auto acc = in_memory->Access();
        auto edge = acc->FindEdge(edge_gid, View::NEW);
        if (!edge) continue;  // Other thread could delete the edge
        const auto props = edge->Properties(View::NEW);
        if (props.HasError()) continue;
        bool can_commit = true;
        if (props->empty()) {
          can_commit = edge->SetProperty(p1, PropertyValue{""}).HasValue();
        } else {
          can_commit = edge->ClearProperties().HasValue();
        }
        if (can_commit) ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };

  auto modify_edge2 = [&]() {
    uint8_t i = 0;
    while (running) {
      ++i;
      if (i % 7 == 0) {
        if (i % 2 == 0) {
          // Modify property
          auto acc = in_memory->Access();
          auto edge = acc->FindEdge(edge_gid, View::NEW);
          if (!edge) continue;  // Edge could be deleted
          const auto props = edge->Properties(View::NEW);
          if (props.HasError()) continue;
          bool can_commit = edge->SetProperty(p1, PropertyValue{123}).HasValue();
          if (i % 3) {
            can_commit &= edge->SetProperty(p1, PropertyValue{true}).HasValue();
          }
          if (can_commit) ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
        } else if (in_memory->storage_mode_ ==
                   memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {  // Analytical doesn't support mt
                                                                               // deletion/modification
          // Delete/Create edge
          auto acc = in_memory->Access();
          auto edge = acc->FindEdge(edge_gid, View::NEW);
          bool can_commit = true;
          if (edge) {  // Edge exists, delete it
            can_commit = acc->DeleteEdge(&*edge).HasValue();
          } else {  // Edge doesn't exist, create it
            auto v1 = acc->FindVertex(from_gid, View::NEW);
            ASSERT_TRUE(v1);
            auto v2 = acc->FindVertex(to_gid, View::NEW);
            ASSERT_TRUE(v2);
            auto edge = acc->CreateEdge(&*v1, &*v2, e1);
            if (edge.HasError()) continue;
            edge_gid = edge->Gid();
          }
          if (can_commit) ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };

  auto read_schema = [&]() {
    auto stop = memgraph::utils::OnScopeExit{[&]() { running = false; }};
    uint16_t i = 0;
    while (i++ < 15000) {
      const auto json = in_memory->schema_info_.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
      // Possible schemas:
      static const auto no_labels_no_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":2,"labels":[],"properties":[]}]})");
      static const auto from_label_no_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L1"],"properties":[]}]})");
      static const auto to_label_no_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto both_labels_no_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":["L1"],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto no_labels_w_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "String"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":2,"labels":[],"properties":[]}]})");
      static const auto no_labels_w_prop2 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "Integer"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":2,"labels":[],"properties":[]}]})");
      static const auto no_labels_w_prop3 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "Boolean"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":2,"labels":[],"properties":[]}]})");
      static const auto from_label_w_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "String"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L1"],"properties":[]}]})");
      static const auto from_label_w_prop2 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "Integer"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L1"],"properties":[]}]})");
      static const auto from_label_w_prop3 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":[],"properties":[{"count" : 1, "filling_factor" : 100.0, "key" :
          "p1", "types" : [ {"count" : 1, "type" : "Boolean"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L1"],"properties":[]}]})");
      static const auto to_label_w_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "String"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto to_label_w_prop2 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "Integer"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto to_label_w_prop3 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "Boolean"}
          ]}],"start_node_labels":[],"type":"E1"}],"nodes":[{"count":1,"labels":[],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto both_labels_w_prop = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "String"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":["L1"],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto both_labels_w_prop2 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "Integer"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":["L1"],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");
      static const auto both_labels_w_prop3 = nlohmann::json::parse(
          R"({"edges":[{"count":1,"end_node_labels":["L2"],"properties":[{"count" : 1, "filling_factor" : 100.0,
          "key" : "p1", "types" : [ {"count" : 1, "type" : "Boolean"}
          ]}],"start_node_labels":["L1"],"type":"E1"}],"nodes":[{"count":1,"labels":["L1"],"properties":[]},{"count":1,"labels":["L2"],"properties":[]}]})");

      static const std::array<nlohmann::json, 16> possible_schemas = {
          no_labels_no_prop, from_label_no_prop, to_label_no_prop, both_labels_no_prop,
          no_labels_w_prop,  from_label_w_prop,  to_label_w_prop,  both_labels_w_prop,
          no_labels_w_prop2, from_label_w_prop2, to_label_w_prop2, both_labels_w_prop2,
          no_labels_w_prop3, from_label_w_prop3, to_label_w_prop3, both_labels_w_prop3};

      auto itr = std::find_if(possible_schemas.begin(), possible_schemas.end(), [&json](auto &in) {
        // Support no edges as well
        if (json["edges"].empty()) {
          return ConfrontJSON(json["nodes"], in["nodes"]);
        }
        return ConfrontJSON(json, in);
      });

      ASSERT_NE(itr, possible_schemas.end()) << json;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  };

  auto t1 = std::jthread(modify_labels);
  auto t2 = std::jthread(modify_edge);
  auto t3 = std::jthread(modify_edge2);
  auto t4 = std::jthread(read_schema);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTest, AllPropertyTypes) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto p1 = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto p3 = in_memory->NameToProperty("p3");
  auto p4 = in_memory->NameToProperty("p4");
  auto p5 = in_memory->NameToProperty("p5");
  auto p6 = in_memory->NameToProperty("p6");
  auto p7 = in_memory->NameToProperty("p7");
  auto p8 = in_memory->NameToProperty("p8");
  auto p9 = in_memory->NameToProperty("p9");
  auto p10 = in_memory->NameToProperty("p10");
  auto p11 = in_memory->NameToProperty("p11");
  auto p12 = in_memory->NameToProperty("p12");
  auto p13 = in_memory->NameToProperty("p13");
  auto p14 = in_memory->NameToProperty("p14");
  auto p15 = in_memory->NameToProperty("p15");
  auto p16 = in_memory->NameToProperty("p16");

  ASSERT_TRUE(in_memory->enum_store_.RegisterEnum("enum1", {"a", "b"}).HasValue());
  ASSERT_TRUE(in_memory->enum_store_.RegisterEnum("enum2", {"1", "2"}).HasValue());
  auto enum1 = *in_memory->enum_store_.ToEnum("enum1", "a");
  auto enum2 = *in_memory->enum_store_.ToEnum("enum2", "2");

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create vertex and add all property types
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    ASSERT_FALSE(v1.SetProperty(p1, PropertyValue{}).HasError());
    ASSERT_FALSE(v1.SetProperty(p2, PropertyValue{true}).HasError());
    ASSERT_FALSE(v1.SetProperty(p3, PropertyValue{123}).HasError());
    ASSERT_FALSE(v1.SetProperty(p4, PropertyValue{45.678}).HasError());
    ASSERT_FALSE(v1.SetProperty(p5, PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(v1.SetProperty(p6, PropertyValue{PropertyValue::list_t{}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p7, PropertyValue{PropertyValue::map_t{}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p8, PropertyValue{TemporalData{TemporalType::Date, 1}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p9, PropertyValue{TemporalData{TemporalType::Duration, 1}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p10, PropertyValue{TemporalData{TemporalType::LocalTime, 1}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p11, PropertyValue{TemporalData{TemporalType::LocalDateTime, 1}}).HasError());
    ASSERT_FALSE(
        v1.SetProperty(p12,
                       PropertyValue{ZonedTemporalData{
                           ZonedTemporalType::ZonedDateTime, {}, memgraph::utils::Timezone{std::chrono::minutes{0}}}})
            .HasError());
    ASSERT_FALSE(v1.SetProperty(p13, PropertyValue{enum1}).HasError());
    ASSERT_FALSE(v1.SetProperty(p14, PropertyValue{enum2}).HasError());
    ASSERT_FALSE(v1.SetProperty(p15, PropertyValue{Point2d{}}).HasError());
    ASSERT_FALSE(v1.SetProperty(p16, PropertyValue{Point3d{}}).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);

    const auto &node_json = json["nodes"];
    ASSERT_EQ(node_json.size(), 1);
    const auto &prop_json = node_json[0]["properties"];
    ASSERT_EQ(prop_json.size(), 15);

    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p2"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Boolean"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p3"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Integer"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", 100.0},
                                                {"key", "p4"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Float"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p5"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "String"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", 100.0},
                                                {"key", "p6"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "List"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", 100.0},
                                                {"key", "p7"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Map"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", 100.0},
                                                {"key", "p8"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Date"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p9"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Duration"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p10"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "LocalTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p11"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "LocalDateTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p12"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "ZonedDateTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p13"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Enum::enum1"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p14"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Enum::enum2"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p15"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Point2D"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", 100.0},
                                  {"key", "p16"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Point3D"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(SchemaInfoTestWEdgeProp, AllPropertyTypes) {
  auto *in_memory = static_cast<memgraph::storage::InMemoryStorage *>(this->storage.get());
  auto &schema_info = in_memory->schema_info_;

  auto e = in_memory->NameToEdgeType("E");
  auto p1 = in_memory->NameToProperty("p1");
  auto p2 = in_memory->NameToProperty("p2");
  auto p3 = in_memory->NameToProperty("p3");
  auto p4 = in_memory->NameToProperty("p4");
  auto p5 = in_memory->NameToProperty("p5");
  auto p6 = in_memory->NameToProperty("p6");
  auto p7 = in_memory->NameToProperty("p7");
  auto p8 = in_memory->NameToProperty("p8");
  auto p9 = in_memory->NameToProperty("p9");
  auto p10 = in_memory->NameToProperty("p10");
  auto p11 = in_memory->NameToProperty("p11");
  auto p12 = in_memory->NameToProperty("p12");
  auto p13 = in_memory->NameToProperty("p13");
  auto p14 = in_memory->NameToProperty("p14");
  auto p15 = in_memory->NameToProperty("p15");
  auto p16 = in_memory->NameToProperty("p16");

  ASSERT_TRUE(in_memory->enum_store_.RegisterEnum("enum1", {"a", "b"}).HasValue());
  ASSERT_TRUE(in_memory->enum_store_.RegisterEnum("enum2", {"1", "2"}).HasValue());
  auto enum1 = *in_memory->enum_store_.ToEnum("enum1", "a");
  auto enum2 = *in_memory->enum_store_.ToEnum("enum2", "2");

  auto set_properties = [&](auto &obj) {
    ASSERT_FALSE(obj.SetProperty(p1, PropertyValue{}).HasError());
    ASSERT_FALSE(obj.SetProperty(p2, PropertyValue{true}).HasError());
    ASSERT_FALSE(obj.SetProperty(p3, PropertyValue{123}).HasError());
    ASSERT_FALSE(obj.SetProperty(p4, PropertyValue{45.678}).HasError());
    ASSERT_FALSE(obj.SetProperty(p5, PropertyValue{"abc"}).HasError());
    ASSERT_FALSE(obj.SetProperty(p6, PropertyValue{PropertyValue::list_t{}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p7, PropertyValue{PropertyValue::map_t{}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p8, PropertyValue{TemporalData{TemporalType::Date, 1}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p9, PropertyValue{TemporalData{TemporalType::Duration, 1}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p10, PropertyValue{TemporalData{TemporalType::LocalTime, 1}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p11, PropertyValue{TemporalData{TemporalType::LocalDateTime, 1}}).HasError());
    ASSERT_FALSE(
        obj.SetProperty(p12,
                        PropertyValue{ZonedTemporalData{
                            ZonedTemporalType::ZonedDateTime, {}, memgraph::utils::Timezone{std::chrono::minutes{0}}}})
            .HasError());
    ASSERT_FALSE(obj.SetProperty(p13, PropertyValue{enum1}).HasError());
    ASSERT_FALSE(obj.SetProperty(p14, PropertyValue{enum2}).HasError());
    ASSERT_FALSE(obj.SetProperty(p15, PropertyValue{Point2d{}}).HasError());
    ASSERT_FALSE(obj.SetProperty(p16, PropertyValue{Point3d{}}).HasError());
  };

  auto check_json = [](const auto &json, float fill_factor) {
    ASSERT_EQ(json.size(), 1);
    const auto &prop_json = json[0]["properties"];
    ASSERT_EQ(prop_json.size(), 15);

    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p2"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Boolean"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p3"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Integer"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", fill_factor},
                                                {"key", "p4"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Float"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p5"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "String"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", fill_factor},
                                                {"key", "p6"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "List"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", fill_factor},
                                                {"key", "p7"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Map"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop = nlohmann::json::object({{"count", 1},
                                                {"filling_factor", fill_factor},
                                                {"key", "p8"},
                                                {"types", nlohmann::json::array({{{"count", 1}, {"type", "Date"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p9"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Duration"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p10"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "LocalTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p11"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "LocalDateTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p12"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "ZonedDateTime"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p13"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Enum::enum1"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p14"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Enum::enum2"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p15"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Point2D"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
    {
      const auto prop =
          nlohmann::json::object({{"count", 1},
                                  {"filling_factor", fill_factor},
                                  {"key", "p16"},
                                  {"types", nlohmann::json::array({{{"count", 1}, {"type", "Point3D"}}})}});
      EXPECT_TRUE(std::any_of(prop_json.begin(), prop_json.end(), [&](const auto &in) { return in == prop; }));
    }
  };

  // Empty
  {
    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);
    ASSERT_TRUE(json["nodes"].empty());
    ASSERT_TRUE(json["edges"].empty());
  }

  // create vertex and edge; add all property types
  {
    auto acc = in_memory->Access();
    auto v1 = acc->CreateVertex();
    auto v2 = acc->CreateVertex();
    auto edge = acc->CreateEdge(&v1, &v2, e);
    ASSERT_TRUE(edge.HasValue());

    set_properties(v1);
    set_properties(edge.GetValue());

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());

    const auto json = schema_info.ToJson(*in_memory->name_id_mapper_, in_memory->enum_store_);

    check_json(json["nodes"], 50.0);
    check_json(json["edges"], 100.0);
  }
}
