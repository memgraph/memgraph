// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fmt/format.h>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/storage.hpp"
#include "storage/v3/temporal.hpp"
#include "storage/v3/vertex_id.hpp"

using testing::Pair;
using testing::UnorderedElementsAre;
using SchemaType = memgraph::common::SchemaType;

namespace memgraph::storage::v3::tests {

uint64_t GetTransactionId() {
  static uint64_t transaction_id = 0;
  return transaction_id++;
}

class ShardRSMTest : public testing::Test {
 private:
  NameIdMapper id_mapper_{{{1, "primary_label"},
                           {2, "primary_label2"},
                           {3, "label"},
                           {4, "primary_prop1"},
                           {5, "primary_prop2"},
                           {6, "prop"}}};

 protected:
  ShardRSMTest() {
    PropertyValue min_pk(static_cast<int64_t>(0));
    std::vector<PropertyValue> min_prim_key = {min_pk};
    PropertyValue max_pk(static_cast<int64_t>(10000000));
    std::vector<PropertyValue> max_prim_key = {max_pk};

    auto shard_ptr1 = std::make_unique<Shard>(primary_label, min_prim_key, max_prim_key, std::vector{schema_prop});
    shard_ptr1->StoreMapping({{1, "primary_label"},
                              {2, "primary_label2"},
                              {3, "label"},
                              {4, "primary_prop1"},
                              {5, "primary_prop2"},
                              {6, "prop"}});
    shard_ptr1->CreateSchema(primary_label2, {{primary_property2, SchemaType::INT}});
    shard_rsm = std::make_unique<ShardRsm>(std::move(shard_ptr1));
  }

  LabelId NameToLabel(const std::string &name) { return LabelId::FromUint(id_mapper_.NameToId(name)); }

  PropertyId NameToProperty(const std::string &name) { return PropertyId::FromUint(id_mapper_.NameToId(name)); }

  auto Commit(const auto &req) {
    const coordinator::Hlc commit_timestamp{GetTransactionId()};
    msgs::CommitRequest commit_req;
    commit_req.transaction_id = req.transaction_id;
    commit_req.commit_timestamp = commit_timestamp;
    return shard_rsm->Apply(commit_req);
  }

  void CreateVertex(const msgs::PrimaryKey &primary_key, const std::vector<msgs::Label> labels,
                    const std::vector<std::pair<msgs::PropertyId, msgs::Value>> &properties) {
    msgs::NewVertex vertex = {labels, primary_key, properties};
    msgs::CreateVerticesRequest create_req;
    create_req.new_vertices = {vertex};
    create_req.new_vertices = {vertex};
    create_req.transaction_id.logical_id = GetTransactionId();

    auto write_res = shard_rsm->Apply(create_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CreateVerticesResponse>(write_res));

    auto commit_res = Commit(create_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    ASSERT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }

  void AssertVertexExists(const msgs::PrimaryKey &primary_key, const std::vector<msgs::Label> &labels,
                          const std::vector<std::pair<msgs::PropertyId, msgs::Value>> &properties) {
    msgs::ScanVerticesRequest scan_req;
    scan_req.props_to_return = std::nullopt;
    scan_req.start_id = msgs::VertexId{msgs::Label{.id = primary_label}, primary_key};
    scan_req.storage_view = msgs::StorageView::OLD;
    scan_req.transaction_id.logical_id = GetTransactionId();

    // Make request
    auto maybe_read_res = shard_rsm->Read(scan_req);
    ASSERT_TRUE(std::holds_alternative<msgs::ScanVerticesResponse>(maybe_read_res));
    const auto read_res = std::get<msgs::ScanVerticesResponse>(maybe_read_res);
    EXPECT_TRUE(read_res.success);
    EXPECT_EQ(read_res.results.size(), 1);

    // Read results
    const auto res = read_res.results[0];
    const auto vtx_id = msgs::VertexId{msgs::Label{.id = primary_label}, primary_key};
    EXPECT_EQ(res.vertex.id, vtx_id);
    EXPECT_EQ(res.vertex.labels, labels);
    EXPECT_EQ(res.props, properties);
  }

  LabelId primary_label{NameToLabel("primary_label")};
  LabelId primary_label2{NameToLabel("primary_label2")};
  LabelId label{NameToLabel("label")};
  PropertyId primary_property1{NameToProperty("primary_prop1")};
  PropertyId primary_property2{NameToProperty("primary_prop2")};
  PropertyId prop{NameToProperty("prop")};
  SchemaProperty schema_prop{primary_property1, SchemaType::INT};
  std::unique_ptr<ShardRsm> shard_rsm;
};

TEST_F(ShardRSMTest, TestUpdateVertexSecondaryProperty) {
  const msgs::Value primary_key_val{static_cast<int64_t>(1)};
  const msgs::PrimaryKey pk{primary_key_val};

  // Create Vertex
  CreateVertex(pk, {}, {});

  // Add property prop
  static constexpr int64_t updated_vertex_id{10};
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices =
        std::vector<msgs::UpdateVertex>{{pk, {}, {}, {{msgs::PropertyId(prop), msgs::Value(updated_vertex_id)}}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}, {prop, msgs::Value(updated_vertex_id)}});

  // Update property prop
  static constexpr int64_t updated_vertex_id_2{101};
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices =
        std::vector<msgs::UpdateVertex>{{pk, {}, {}, {{msgs::PropertyId(prop), msgs::Value(updated_vertex_id_2)}}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    ASSERT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
    AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}, {prop, msgs::Value(updated_vertex_id_2)}});
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}, {prop, msgs::Value(updated_vertex_id_2)}});

  // Remove property prop
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices =
        std::vector<msgs::UpdateVertex>{{pk, {}, {}, {{msgs::PropertyId(prop), msgs::Value()}}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}});
}

TEST_F(ShardRSMTest, TestUpdateVertexPrimaryProperty) {
  const msgs::Value primary_key_val{static_cast<int64_t>(1)};
  const msgs::PrimaryKey pk{primary_key_val};

  // Create Vertex
  CreateVertex(pk, {}, {});

  // Try to update primary property
  static constexpr int64_t updated_vertex_id{10};
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{
        {pk, {}, {}, {{msgs::PropertyId(primary_property1), msgs::Value(updated_vertex_id)}}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_FALSE(std::get<msgs::UpdateVerticesResponse>(write_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}});
  // Try to update primary property of another schema
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{
        {pk, {}, {}, {{msgs::PropertyId(primary_property2), msgs::Value(updated_vertex_id)}}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }
  AssertVertexExists(pk, {},
                     {{primary_property1, primary_key_val}, {primary_property2, msgs::Value(updated_vertex_id)}});
}

TEST_F(ShardRSMTest, TestUpdateSecondaryLabel) {
  const msgs::Value primary_key_val{static_cast<int64_t>(1)};
  const msgs::PrimaryKey pk{primary_key_val};

  // Create Vertex
  CreateVertex(pk, {}, {});

  // Add label label
  const msgs::Label secondary_label{label};
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{{pk, {label}, {}, {}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    ASSERT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }
  AssertVertexExists(pk, {secondary_label}, {{primary_property1, primary_key_val}});

  // Remove primary label
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{{pk, {}, {label}, {}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_TRUE(std::get<msgs::UpdateVerticesResponse>(write_res).success);

    const auto commit_res = Commit(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::CommitResponse>(commit_res));
    EXPECT_TRUE(std::get<msgs::CommitResponse>(commit_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}});
}

TEST_F(ShardRSMTest, TestUpdatePrimaryLabel) {
  const msgs::Value primary_key_val{static_cast<int64_t>(1)};
  const msgs::PrimaryKey pk{primary_key_val};

  // Create Vertex
  CreateVertex(pk, {}, {});

  // Remove primary label
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{{pk, {}, {primary_label}, {}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_FALSE(std::get<msgs::UpdateVerticesResponse>(write_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}});

  // Add different primary label
  {
    msgs::UpdateVerticesRequest update_req;
    update_req.transaction_id.logical_id = GetTransactionId();

    update_req.update_vertices = std::vector<msgs::UpdateVertex>{{pk, {primary_label2}, {}, {}}};

    const auto write_res = shard_rsm->Apply(update_req);
    ASSERT_TRUE(std::holds_alternative<msgs::UpdateVerticesResponse>(write_res));
    EXPECT_FALSE(std::get<msgs::UpdateVerticesResponse>(write_res).success);
  }
  AssertVertexExists(pk, {}, {{primary_property1, primary_key_val}});
}

}  // namespace memgraph::storage::v3::tests
