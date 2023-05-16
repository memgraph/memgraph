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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cassert>
#include <exception>
#include <string>
#include <unordered_set>

#include "query/common.hpp"
#include "query/db_accessor.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/rocksdb.hpp"

class RocksDBStorageTest : public ::testing::TestWithParam<bool> {
 public:
  RocksDBStorageTest() { storage = std::unique_ptr<memgraph::storage::Storage>(new memgraph::storage::DiskStorage()); }

  ~RocksDBStorageTest() override {}

 protected:
  std::unique_ptr<memgraph::storage::Storage> storage;
};

/// Serialize vertex which contains only GID.
TEST_F(RocksDBStorageTest, SerializeVertexGID) {
  auto acc = storage->Access();
  auto vertex = acc->CreateVertex();
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_), "|" + memgraph::utils::SerializeIdType(gid));
}

/// Serialize vertex with gid and its single label.
TEST_F(RocksDBStorageTest, SerializeVertexGIDLabels) {
  auto acc = storage->Access();
  auto vertex = acc->CreateVertex();
  auto ser_player_label = acc->NameToLabel("Player");
  auto player_result = vertex.AddLabel(ser_player_label);
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_),
            std::to_string(ser_player_label.AsInt()) + "|" + memgraph::utils::SerializeIdType(gid));
}

/// Serialize vertex with gid and its multiple labels.
TEST_F(RocksDBStorageTest, SerializeVertexGIDMultipleLabels) {
  auto acc = storage->Access();
  auto vertex = acc->CreateVertex();
  auto ser_player_label = acc->NameToLabel("Player");
  auto ser_person_label = acc->NameToLabel("Person");
  auto ser_ball_label = acc->NameToLabel("Ball");
  // NOLINTNEXTLINE
  auto player_res = vertex.AddLabel(ser_player_label);
  auto person_res = vertex.AddLabel(ser_person_label);
  auto ball_res = vertex.AddLabel(ser_ball_label);
  auto gid = vertex.Gid();
  ASSERT_EQ(memgraph::utils::SerializeVertex(*vertex.vertex_),
            std::to_string(ser_player_label.AsInt()) + "," + std::to_string(ser_person_label.AsInt()) + "," +
                std::to_string(ser_ball_label.AsInt()) + "|" + memgraph::utils::SerializeIdType(gid));
}

/// Serialize edge.
TEST_F(RocksDBStorageTest, SerializeEdge) {
  auto acc = storage->Access();
  auto vertex1 = acc->CreateVertex();
  auto vertex2 = acc->CreateVertex();
  auto edge = acc->CreateEdge(&vertex1, &vertex2, acc->NameToEdgeType("KNOWS"));
  auto gid = edge->Gid();
  auto ser_result = memgraph::utils::SerializeEdge(vertex1.Gid(), vertex2.Gid(), edge->EdgeType(), edge->edge_.ptr);
  ASSERT_EQ(ser_result.first,
            memgraph::utils::SerializeIdType(vertex1.Gid()) + "|" + memgraph::utils::SerializeIdType(vertex2.Gid()) +
                "|0|" + std::to_string(edge->EdgeType().AsInt()) + "|" + memgraph::utils::SerializeIdType(gid));
  ASSERT_EQ(ser_result.second,
            memgraph::utils::SerializeIdType(vertex2.Gid()) + "|" + memgraph::utils::SerializeIdType(vertex1.Gid()) +
                "|1|" + std::to_string(edge->EdgeType().AsInt()) + "|" + memgraph::utils::SerializeIdType(gid));
}

TEST_F(RocksDBStorageTest, DeserializeVertex) {
  // NOTE: This test would fail in the case of snaphsot isolation because of the way in which RocksDB
  // serializes commit timestamp.
  auto serialized_vertex = "1|1";
  auto acc = storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  auto acc_ptr = static_cast<memgraph::storage::DiskStorage::DiskAccessor *>(acc.get());
  auto vertex = acc_ptr->DeserializeVertex(serialized_vertex, "garbage");
  ASSERT_EQ(vertex->Gid().AsInt(), 1);
  ASSERT_EQ(*vertex->HasLabel(memgraph::storage::LabelId::FromUint(1), memgraph::storage::View::OLD), true);
}

TEST_F(RocksDBStorageTest, DeserializeEdge) {
  // NOTE: This test would fail in the case of snaphsot isolation because of the way in which RocksDB
  // serializes commit timestamp.
  auto acc = storage->Access(memgraph::storage::IsolationLevel::READ_UNCOMMITTED);
  auto vertex1 = acc->CreateVertex();
  auto vertex2 = acc->CreateVertex();
  auto serialized_edge = fmt::format("{}|{}|0|1|2", vertex1.Gid().AsInt(), vertex2.Gid().AsInt());
  auto acc_ptr = static_cast<memgraph::storage::DiskStorage::DiskAccessor *>(acc.get());
  auto edge = acc_ptr->DeserializeEdge(serialized_edge, "garbage");
  ASSERT_EQ(edge->Gid().AsInt(), 2);
  ASSERT_EQ(edge->EdgeType().AsInt(), 1);
  ASSERT_EQ(edge->from_vertex_->gid.AsInt(), vertex1.Gid().AsInt());
  ASSERT_EQ(edge->to_vertex_->gid.AsInt(), vertex2.Gid().AsInt());
}
