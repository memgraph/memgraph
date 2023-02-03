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

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "coordinator/hybrid_logical_clock.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_id.hpp"

using testing::Pair;
using testing::UnorderedElementsAre;

namespace memgraph::storage::v3::tests {

class ShardSplitTest : public testing::Test {
 protected:
  void SetUp() override {
    storage.StoreMapping(
        {{1, "label"}, {2, "property"}, {3, "edge_property"}, {4, "secondary_label"}, {5, "secondary_prop"}});
  }

  const PropertyId primary_property{PropertyId::FromUint(2)};
  const PropertyId secondary_property{PropertyId::FromUint(5)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};
  const std::vector<PropertyValue> min_pk{PropertyValue{0}};
  const LabelId primary_label{LabelId::FromUint(1)};
  const LabelId secondary_label{LabelId::FromUint(4)};
  const EdgeTypeId edge_type_id{EdgeTypeId::FromUint(3)};
  Shard storage{primary_label, min_pk, std::nullopt /*max_primary_key*/, schema_property_vector};

  coordinator::Hlc last_hlc{0, io::Time{}};

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(1);
    return last_hlc;
  }

  void AssertSplittedShard(SplitData &&splitted_data, const int split_value) {
    auto shard = Shard::FromSplitData(std::move(splitted_data));
    auto acc = shard->Access(GetNextHlc());
    for (int i{0}; i < split_value; ++i) {
      EXPECT_FALSE(acc.FindVertex(PrimaryKey{{PropertyValue(i)}}, View::OLD).has_value());
    }
    for (int i{split_value}; i < split_value * 2; ++i) {
      const auto vtx = acc.FindVertex(PrimaryKey{{PropertyValue(i)}}, View::OLD);
      ASSERT_TRUE(vtx.has_value());
      EXPECT_TRUE(vtx->InEdges(View::OLD)->size() == 1 || vtx->OutEdges(View::OLD)->size() == 1);
    }
  }
};

void AssertEqVertexContainer(const VertexContainer &actual, const VertexContainer &expected) {
  ASSERT_EQ(actual.size(), expected.size());

  auto expected_it = expected.begin();
  auto actual_it = actual.begin();
  while (expected_it != expected.end()) {
    EXPECT_EQ(actual_it->first, expected_it->first);
    EXPECT_EQ(actual_it->second.deleted, expected_it->second.deleted);
    EXPECT_EQ(actual_it->second.labels, expected_it->second.labels);

    auto *expected_delta = expected_it->second.delta;
    auto *actual_delta = actual_it->second.delta;
    // This asserts delta chain
    while (expected_delta != nullptr) {
      EXPECT_EQ(actual_delta->action, expected_delta->action);
      EXPECT_EQ(actual_delta->id, expected_delta->id);

      switch (expected_delta->action) {
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL: {
          EXPECT_EQ(actual_delta->label, expected_delta->label);
          break;
        }
        case Delta::Action::SET_PROPERTY: {
          EXPECT_EQ(actual_delta->property.key, expected_delta->property.key);
          EXPECT_EQ(actual_delta->property.value, expected_delta->property.value);
          break;
        }
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::REMOVE_OUT_EDGE: {
          break;
        }
      }

      const auto expected_prev = expected_delta->prev.Get();
      const auto actual_prev = actual_delta->prev.Get();
      switch (expected_prev.type) {
        case PreviousPtr::Type::NULLPTR: {
          ASSERT_EQ(actual_prev.type, PreviousPtr::Type::NULLPTR) << "Expected type is nullptr!";
          break;
        }
        case PreviousPtr::Type::DELTA: {
          ASSERT_EQ(actual_prev.type, PreviousPtr::Type::DELTA) << "Expected type is delta!";
          EXPECT_EQ(actual_prev.delta->action, expected_prev.delta->action);
          EXPECT_EQ(actual_prev.delta->id, expected_prev.delta->id);
          break;
        }
        case v3::PreviousPtr::Type::EDGE: {
          ASSERT_EQ(actual_prev.type, PreviousPtr::Type::EDGE) << "Expected type is edge!";
          EXPECT_EQ(actual_prev.edge->gid, expected_prev.edge->gid);
          break;
        }
        case v3::PreviousPtr::Type::VERTEX: {
          ASSERT_EQ(actual_prev.type, PreviousPtr::Type::VERTEX) << "Expected type is vertex!";
          EXPECT_EQ(actual_prev.vertex->first, expected_prev.vertex->first);
          break;
        }
      }

      expected_delta = expected_delta->next;
      actual_delta = actual_delta->next;
    }
    EXPECT_EQ(expected_delta, nullptr);
    EXPECT_EQ(actual_delta, nullptr);
    ++expected_it;
    ++actual_it;
  }
}

void AssertEqDeltaLists(const std::list<Delta> &actual, const std::list<Delta> &expected) {
  ASSERT_EQ(actual.size(), expected.size());
  auto actual_it = actual.begin();
  auto expected_it = expected.begin();
  while (actual_it != actual.end()) {
    EXPECT_EQ(actual_it->action, expected_it->action);
    EXPECT_EQ(actual_it->id, expected_it->id);
    EXPECT_NE(&*actual_it, &*expected_it) << "Deltas must be different objects!";
  }
}

void AddDeltaToDeltaChain(Vertex *object, Delta *new_delta) {
  auto *delta_holder = GetDeltaHolder(object);

  new_delta->next = delta_holder->delta;
  new_delta->prev.Set(object);
  if (delta_holder->delta) {
    delta_holder->delta->prev.Set(new_delta);
  }
  delta_holder->delta = new_delta;
}

TEST_F(ShardSplitTest, TestBasicSplitWithVertices) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(
      acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(5)}, {{secondary_property, PropertyValue(121)}})
          .HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());
  auto current_hlc = GetNextHlc();
  acc.Commit(current_hlc);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 0);
  EXPECT_EQ(splitted_data.transactions.size(), 1);
  EXPECT_EQ(splitted_data.label_indices.size(), 0);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 0);

  CommitInfo commit_info{.start_or_commit_timestamp = current_hlc};
  Delta delta_delete1{Delta::DeleteObjectTag{}, &commit_info, 4, 1};
  Delta delta_delete2{Delta::DeleteObjectTag{}, &commit_info, 5, 2};
  Delta delta_remove_label{Delta::RemoveLabelTag{}, secondary_label, &commit_info, 7, 4};
  Delta delta_set_property{Delta::SetPropertyTag{}, secondary_property, PropertyValue(), &commit_info, 6, 4};
  Delta delta_delete3{Delta::DeleteObjectTag{}, &commit_info, 8, 3};

  VertexContainer expected_vertices;
  auto [it_4, inserted1] = expected_vertices.emplace(PrimaryKey{PropertyValue{4}}, VertexData(&delta_delete1));
  delta_delete1.prev.Set(&*it_4);
  auto [it_5, inserted2] = expected_vertices.emplace(PrimaryKey{PropertyValue{5}}, VertexData(&delta_delete2));
  delta_delete2.prev.Set(&*it_5);
  auto [it_6, inserted3] = expected_vertices.emplace(PrimaryKey{PropertyValue{6}}, VertexData(&delta_delete3));
  delta_delete3.prev.Set(&*it_6);
  it_5->second.labels.push_back(secondary_label);
  AddDeltaToDeltaChain(&*it_5, &delta_set_property);
  AddDeltaToDeltaChain(&*it_5, &delta_remove_label);

  AssertEqVertexContainer(splitted_data.vertices, expected_vertices);

  // This is to ensure that the transaction that we have don't point to invalid
  // object on the other shard
  std::list<Delta> expected_deltas;
  expected_deltas.emplace_back(Delta::DeleteObjectTag{}, &commit_info, 4, 1);
  expected_deltas.emplace_back(Delta::DeleteObjectTag{}, &commit_info, 5, 2);
  expected_deltas.emplace_back(Delta::RemoveLabelTag{}, secondary_label, &commit_info, 7, 4);
  expected_deltas.emplace_back(Delta::SetPropertyTag{}, secondary_property, PropertyValue(), &commit_info, 6, 4);
  expected_deltas.emplace_back(Delta::DeleteObjectTag{}, &commit_info, 8, 3);
  // AssertEqDeltaLists(splitted_data.transactions.begin()->second->deltas, expected_deltas);
}

TEST_F(ShardSplitTest, TestBasicSplitVerticesAndEdges) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(5)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());

  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(5)}}, edge_type_id, Gid::FromUint(1))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(4)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(6)}}, edge_type_id, Gid::FromUint(2))
                   .HasError());

  auto current_hlc = GetNextHlc();
  acc.Commit(current_hlc);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 2);
  EXPECT_EQ(splitted_data.transactions.size(), 1);
  EXPECT_EQ(splitted_data.label_indices.size(), 0);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 0);

  CommitInfo commit_info{.start_or_commit_timestamp = current_hlc};
  Delta delta_delete1{Delta::DeleteObjectTag{}, &commit_info, 12, 1};
  Delta delta_delete2{Delta::DeleteObjectTag{}, &commit_info, 13, 1};
  Delta delta_delete3{Delta::DeleteObjectTag{}, &commit_info, 14, 1};
  Delta delta_add_in_edge1{Delta::RemoveInEdgeTag{},
                           edge_type_id,
                           VertexId{primary_label, {PropertyValue(1)}},
                           EdgeRef{Gid::FromUint(1)},
                           &commit_info,
                           17,
                           1};
  Delta delta_add_out_edge2{Delta::RemoveOutEdgeTag{},
                            edge_type_id,
                            VertexId{primary_label, {PropertyValue(6)}},
                            EdgeRef{Gid::FromUint(2)},
                            &commit_info,
                            19,
                            1};
  Delta delta_add_in_edge2{Delta::RemoveInEdgeTag{},
                           edge_type_id,
                           VertexId{primary_label, {PropertyValue(4)}},
                           EdgeRef{Gid::FromUint(2)},
                           &commit_info,
                           20,
                           1};
  VertexContainer expected_vertices;
  auto [vtx4, inserted4] = expected_vertices.emplace(PrimaryKey{PropertyValue{4}}, VertexData(&delta_delete1));
  auto [vtx5, inserted5] = expected_vertices.emplace(PrimaryKey{PropertyValue{5}}, VertexData(&delta_delete2));
  auto [vtx6, inserted6] = expected_vertices.emplace(PrimaryKey{PropertyValue{6}}, VertexData(&delta_delete3));
  AddDeltaToDeltaChain(&*vtx4, &delta_add_out_edge2);
  AddDeltaToDeltaChain(&*vtx5, &delta_add_in_edge1);
  AddDeltaToDeltaChain(&*vtx6, &delta_add_in_edge2);

  AssertEqVertexContainer(splitted_data.vertices, expected_vertices);
}

TEST_F(ShardSplitTest, TestBasicSplitBeforeCommit) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(5)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());

  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(2)}}, edge_type_id, Gid::FromUint(0))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(5)}}, edge_type_id, Gid::FromUint(1))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(4)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(6)}}, edge_type_id, Gid::FromUint(2))
                   .HasError());

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 2);
  EXPECT_EQ(splitted_data.transactions.size(), 1);
  EXPECT_EQ(splitted_data.label_indices.size(), 0);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 0);
}

TEST_F(ShardSplitTest, TestBasicSplitWithCommitedAndOngoingTransactions) {
  {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(1)}, {}).HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(5)}, {}).HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(6)}, {}).HasError());

    acc.Commit(GetNextHlc());
  }
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(2)}}, edge_type_id, Gid::FromUint(0))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(1)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(5)}}, edge_type_id, Gid::FromUint(1))
                   .HasError());
  EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(4)}},
                              VertexId{primary_label, PrimaryKey{PropertyValue(6)}}, edge_type_id, Gid::FromUint(2))
                   .HasError());

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);
  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 2);
  EXPECT_EQ(splitted_data.transactions.size(), 2);
  EXPECT_EQ(splitted_data.label_indices.size(), 0);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 0);
}

TEST_F(ShardSplitTest, TestBasicSplitWithLabelIndex) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(1)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(5)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(6)}, {}).HasError());
  acc.Commit(GetNextHlc());
  storage.CreateIndex(secondary_label);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);

  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 0);
  EXPECT_EQ(splitted_data.transactions.size(), 1);
  EXPECT_EQ(splitted_data.label_indices.size(), 1);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 0);
}

TEST_F(ShardSplitTest, TestBasicSplitWithLabelPropertyIndex) {
  auto acc = storage.Access(GetNextHlc());
  EXPECT_FALSE(
      acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(1)}, {{secondary_property, PropertyValue(1)}})
          .HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(2)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(3)}, {}).HasError());
  EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(4)}, {}).HasError());
  EXPECT_FALSE(
      acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(5)}, {{secondary_property, PropertyValue(21)}})
          .HasError());
  EXPECT_FALSE(
      acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(6)}, {{secondary_property, PropertyValue(22)}})
          .HasError());
  acc.Commit(GetNextHlc());
  storage.CreateIndex(secondary_label, secondary_property);

  auto splitted_data = storage.PerformSplit({PropertyValue(4)}, 2);

  EXPECT_EQ(splitted_data.vertices.size(), 3);
  EXPECT_EQ(splitted_data.edges->size(), 0);
  EXPECT_EQ(splitted_data.transactions.size(), 1);
  EXPECT_EQ(splitted_data.label_indices.size(), 0);
  EXPECT_EQ(splitted_data.label_property_indices.size(), 1);
}

TEST_F(ShardSplitTest, TestBigSplit) {
  int pk{0};
  for (int64_t i{0}; i < 10'000; ++i) {
    auto acc = storage.Access(GetNextHlc());
    EXPECT_FALSE(
        acc.CreateVertexAndValidate({secondary_label}, {PropertyValue(pk++)}, {{secondary_property, PropertyValue(i)}})
            .HasError());
    EXPECT_FALSE(acc.CreateVertexAndValidate({}, {PropertyValue(pk++)}, {}).HasError());

    EXPECT_FALSE(acc.CreateEdge(VertexId{primary_label, PrimaryKey{PropertyValue(pk - 2)}},
                                VertexId{primary_label, PrimaryKey{PropertyValue(pk - 1)}}, edge_type_id,
                                Gid::FromUint(pk))
                     .HasError());
    acc.Commit(GetNextHlc());
  }
  storage.CreateIndex(secondary_label, secondary_property);

  const auto split_value = pk / 2;
  auto splitted_data = storage.PerformSplit({PropertyValue(split_value)}, 2);

  // EXPECT_EQ(splitted_data.vertices.size(), 100000);
  // EXPECT_EQ(splitted_data.edges->size(), 50000);
  // EXPECT_EQ(splitted_data.transactions.size(), 50000);
  // EXPECT_EQ(splitted_data.label_indices.size(), 0);
  // EXPECT_EQ(splitted_data.label_property_indices.size(), 1);

  AssertSplittedShard(std::move(splitted_data), split_value);
}

}  // namespace memgraph::storage::v3::tests
