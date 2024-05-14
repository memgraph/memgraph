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

#include "storage/v2/vertex.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <atomic>

using namespace memgraph::storage;
using namespace testing;

TEST(VertexTest, ConstructorInitializesFields) {
  std::atomic<uint64_t> ts(1);
  int command_id = 2;
  Delta delta(Delta::DeleteObjectTag(), &ts, command_id);
  Gid gid = Gid::FromInt(3);
  Vertex vertex(gid, &delta);

  EXPECT_EQ(vertex.gid, gid);
  EXPECT_TRUE(vertex.labels.empty());
  EXPECT_TRUE(vertex.properties.Properties().empty());
  EXPECT_FALSE(vertex.deleted);
  EXPECT_EQ(vertex.delta->timestamp->load(), ts.load());
  EXPECT_EQ(vertex.delta->command_id, command_id);
  EXPECT_EQ(vertex.delta->action, Delta::Action::DELETE_OBJECT);
  EXPECT_FALSE(vertex.HasEdges());
  EXPECT_EQ(vertex.InEdgesSize(), 0);
  EXPECT_EQ(vertex.OutEdgesSize(), 0);
}

TEST(VertexTest, AddInEdge) {
  Vertex dst_vertex(Gid::FromInt(3), nullptr);

  Vertex src_vertex(Gid::FromInt(4), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(5);
  Gid edge_gid = Gid::FromInt(6);
  EdgeRef ref(edge_gid);

  dst_vertex.AddInEdge(type, &src_vertex, ref);

  EXPECT_TRUE(dst_vertex.HasEdges());
  EXPECT_EQ(dst_vertex.InEdgesSize(), 1);
  auto opt_edge_tuple = dst_vertex.GetInEdge(edge_gid, false);

  EXPECT_TRUE(opt_edge_tuple.has_value());
  auto &edge_tuple = *opt_edge_tuple;

  EXPECT_EQ(std::get<0>(edge_tuple), type);
  EXPECT_EQ(*std::get<1>(edge_tuple), src_vertex);
  EXPECT_EQ(std::get<2>(edge_tuple), ref);
}

TEST(VertexTest, AddInEdgeWithProperties) {
  Vertex src_vertex(Gid::FromInt(1), nullptr);

  Vertex dst_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);

  Edge edge(edge_gid, nullptr);
  EdgeRef ref(&edge);

  dst_vertex.AddInEdge(type, &src_vertex, ref);

  EXPECT_TRUE(dst_vertex.HasEdges());
  EXPECT_EQ(dst_vertex.InEdgesSize(), 1);
  auto opt_edge_tuple = dst_vertex.GetInEdge(edge_gid, true);
  EXPECT_TRUE(opt_edge_tuple.has_value());
  auto &edge_tuple = *opt_edge_tuple;

  EXPECT_EQ(std::get<0>(edge_tuple), type);
  EXPECT_EQ(*std::get<1>(edge_tuple), src_vertex);
  EXPECT_EQ(std::get<2>(edge_tuple), ref);
}

TEST(VertexTest, AddOutEdge) {
  Vertex src_vertex(Gid::FromInt(1), nullptr);

  Vertex dst_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);
  EdgeRef ref(edge_gid);

  src_vertex.AddOutEdge(type, &dst_vertex, ref);

  EXPECT_TRUE(src_vertex.HasEdges());
  EXPECT_EQ(src_vertex.OutEdgesSize(), 1);
  auto opt_edge_tuple = src_vertex.GetOutEdge(edge_gid, false);

  EXPECT_TRUE(opt_edge_tuple.has_value());
  auto &edge_tuple = *opt_edge_tuple;

  EXPECT_EQ(std::get<0>(edge_tuple), type);
  EXPECT_EQ(*std::get<1>(edge_tuple), dst_vertex);
  EXPECT_EQ(std::get<2>(edge_tuple), ref);
}

TEST(VertexTest, AddOutEdgeWithProperties) {
  Vertex src_vertex(Gid::FromInt(1), nullptr);

  Vertex dst_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);

  Edge edge(edge_gid, nullptr);
  EdgeRef ref(&edge);

  src_vertex.AddOutEdge(type, &dst_vertex, ref);

  EXPECT_TRUE(src_vertex.HasEdges());
  EXPECT_EQ(src_vertex.OutEdgesSize(), 1);
  auto opt_edge_tuple = src_vertex.GetOutEdge(edge_gid, true);
  EXPECT_TRUE(opt_edge_tuple.has_value());
  auto &edge_tuple = *opt_edge_tuple;

  EXPECT_EQ(std::get<0>(edge_tuple), type);
  EXPECT_EQ(*std::get<1>(edge_tuple), dst_vertex);
  EXPECT_EQ(std::get<2>(edge_tuple), ref);
}

TEST(VertexTest, RemoveInEdge) {
  Vertex dst_vertex(Gid::FromInt(1), nullptr);
  Vertex src_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);
  EdgeRef ref(edge_gid);

  dst_vertex.AddInEdge(type, &src_vertex, ref);

  EXPECT_EQ(dst_vertex.InEdgesSize(), 1);
  auto opt_edge_tuple = dst_vertex.GetInEdge(edge_gid, false);

  EXPECT_TRUE(dst_vertex.RemoveInEdge(*opt_edge_tuple));
  EXPECT_FALSE(dst_vertex.HasEdges());
  EXPECT_EQ(dst_vertex.InEdgesSize(), 0);
}

TEST(VertexTest, RemoveNotExistingInEdge) {
  Vertex vertex(Gid::FromInt(1), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(2);
  Gid edge_gid = Gid::FromInt(3);
  EdgeRef ref(edge_gid);

  EXPECT_FALSE(vertex.RemoveInEdge({type, &vertex, ref}));
}

TEST(VertexTest, RemoveOutEdge) {
  Vertex dst_vertex(Gid::FromInt(1), nullptr);
  Vertex src_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);
  EdgeRef ref(edge_gid);

  src_vertex.AddOutEdge(type, &dst_vertex, ref);

  EXPECT_EQ(src_vertex.OutEdgesSize(), 1);
  auto opt_edge_tuple = src_vertex.GetOutEdge(edge_gid, false);

  EXPECT_TRUE(src_vertex.RemoveOutEdge(*opt_edge_tuple));
  EXPECT_FALSE(src_vertex.HasEdges());
  EXPECT_EQ(src_vertex.OutEdgesSize(), 0);
}

TEST(VertexTest, RemoveNotExistingOutEdge) {
  Vertex vertex(Gid::FromInt(1), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(2);
  Gid edge_gid = Gid::FromInt(3);
  EdgeRef ref(edge_gid);

  EXPECT_FALSE(vertex.RemoveOutEdge({type, &vertex, ref}));
}

TEST(VertexTest, HasInEdge) {
  Vertex dst_vertex(Gid::FromInt(1), nullptr);
  Vertex src_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);
  EdgeRef ref(edge_gid);

  dst_vertex.AddInEdge(type, &src_vertex, ref);

  EXPECT_EQ(dst_vertex.InEdgesSize(), 1);
  auto opt_edge_tuple = dst_vertex.GetInEdge(edge_gid, false);

  EXPECT_TRUE(dst_vertex.HasInEdge(*opt_edge_tuple));
  EXPECT_TRUE(dst_vertex.HasInEdge(ref));
  EXPECT_TRUE(dst_vertex.HasEdges());
}

TEST(VertexTest, HasNotExistingInEdge) {
  Vertex vertex(Gid::FromInt(1), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(2);
  Gid edge_gid = Gid::FromInt(3);
  EdgeRef ref(edge_gid);

  EXPECT_FALSE(vertex.HasInEdge({type, &vertex, ref}));
  EXPECT_FALSE(vertex.HasInEdge(ref));
  EXPECT_FALSE(vertex.HasEdges());
}

TEST(VertexTest, HasOutEdge) {
  Vertex dst_vertex(Gid::FromInt(1), nullptr);
  Vertex src_vertex(Gid::FromInt(2), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(3);
  Gid edge_gid = Gid::FromInt(4);
  EdgeRef ref(edge_gid);

  src_vertex.AddOutEdge(type, &dst_vertex, ref);

  EXPECT_EQ(src_vertex.OutEdgesSize(), 1);
  auto opt_edge_tuple = src_vertex.GetOutEdge(edge_gid, false);

  EXPECT_TRUE(src_vertex.HasOutEdge(*opt_edge_tuple));
  EXPECT_TRUE(src_vertex.HasOutEdge(ref));
  EXPECT_TRUE(src_vertex.HasEdges());
}

TEST(VertexTest, HasNotExistingOutEdge) {
  Vertex vertex(Gid::FromInt(1), nullptr);
  EdgeTypeId type = EdgeTypeId::FromInt(2);
  Gid edge_gid = Gid::FromInt(3);
  EdgeRef ref(edge_gid);

  EXPECT_FALSE(vertex.HasOutEdge({type, &vertex, ref}));
  EXPECT_FALSE(vertex.HasOutEdge(ref));
  EXPECT_FALSE(vertex.HasEdges());
}

// TEST(VertexTest, PopBackInEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddInEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.PopBackInEdge();
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);
//   EXPECT_TRUE(vertex.in_edges.empty());

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, PopBackOutEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddOutEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.PopBackOutEdge();
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);
//   EXPECT_TRUE(vertex.out_edges.empty());

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, ChangeInEdgeType) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddInEdge(type, other_vertex, ref);

//   EdgeTypeId new_type = 2;
//   EXPECT_TRUE(vertex.ChangeInEdgeType({type, other_vertex, ref}, new_type));
//   EXPECT_EQ(std::get<0>(vertex.in_edges[0]), new_type);

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, ChangeOutEdgeType) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddOutEdge(type, other_vertex, ref);

//   EdgeTypeId new_type = 2;
//   EXPECT_TRUE(vertex.ChangeOutEdgeType({type, other_vertex, ref}, new_type));
//   EXPECT_EQ(std::get<0>(vertex.out_edges[0]), new_type);

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, GetInEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddInEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.GetInEdge(ref.gid, false);
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, GetOutEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   vertex.AddOutEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.GetOutEdge(ref.gid, false);
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);

//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, FindInEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   Edge *edge = new Edge(ref.gid, type, 123, 456);
//   vertex.AddInEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.FindInEdge(edge);
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);

//   delete edge;
//   delete other_vertex;
//   delete delta;
// }

// TEST(VertexTest, FindOutEdge) {
//   Delta *delta = new Delta(Delta::Action::DELETE_OBJECT);
//   Vertex vertex(123, delta);
//   Vertex *other_vertex = new Vertex(456, nullptr);
//   EdgeTypeId type = 1;
//   EdgeRef ref(789);
//   Edge *edge = new Edge(ref.gid, type, 123, 456);
//   vertex.AddOutEdge(type, other_vertex, ref);

//   auto edge_tuple = vertex.FindOutEdge(edge);
//   EXPECT_TRUE(edge_tuple.has_value());
//   EXPECT_EQ(std::get<0>(*edge_tuple), type);
//   EXPECT_EQ(std::get<1>(*edge_tuple), other_vertex);
//   EXPECT_EQ(std::get<2>(*edge_tuple), ref);

//   delete edge;
//   delete other_vertex;
//   delete delta;
// }
