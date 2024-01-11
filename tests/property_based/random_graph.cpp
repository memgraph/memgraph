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

#include <memory>
#include <unordered_map>
#include <vector>

/**
 * gtest/gtest.h must be included before rapidcheck/gtest.h!
 */
#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
using memgraph::replication::ReplicationRole;
/**
 * It is possible to run test with custom seed with:
 * RC_PARAMS="seed=1" ./random_graph
 */
RC_GTEST_PROP(RandomGraph, RandomGraph, (std::vector<std::string> vertex_labels, std::vector<std::string> edge_types)) {
  RC_PRE(!vertex_labels.empty());
  RC_PRE(!edge_types.empty());

  int vertices_num = vertex_labels.size();
  int edges_num = edge_types.size();

  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage()};
  std::vector<memgraph::storage::VertexAccessor> vertices;
  std::unordered_map<memgraph::storage::VertexAccessor, std::string> vertex_label_map;
  std::unordered_map<memgraph::storage::EdgeAccessor, std::string> edge_type_map;

  auto dba = db->Access(ReplicationRole::MAIN);

  for (auto label : vertex_labels) {
    auto vertex_accessor = dba->CreateVertex();
    RC_ASSERT(vertex_accessor.AddLabel(dba->NameToLabel(label)).HasValue());
    vertex_label_map.emplace(vertex_accessor, label);
    vertices.push_back(vertex_accessor);
  }

  for (auto type : edge_types) {
    auto &from = vertices[*rc::gen::inRange(0, vertices_num)];
    auto &to = vertices[*rc::gen::inRange(0, vertices_num)];
    auto maybe_edge_accessor = dba->CreateEdge(&from, &to, dba->NameToEdgeType(type));
    RC_ASSERT(maybe_edge_accessor.HasValue());
    edge_type_map.insert({*maybe_edge_accessor, type});
  }

  dba->AdvanceCommand();

  int edges_num_check = 0;
  int vertices_num_check = 0;
  for (auto vertex : dba->Vertices(memgraph::storage::View::OLD)) {
    auto label = vertex_label_map.at(vertex);
    auto maybe_labels = vertex.Labels(memgraph::storage::View::OLD);
    RC_ASSERT(maybe_labels.HasValue());
    const auto &labels = *maybe_labels;
    RC_ASSERT(labels.size() == 1);
    RC_ASSERT(dba->LabelToName(labels[0]) == label);
    vertices_num_check++;
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::OLD);
    RC_ASSERT(maybe_edges.HasValue());
    for (auto &edge : maybe_edges->edges) {
      const auto &type = edge_type_map.at(edge);
      RC_ASSERT(dba->EdgeTypeToName(edge.EdgeType()) == type);
      edges_num_check++;
    }
  }
  RC_ASSERT(vertices_num_check == vertices_num);
  RC_ASSERT(edges_num_check == edges_num);
}
