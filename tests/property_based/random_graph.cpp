#include <unordered_map>
#include <vector>

/**
 * gtest/gtest.h must be included before rapidcheck/gtest.h!
 */
#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "storage/vertex_accessor.hpp"

/**
 * It is possible to run test with custom seed with:
 * RC_PARAMS="seed=1" ./random_graph
 */
RC_GTEST_PROP(RandomGraph, RandomGraph, (std::vector<std::string> vertex_labels,
                                         std::vector<std::string> edge_types)) {
  RC_PRE(!vertex_labels.empty());
  RC_PRE(!edge_types.empty());

  int vertices_num = vertex_labels.size();
  int edges_num = edge_types.size();

  database::GraphDb db;
  std::vector<VertexAccessor> vertices;
  std::unordered_map<VertexAccessor, std::string> vertex_label_map;
  std::unordered_map<EdgeAccessor, std::string> edge_type_map;

  auto dba = db.Access();

  for (auto label : vertex_labels) {
    auto vertex_accessor = dba.InsertVertex();
    vertex_accessor.add_label(dba.Label(label));
    vertex_label_map.insert({vertex_accessor, label});
    vertices.push_back(vertex_accessor);
  }

  for (auto type : edge_types) {
    auto from = vertices[*rc::gen::inRange(0, vertices_num)];
    auto to = vertices[*rc::gen::inRange(0, vertices_num)];
    auto edge_accessor = dba.InsertEdge(from, to, dba.EdgeType(type));
    edge_type_map.insert({edge_accessor, type});
  }

  dba.AdvanceCommand();

  int edges_num_check = 0;
  int vertices_num_check = 0;
  for (const auto &vertex : dba.Vertices(false)) {
    auto label = vertex_label_map.at(vertex);
    RC_ASSERT(vertex.labels().size() == 1);
    RC_ASSERT(dba.LabelName(vertex.labels()[0]) == label);
    vertices_num_check++;
  }
  for (const auto &edge : dba.Edges(false)) {
    auto type = edge_type_map.at(edge);
    RC_ASSERT(dba.EdgeTypeName(edge.EdgeType()) == type);
    edges_num_check++;
  }
  RC_ASSERT(vertices_num_check == vertices_num);
  RC_ASSERT(edges_num_check == edges_num);
}
