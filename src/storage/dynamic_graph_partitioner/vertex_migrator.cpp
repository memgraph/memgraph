#include "storage/dynamic_graph_partitioner/vertex_migrator.hpp"

#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

VertexMigrator::VertexMigrator(database::GraphDbAccessor *dba) : dba_(dba) {}

void VertexMigrator::MigrateVertex(VertexAccessor &vertex, int destination) {
  auto get_props = [](auto &record) {
    std::unordered_map<storage::Property, query::TypedValue> properties;
    for (auto prop : record.Properties()) {
      properties[prop.first] = prop.second;
    }
    return properties;
  };

  auto update_if_moved = [this](auto &vertex) {
    if (vertex_migrated_to_.count(vertex.gid())) {
      vertex = VertexAccessor(vertex_migrated_to_[vertex.gid()], *dba_);
    }
  };

  auto relocated_vertex = dba_->InsertVertexIntoRemote(
      destination, vertex.labels(), get_props(vertex));

  vertex_migrated_to_[vertex.gid()] = relocated_vertex.address();

  for (auto in_edge : vertex.in()) {
    auto from = in_edge.from();
    update_if_moved(from);
    auto new_in_edge =
        dba_->InsertEdge(from, relocated_vertex, in_edge.EdgeType());
    for (auto prop : get_props(in_edge)) {
      new_in_edge.PropsSet(prop.first, prop.second);
    }
  }

  for (auto out_edge : vertex.out()) {
    auto to = out_edge.to();
    // Continue on self-loops since those edges have already been added
    // while iterating over in edges
    if (to == vertex) continue;
    update_if_moved(to);
    auto new_out_edge =
        dba_->InsertEdge(relocated_vertex, to, out_edge.EdgeType());
    for (auto prop : get_props(out_edge)) {
      new_out_edge.PropsSet(prop.first, prop.second);
    }
  }

  dba_->DetachRemoveVertex(vertex);
}
