#include "distributed/dgp/vertex_migrator.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

namespace distributed::dgp {

VertexMigrator::VertexMigrator(database::GraphDbAccessor *dba) : dba_(dba) {}

void VertexMigrator::MigrateVertex(VertexAccessor &vertex, int destination) {
  auto get_props = [](auto &record) {
    std::unordered_map<storage::Property, PropertyValue> properties;
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

  auto relocated_vertex = database::InsertVertexIntoRemote(
      dba_, destination, vertex.labels(), get_props(vertex), vertex.CypherId());

  vertex_migrated_to_[vertex.gid()] = relocated_vertex.address();

  for (auto out_edge : vertex.out()) {
    auto to = out_edge.to();
    update_if_moved(to);
    // Here cypher_id has to be passed to the other machine because this
    // machine owns the edge.
    auto new_out_edge =
        dba_->InsertEdge(relocated_vertex, to, out_edge.EdgeType(),
                         std::experimental::nullopt, out_edge.CypherId());
    for (auto prop : get_props(out_edge)) {
      new_out_edge.PropsSet(prop.first, prop.second);
    }
  }

  for (auto in_edge : vertex.in()) {
    auto from = in_edge.from();
    // Continue on self-loops since those edges have already been added
    // while iterating over out edges.
    if (from == vertex) continue;
    update_if_moved(from);
    // Both gid and cypher_id should be without value because this machine
    // doesn't own the edge.
    auto new_in_edge =
        dba_->InsertEdge(from, relocated_vertex, in_edge.EdgeType(),
                         std::experimental::nullopt, in_edge.CypherId());
    for (auto prop : get_props(in_edge)) {
      new_in_edge.PropsSet(prop.first, prop.second);
    }
  }

  dba_->DetachRemoveVertex(vertex);
}
}  // namespace distributed::dgp
