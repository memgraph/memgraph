
#include <storage/edge.hpp>
#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "snapshot/snapshoter.hpp"

#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"

GraphDb::GraphDb(bool import_snapshot) : GraphDb("default", import_snapshot) {}

GraphDb::GraphDb(const std::string &name, bool import_snapshot)
    : GraphDb(name.c_str(), import_snapshot) {
}

//GraphDb::GraphDb(const char *name, bool import_snapshot) : name_(name) {
//  if (import_snapshot) snap_engine.import();
//}

VertexAccessor GraphDb::insert_vertex(DbTransaction &db_trans) {
  auto vertex_vlist = new mvcc::VersionList<Vertex>();
  vertex_vlist->insert(db_trans.trans);

  // TODO make this configurable
  for (int i = 0; i < 5; ++i) {
    bool success = vertices_.access().insert(vertex_vlist).second;
    if (success)
      return VertexAccessor(vertex_vlist, db_trans);
    // TODO sleep for some amount of time
  }

  throw CreationException("Unable to create a Vertex after 5 attempts");
}

EdgeAccessor GraphDb::insert_edge(
    DbTransaction& db_trans, VertexAccessor& from,
    VertexAccessor& to, EdgeType type) {

  auto edge_vlist = new mvcc::VersionList<Edge>();
  Edge* edge = edge_vlist->insert(db_trans.trans);

  // set the given values of the new edge
  edge->edge_type_ = type;
  // connect the edge to vertices
  edge->from_ = from.vlist(pass_key);
  edge->to_ = to.vlist(pass_key);
  // connect the vertices to edge
  from.vlist(pass_key).out_.emplace(edge_vlist);
  to.vlist(pass_key).in_.emplace(edge_vlist);

  // TODO make this configurable
  for (int i = 0; i < 5; ++i) {
    bool success = edges_.access().insert(edge_vlist).second;
    if (success)
      return EdgeAccessor(edge_vlist, db_trans);
    // TODO sleep for some amount of time
  }

  throw CreationException("Unable to create an Edge after 5 attempts");

  EdgeRecord edge_record(next, from, to);
  auto edge = edge_record.insert(t.trans);

  // insert the new vertex record into the vertex store
  auto edges_accessor = edges.access();
  auto result = edges_accessor.insert(next, std::move(edge_record));

  // create new vertex
  auto inserted_edge_record = result.first;

  t.to_update_index<TypeGroupEdge>(&inserted_edge_record->second, edge);

  return EdgeAccessor(edge, &inserted_edge_record->second, t);
}
`
