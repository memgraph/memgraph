#include "database/graph_db_accessor.hpp"
#include "database/creation_exception.hpp"

#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"

GraphDbAccessor::GraphDbAccessor(GraphDb& db)
    : db_(db), transaction_(std::move(db.tx_engine.begin())) {}

const std::string& GraphDbAccessor::name() const { return db_.name_; }

VertexAccessor GraphDbAccessor::insert_vertex() {
  // create a vertex
  auto vertex_vlist = new mvcc::VersionList<Vertex>();
  Vertex* vertex = vertex_vlist->insert(transaction_);

  // insert the newly created record into the main storage
  // TODO make the number of tries configurable
  for (int i = 0; i < 5; ++i) {
    bool success = db_.vertices_.access().insert(vertex_vlist).second;
    if (success) return VertexAccessor(*vertex_vlist, *vertex, *this);
    // TODO sleep for some configurable amount of time
  }

  throw CreationException("Unable to create a Vertex after 5 attempts");
}

bool GraphDbAccessor::remove_vertex(VertexAccessor& vertex_accessor) {
  // TODO consider if this works well with MVCC
  if (vertex_accessor.out_degree() > 0 || vertex_accessor.in_degree() > 0)
    return false;

  vertex_accessor.vlist_.remove(&vertex_accessor.update(), transaction_);
  return true;
}

void GraphDbAccessor::detach_remove_vertex(VertexAccessor& vertex_accessor) {
  // removing edges via accessors is both safe
  // and it should remove all the pointers in the relevant
  // vertices (including this one)
  for (auto edge_accessor : vertex_accessor.in()) remove_edge(edge_accessor);

  for (auto edge_accessor : vertex_accessor.out()) remove_edge(edge_accessor);

  // mvcc removal of the vertex
  vertex_accessor.vlist_.remove(&vertex_accessor.update(), transaction_);
}

std::vector<VertexAccessor> GraphDbAccessor::vertices() {
  auto sl_accessor = db_.vertices_.access();

  std::vector<VertexAccessor> accessors;
  accessors.reserve(sl_accessor.size());

  for (auto vlist : sl_accessor) {
    auto record = vlist->find(transaction_);
    if (record == nullptr) continue;
    accessors.emplace_back(*vlist, *record, *this);
  }

  return accessors;
}

EdgeAccessor GraphDbAccessor::insert_edge(VertexAccessor& from,
                                          VertexAccessor& to,
                                          GraphDb::EdgeType edge_type) {
  // create an edge
  auto edge_vlist = new mvcc::VersionList<Edge>();
  Edge* edge =
      edge_vlist->insert(transaction_, from.vlist_, to.vlist_, edge_type);

  // set the vertex connections to this edge
  from.update().out_.emplace_back(edge_vlist);
  to.update().in_.emplace_back(edge_vlist);

  // insert the newly created record into the main storage
  // TODO make the number of tries configurable
  for (int i = 0; i < 5; ++i) {
    bool success = db_.edges_.access().insert(edge_vlist).second;
    if (success) return EdgeAccessor(*edge_vlist, *edge, *this);
    // TODO sleep for some amount of time
  }

  throw CreationException("Unable to create an Edge after 5 attempts");
}

/**
 * Removes the given edge pointer from a vector of pointers.
 * Does NOT maintain edge pointer ordering (for efficiency).
 */
void swap_out_edge(std::vector<mvcc::VersionList<Edge>*>& edges,
                   mvcc::VersionList<Edge>* edge) {
  auto found = std::find(edges.begin(), edges.end(), edge);
  assert(found != edges.end());
  std::swap(*found, edges.back());
  edges.pop_back();
}

void GraphDbAccessor::remove_edge(EdgeAccessor& edge_accessor) {
  swap_out_edge(edge_accessor.from().update().out_, &edge_accessor.vlist_);
  swap_out_edge(edge_accessor.to().update().in_, &edge_accessor.vlist_);
  edge_accessor.vlist_.remove(&edge_accessor.update(), transaction_);
}

std::vector<EdgeAccessor> GraphDbAccessor::edges() {
  auto sl_accessor = db_.edges_.access();

  std::vector<EdgeAccessor> accessors;
  accessors.reserve(sl_accessor.size());

  for (auto vlist : sl_accessor) {
    auto record = vlist->find(transaction_);
    if (record == nullptr) continue;
    accessors.emplace_back(*vlist, *record, *this);
  }

  return accessors;
}

GraphDb::Label GraphDbAccessor::label(const std::string& label_name) {
  return &(*db_.labels_.access().insert(label_name).first);
}

std::string& GraphDbAccessor::label_name(const GraphDb::Label label) const {
  return *label;
}

GraphDb::EdgeType GraphDbAccessor::edge_type(
    const std::string& edge_type_name) {
  return &(*db_.edge_types_.access().insert(edge_type_name).first);
}

std::string& GraphDbAccessor::edge_type_name(
    const GraphDb::EdgeType edge_type) const {
  return *edge_type;
}

GraphDb::Property GraphDbAccessor::property(const std::string& property_name) {
  return &(*db_.properties_.access().insert(property_name).first);
}

std::string& GraphDbAccessor::property_name(
    const GraphDb::Property property) const {
  return *property;
}
