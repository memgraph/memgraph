#include "database/graph_db_accessor.hpp"
#include "database/creation_exception.hpp"

#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/assert.hpp"

GraphDbAccessor::GraphDbAccessor(GraphDb &db)
    : db_(db), transaction_(db.tx_engine.begin()) {}

GraphDbAccessor::~GraphDbAccessor() {
  if (!commited_ && !aborted_) {
    this->abort();
  }
}

const std::string &GraphDbAccessor::name() const { return db_.name_; }

void GraphDbAccessor::advance_command() {
  transaction_->engine.advance(transaction_->id);
}

void GraphDbAccessor::commit() {
  debug_assert(!commited_ && !aborted_,
               "Already aborted or commited transaction.");
  transaction_->commit();
  commited_ = true;
}

void GraphDbAccessor::abort() {
  debug_assert(!commited_ && !aborted_,
               "Already aborted or commited transaction.");
  transaction_->abort();
  aborted_ = true;
}

VertexAccessor GraphDbAccessor::insert_vertex() {
  // create a vertex
  auto vertex_vlist = new mvcc::VersionList<Vertex>(*transaction_);

  bool success = db_.vertices_.access().insert(vertex_vlist).second;
  if (success) return VertexAccessor(*vertex_vlist, *this);
  throw CreationException("Unable to create a Vertex.");
}

void GraphDbAccessor::update_label_indices(
    const GraphDbTypes::Label &label, const VertexAccessor &vertex_accessor,
    const Vertex *const vertex) {
  this->db_.labels_index_.Update(label, vertex_accessor.vlist_, vertex);
  this->db_.label_property_index_.UpdateOnLabel(label, vertex_accessor.vlist_,
                                                vertex);
}

void GraphDbAccessor::update_property_index(
    const GraphDbTypes::Property &property,
    const RecordAccessor<Vertex> &record_accessor, const Vertex *const vertex) {
  this->db_.label_property_index_.UpdateOnProperty(
      property, record_accessor.vlist_, vertex);
}

size_t GraphDbAccessor::vertices_count() const {
  return db_.vertices_.access().size();
}

size_t GraphDbAccessor::vertices_count(const GraphDbTypes::Label &label) const {
  return db_.labels_index_.Count(label);
}

size_t GraphDbAccessor::vertices_count(
    const GraphDbTypes::Label &label,
    const GraphDbTypes::Property &property) const {
  const LabelPropertyIndex::Key key(label, property);
  debug_assert(db_.label_property_index_.IndexExists(key),
               "Index doesn't exist.");
  return db_.label_property_index_.Count(key);
}

bool GraphDbAccessor::remove_vertex(VertexAccessor &vertex_accessor) {
  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (vertex_accessor.current_->is_deleted_by(*transaction_)) return true;
  if (vertex_accessor.out_degree() > 0 || vertex_accessor.in_degree() > 0)
    return false;

  vertex_accessor.vlist_->remove(vertex_accessor.current_, *transaction_);
  return true;
}

void GraphDbAccessor::detach_remove_vertex(VertexAccessor &vertex_accessor) {
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.in()) remove_edge(edge_accessor);
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.out()) remove_edge(edge_accessor);
  if (!remove_vertex(vertex_accessor))
    permanent_fail("Unable to remove vertex after all edges detached");
}

EdgeAccessor GraphDbAccessor::insert_edge(VertexAccessor &from,
                                          VertexAccessor &to,
                                          GraphDbTypes::EdgeType edge_type) {
  // create an edge
  auto edge_vlist = new mvcc::VersionList<Edge>(*transaction_, *from.vlist_,
                                                *to.vlist_, edge_type);

  // ensure that the "from" accessor has the latest version
  from.SwitchNew();
  from.update().out_.emplace_back(edge_vlist);
  // ensure that the "to" accessor has the latest version
  // WARNING: must do that after the above "from.update()" for cases when
  // we are creating a cycle and "from" and "to" are the same vlist
  to.SwitchNew();
  to.update().in_.emplace_back(edge_vlist);

  bool success = db_.edges_.access().insert(edge_vlist).second;
  const auto edge_accessor = EdgeAccessor(*edge_vlist, *this);
  if (success) {
    // This has to be here because there is no additional method for setting
    // edge type.
    update_edge_type_index(edge_type, edge_accessor, &edge_accessor.current());
    return edge_accessor;
  }

  throw CreationException("Unable to create an Edge.");
}

void GraphDbAccessor::update_edge_type_index(
    const GraphDbTypes::EdgeType &edge_type, const EdgeAccessor &edge_accessor,
    const Edge *const edge) {
  this->db_.edge_types_index_.Update(edge_type, edge_accessor.vlist_, edge);
}

size_t GraphDbAccessor::edges_count() const {
  return db_.edges_.access().size();
}

size_t GraphDbAccessor::edges_count(
    const GraphDbTypes::EdgeType &edge_type) const {
  return db_.edge_types_index_.Count(edge_type);
}

/**
 * Removes the given edge pointer from a vector of pointers.
 * Does NOT maintain edge pointer ordering (for efficiency).
 */
void swap_out_edge(std::vector<mvcc::VersionList<Edge> *> &edges,
                   mvcc::VersionList<Edge> *edge) {
  auto found = std::find(edges.begin(), edges.end(), edge);
  debug_assert(found != edges.end(), "Edge doesn't exist.");
  std::swap(*found, edges.back());
  edges.pop_back();
}

void GraphDbAccessor::remove_edge(EdgeAccessor &edge_accessor) {
  // it's possible the edge was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  edge_accessor.SwitchNew();
  if (edge_accessor.current_->is_deleted_by(*transaction_)) return;
  swap_out_edge(edge_accessor.from().update().out_, edge_accessor.vlist_);
  swap_out_edge(edge_accessor.to().update().in_, edge_accessor.vlist_);
  edge_accessor.vlist_->remove(edge_accessor.current_, *transaction_);
}

GraphDbTypes::Label GraphDbAccessor::label(const std::string &label_name) {
  return &(*db_.labels_.access().insert(label_name).first);
}

std::string &GraphDbAccessor::label_name(
    const GraphDbTypes::Label label) const {
  return *label;
}

GraphDbTypes::EdgeType GraphDbAccessor::edge_type(
    const std::string &edge_type_name) {
  return &(*db_.edge_types_.access().insert(edge_type_name).first);
}

std::string &GraphDbAccessor::edge_type_name(
    const GraphDbTypes::EdgeType edge_type) const {
  return *edge_type;
}

GraphDbTypes::Property GraphDbAccessor::property(
    const std::string &property_name) {
  return &(*db_.properties_.access().insert(property_name).first);
}

std::string &GraphDbAccessor::property_name(
    const GraphDbTypes::Property property) const {
  return *property;
}
