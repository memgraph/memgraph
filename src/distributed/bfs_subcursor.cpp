#include "bfs_subcursor.hpp"

#include <unordered_map>

#include "database/distributed_graph_db.hpp"
#include "distributed/bfs_rpc_clients.hpp"
#include "query/plan/operator.hpp"
#include "storage/address_types.hpp"
#include "storage/vertex_accessor.hpp"

namespace distributed {

using query::TypedValue;

ExpandBfsSubcursor::ExpandBfsSubcursor(
    database::GraphDb *db, tx::TransactionId tx_id,
    query::EdgeAtom::Direction direction,
    std::vector<storage::EdgeType> edge_types, query::GraphView graph_view,
    BfsRpcClients *bfs_subcursor_clients)
    : bfs_subcursor_clients_(bfs_subcursor_clients),
      dba_(*db, tx_id),
      direction_(direction),
      edge_types_(std::move(edge_types)),
      graph_view_(graph_view) {
  Reset();
}

void ExpandBfsSubcursor::Reset() {
  pull_index_ = 0;
  processed_.clear();
  to_visit_current_.clear();
  to_visit_next_.clear();
}

void ExpandBfsSubcursor::SetSource(storage::VertexAddress source_address) {
  Reset();
  auto source = VertexAccessor(source_address, dba_);
  SwitchAccessor(source, graph_view_);
  processed_.emplace(source, std::experimental::nullopt);
  ExpandFromVertex(source);
}

void ExpandBfsSubcursor::PrepareForExpand(bool clear) {
  if (clear) {
    Reset();
  } else {
    std::swap(to_visit_current_, to_visit_next_);
    to_visit_next_.clear();
  }
}

bool ExpandBfsSubcursor::ExpandLevel() {
  bool expanded = false;
  for (const auto &expansion : to_visit_current_) {
    expanded |= ExpandFromVertex(expansion.second);
  }
  pull_index_ = 0;
  return expanded;
}

std::experimental::optional<VertexAccessor> ExpandBfsSubcursor::Pull() {
  return pull_index_ < to_visit_next_.size()
             ? std::experimental::make_optional(
                   to_visit_next_[pull_index_++].second)
             : std::experimental::nullopt;
}

bool ExpandBfsSubcursor::ExpandToLocalVertex(storage::EdgeAddress edge,
                                             VertexAccessor vertex) {
  CHECK(vertex.address().is_local())
      << "ExpandToLocalVertex called with remote vertex";

  edge = dba_.db().storage().LocalizedAddressIfPossible(edge);
  SwitchAccessor(vertex, graph_view_);

  std::lock_guard<std::mutex> lock(mutex_);
  auto got = processed_.emplace(vertex, edge);
  if (got.second) {
    to_visit_next_.emplace_back(edge, vertex);
  }
  return got.second;
}

bool ExpandBfsSubcursor::ExpandToLocalVertex(storage::EdgeAddress edge,
                                             storage::VertexAddress vertex) {
  auto vertex_accessor = VertexAccessor(vertex, dba_);
  return ExpandToLocalVertex(edge, VertexAccessor(vertex, dba_));
}

PathSegment ExpandBfsSubcursor::ReconstructPath(
    storage::EdgeAddress edge_address) {
  EdgeAccessor edge(edge_address, dba_);
  CHECK(edge.address().is_local()) << "ReconstructPath called with remote edge";
  DCHECK(edge.from_addr().is_local()) << "`from` vertex should always be local";
  DCHECK(!edge.to_addr().is_local()) << "`to` vertex should be remote when "
                                        "calling ReconstructPath with edge";

  PathSegment result;
  result.edges.emplace_back(edge);
  ReconstructPathHelper(edge.from(), &result);
  return result;
}

PathSegment ExpandBfsSubcursor::ReconstructPath(
    storage::VertexAddress vertex_addr) {
  VertexAccessor vertex(vertex_addr, dba_);
  CHECK(vertex.address().is_local())
      << "ReconstructPath called with remote vertex";
  PathSegment result;
  ReconstructPathHelper(vertex, &result);
  return result;
}

void ExpandBfsSubcursor::ReconstructPathHelper(VertexAccessor vertex,
                                               PathSegment *result) {
  auto it = processed_.find(vertex);
  CHECK(it != processed_.end())
      << "ReconstructPath called with unvisited vertex";

  auto in_edge_address = it->second;
  while (in_edge_address) {
    // In-edge is stored on another worker. It should be returned to master from
    // that worker, and path reconstruction should be continued there.
    if (in_edge_address->is_remote()) {
      result->next_edge = in_edge_address;
      break;
    }

    result->edges.emplace_back(*in_edge_address, dba_);

    auto &in_edge = result->edges.back();
    auto next_vertex_address =
        in_edge.from_is(vertex) ? in_edge.to_addr() : in_edge.from_addr();

    // We own the in-edge, but the next vertex on the path is stored on another
    // worker.
    if (next_vertex_address.is_remote()) {
      result->next_vertex = next_vertex_address;
      break;
    }

    vertex = VertexAccessor(next_vertex_address, dba_);
    in_edge_address = processed_[vertex];
  }
}

bool ExpandBfsSubcursor::ExpandToVertex(EdgeAccessor edge,
                                        VertexAccessor vertex) {
  // TODO(mtomic): lambda filtering in distributed
  return vertex.is_local() ? ExpandToLocalVertex(edge.address(), vertex)
                           : bfs_subcursor_clients_->ExpandToRemoteVertex(
                                 subcursor_ids_, edge, vertex);
}

bool ExpandBfsSubcursor::ExpandFromVertex(VertexAccessor vertex) {
  bool expanded = false;
  if (direction_ != query::EdgeAtom::Direction::IN) {
    for (const EdgeAccessor &edge : vertex.out(&edge_types_))
      expanded |= ExpandToVertex(edge, edge.to());
  }
  if (direction_ != query::EdgeAtom::Direction::OUT) {
    for (const EdgeAccessor &edge : vertex.in(&edge_types_))
      expanded |= ExpandToVertex(edge, edge.from());
  }
  return expanded;
}

BfsSubcursorStorage::BfsSubcursorStorage(database::GraphDb *db,
                                         BfsRpcClients *bfs_subcursor_clients)
    : db_(db), bfs_subcursor_clients_(bfs_subcursor_clients) {}

int64_t BfsSubcursorStorage::Create(tx::TransactionId tx_id,
                                    query::EdgeAtom::Direction direction,
                                    std::vector<storage::EdgeType> edge_types,
                                    query::GraphView graph_view) {
  std::lock_guard<std::mutex> lock(mutex_);
  int64_t id = next_subcursor_id_++;
  auto got =
      storage_.emplace(id, std::make_unique<ExpandBfsSubcursor>(
                               db_, tx_id, direction, std::move(edge_types),
                               graph_view, bfs_subcursor_clients_));
  CHECK(got.second) << "Subcursor with ID " << id << " already exists";
  return id;
}

void BfsSubcursorStorage::Erase(int64_t subcursor_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto removed = storage_.erase(subcursor_id);
  CHECK(removed == 1) << "Subcursor with ID " << subcursor_id << " not found";
}

ExpandBfsSubcursor *BfsSubcursorStorage::Get(int64_t subcursor_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = storage_.find(subcursor_id);
  CHECK(it != storage_.end())
      << "Subcursor with ID " << subcursor_id << " not found";
  return it->second.get();
}

}  // namespace distributed
