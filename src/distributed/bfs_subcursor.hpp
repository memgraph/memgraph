/// @file
#pragma once

#include <map>
#include <memory>
#include <unordered_map>

#include "glog/logging.h"

#include "database/distributed/graph_db_accessor.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/eval.hpp"
#include "query/plan/operator.hpp"

namespace database {
class GraphDb;
}

namespace distributed {

class BfsRpcClients;

/// Path from BFS source to a vertex might span multiple workers. This struct
/// stores information describing segment of a path stored on a worker and
/// information necessary to continue path reconstruction on another worker.
struct PathSegment {
  std::vector<EdgeAccessor> edges;
  std::experimental::optional<storage::VertexAddress> next_vertex;
  std::experimental::optional<storage::EdgeAddress> next_edge;
};

/// Class storing the worker-local state of distributed BFS traversal. For each
/// traversal (uniquely identified by cursor id), there is one instance of this
/// class per worker, and those instances communicate via RPC calls.
class ExpandBfsSubcursor {
 public:
  ExpandBfsSubcursor(database::GraphDbAccessor *dba,
                     query::EdgeAtom::Direction direction,
                     std::vector<storage::EdgeType> edge_types,
                     query::SymbolTable symbol_table,
                     std::unique_ptr<query::AstStorage> ast_storage,
                     query::plan::ExpansionLambda filter_lambda,
                     query::EvaluationContext evaluation_context,
                     BfsRpcClients *bfs_subcursor_clients);

  // Stores subcursor ids of other workers.
  void RegisterSubcursors(std::unordered_map<int16_t, int64_t> subcursor_ids) {
    subcursor_ids_ = std::move(subcursor_ids);
  }

  /// Sets the source to be used for new expansion.
  void SetSource(storage::VertexAddress source_address);

  /// Notifies the subcursor that a new expansion should take place.
  /// `to_visit_next_` must be moved to `to_visit_current_` synchronously for
  /// all subcursors participating in expansion to avoid race condition with
  /// `ExpandToRemoteVertex` RPC requests. Also used before setting new source
  /// with `clear` set to true, to avoid a race condition similar to one
  /// described above.
  ///
  /// @param clear   if set to true, `Reset` will be called instead of moving
  ///                `to_visit_next_`
  //  @param frame   frame for evaluation of filter lambda expression
  void PrepareForExpand(bool clear, std::vector<query::TypedValue> frame);

  /// Expands the BFS frontier once. Returns true if there was a successful
  /// expansion.
  bool ExpandLevel();

  /// Pulls the next vertex in the current BFS frontier, if there is one.
  std::experimental::optional<VertexAccessor> Pull();

  /// Expands to a local vertex, if it wasn't already visited. Returns true if
  /// expansion was successful.
  bool ExpandToLocalVertex(storage::EdgeAddress edge, VertexAccessor vertex);
  bool ExpandToLocalVertex(storage::EdgeAddress edge,
                           storage::VertexAddress vertex);

  /// Reconstruct the part of path ending with given edge, stored on this
  /// worker.
  PathSegment ReconstructPath(storage::EdgeAddress edge_address);

  /// Reconstruct the part of path to given vertex stored on this worker.
  PathSegment ReconstructPath(storage::VertexAddress vertex_addr);

  database::GraphDbAccessor *db_accessor() { return dba_; }

  tx::TransactionId tx_id() { return tx_id_; }

  /// Used to reset subcursor state before starting expansion from new source.
  void Reset();

 private:
  /// Expands to a local or remote vertex, returns true if expansion was
  /// successful.
  bool ExpandToVertex(EdgeAccessor edge, VertexAccessor vertex);

  /// Tries to expand to all vertices connected to given one and returns true if
  /// any of them was successful.
  bool ExpandFromVertex(VertexAccessor vertex);

  /// Helper for path reconstruction doing the actual work.
  void ReconstructPathHelper(VertexAccessor vertex, PathSegment *result);

  BfsRpcClients *bfs_subcursor_clients_{nullptr};

  database::GraphDbAccessor *dba_;

  /// IDs of subcursors on other workers, used when sending RPCs.
  std::unordered_map<int16_t, int64_t> subcursor_ids_;

  query::EdgeAtom::Direction direction_;
  std::vector<storage::EdgeType> edge_types_;

  /// Symbol table and AstStorage for filter lambda evaluation. If subcursor
  /// doesn't own the filter lambda expression, `ast_storage_` is set to
  /// nullptr.
  query::SymbolTable symbol_table_;
  std::unique_ptr<query::AstStorage> ast_storage_;
  query::plan::ExpansionLambda filter_lambda_;

  /// Evaluation context, frame and expression evaluator for evaluation of
  /// filter lambda.
  query::EvaluationContext evaluation_context_;
  query::Frame frame_;
  query::ExpressionEvaluator expression_evaluator_;

  /// Mutex protecting `to_visit_next_` and `processed_`, because there is a
  /// race between expansions done locally using `ExpandToLocalVertex` and
  /// incoming `ExpandToRemoteVertex` RPCs.
  std::mutex mutex_;

  /// List of visited vertices and their incoming edges. Local address is stored
  /// for local edges, global address for remote edges.
  std::unordered_map<VertexAccessor,
                     std::experimental::optional<storage::EdgeAddress>>
      processed_;

  /// List of vertices at the current expansion level.
  std::vector<std::pair<storage::EdgeAddress, VertexAccessor>>
      to_visit_current_;

  /// List of unvisited vertices reachable from current expansion level.
  std::vector<std::pair<storage::EdgeAddress, VertexAccessor>> to_visit_next_;

  /// Index of the vertex from `to_visit_next_` to return on next pull.
  size_t pull_index_;

  // Transaction ID used for transactional cache clean-up mechanism.
  tx::TransactionId tx_id_;
};

/// Thread-safe storage for BFS subcursors.
class BfsSubcursorStorage {
 public:
  explicit BfsSubcursorStorage(BfsRpcClients *bfs_subcursor_clients);

  int64_t Create(database::GraphDbAccessor *dba,
                 query::EdgeAtom::Direction direction,
                 std::vector<storage::EdgeType> edge_types,
                 query::SymbolTable symbol_table,
                 std::unique_ptr<query::AstStorage> ast_storage,
                 query::plan::ExpansionLambda filter_lambda,
                 query::EvaluationContext evaluation_context);
  ExpandBfsSubcursor *Get(int64_t subcursor_id);
  void ClearTransactionalCache(tx::TransactionId oldest_active);

 private:
  BfsRpcClients *bfs_subcursor_clients_{nullptr};

  std::mutex mutex_;
  std::map<int64_t, std::unique_ptr<ExpandBfsSubcursor>> storage_;
  int64_t next_subcursor_id_{0};
};

}  // namespace distributed
