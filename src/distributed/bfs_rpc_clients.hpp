/// @file
#pragma once

#include "distributed/bfs_subcursor.hpp"
#include "distributed/coordination.hpp"
#include "transactions/transaction.hpp"

namespace database {
class GraphDb;
}

namespace distributed {

class DataManager;

/// Along with `BfsRpcServer`, this class is used to expose `BfsSubcursor`
/// interface over the network so that subcursors can communicate during the
/// traversal. It is just a thin wrapper making RPC calls that also takes
/// care for storing remote data into cache upon receival. Special care is taken
/// to avoid sending local RPCs. Instead, subcursor storage is accessed
/// directly.
class BfsRpcClients {
 public:
  BfsRpcClients(database::GraphDb *db,
                BfsSubcursorStorage *subcursor_storage,
                Coordination *coordination, DataManager *data_manager);

  std::unordered_map<int16_t, int64_t> CreateBfsSubcursors(
      database::GraphDbAccessor *dba, query::EdgeAtom::Direction direction,
      const std::vector<storage::EdgeType> &edge_types,
      const query::plan::ExpansionLambda &filter_lambda,
      const query::SymbolTable &symbol_table,
      const query::EvaluationContext &evaluation_context);


  void RegisterSubcursors(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids);

  void ResetSubcursors(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids);

  std::optional<VertexAccessor> Pull(int16_t worker_id, int64_t subcursor_id,
                                     database::GraphDbAccessor *dba);

  bool ExpandLevel(const std::unordered_map<int16_t, int64_t> &subcursor_ids);

  void SetSource(const std::unordered_map<int16_t, int64_t> &subcursor_ids,
                 storage::VertexAddress source_address);

  bool ExpandToRemoteVertex(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids,
      EdgeAccessor edge, VertexAccessor vertex);

  PathSegment ReconstructPath(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids,
      storage::EdgeAddress edge, database::GraphDbAccessor *dba);

  PathSegment ReconstructPath(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids,
      storage::VertexAddress vertex, database::GraphDbAccessor *dba);

  void PrepareForExpand(
      const std::unordered_map<int16_t, int64_t> &subcursor_ids, bool clear,
      const std::vector<query::TypedValue> &frame);

 private:
  database::GraphDb *db_;
  distributed::BfsSubcursorStorage *subcursor_storage_;
  distributed::Coordination *coordination_;
  distributed::DataManager *data_manager_;
};

}  // namespace distributed
