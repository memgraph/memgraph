#pragma once

#include <unordered_map>
#include <vector>

#include "glog/logging.h"

#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "distributed/updates_rpc_messages.hpp"
#include "query/typed_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/gid.hpp"
#include "storage/types.hpp"
#include "storage/vertex_accessor.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// An RPC server that accepts and holds deferred updates (deltas) until it's
/// told to apply or discard them. The updates are organized and applied per
/// transaction in this single updates server.
///
/// Attempts to get serialization and update-after-delete errors to happen as
/// soon as possible during query execution (fail fast).
class UpdatesRpcServer {
  // Remote updates for one transaction.
  template <typename TRecordAccessor>
  class TransactionUpdates {
   public:
    TransactionUpdates(database::GraphDb &db, tx::transaction_id_t tx_id)
        : db_accessor_(db, tx_id) {}

    /// Adds a delta and returns the result. Does not modify the state (data) of
    /// the graph element the update is for, but calls the `update` method to
    /// fail-fast on serialization and update-after-delete errors.
    UpdateResult Emplace(const database::StateDelta &delta);

    /// Creates a new vertex and returns it's gid.
    gid::Gid CreateVertex(
        const std::vector<storage::Label> &labels,
        const std::unordered_map<storage::Property, query::TypedValue>
            &properties);

    /// Creates a new edge and returns it's gid. Does not update vertices at the
    /// end of the edge.
    gid::Gid CreateEdge(gid::Gid from, storage::VertexAddress to,
                        storage::EdgeType edge_type);

    /// Applies all the deltas on the record.
    UpdateResult Apply();

    auto &db_accessor() { return db_accessor_; }

   private:
    database::GraphDbAccessor db_accessor_;
    std::unordered_map<
        gid::Gid, std::pair<TRecordAccessor, std::vector<database::StateDelta>>>
        deltas_;
    // Multiple workers might be sending remote updates concurrently.
    SpinLock lock_;

    // Helper method specialized for [Vertex|Edge]Accessor.
    TRecordAccessor FindAccessor(gid::Gid gid);
  };

 public:
  UpdatesRpcServer(database::GraphDb &db, communication::rpc::Server &server);

  /// Applies all existsing updates for the given transaction ID. If there are
  /// no updates for that transaction, nothing happens. Clears the updates cache
  /// after applying them, regardless of the result.
  UpdateResult Apply(tx::transaction_id_t tx_id);

  /// Clears the cache of local transactions that are completed. The signature
  /// of this method is dictated by `distributed::TransactionalCacheCleaner`.
  void ClearTransactionalCache(tx::transaction_id_t oldest_active);

 private:
  database::GraphDb &db_;

  template <typename TAccessor>
  using MapT =
      ConcurrentMap<tx::transaction_id_t, TransactionUpdates<TAccessor>>;
  MapT<VertexAccessor> vertex_updates_;
  MapT<EdgeAccessor> edge_updates_;

  // Gets/creates the TransactionUpdates for the given transaction.
  template <typename TAccessor>
  TransactionUpdates<TAccessor> &GetUpdates(MapT<TAccessor> &updates,
                                            tx::transaction_id_t tx_id);

  // Performs edge creation for the given request.
  CreateResult CreateEdge(const CreateEdgeReqData &req);

  // Performs edge removal for the given request.
  UpdateResult RemoveEdge(const RemoveEdgeData &data);
};

}  // namespace distributed
