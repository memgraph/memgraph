#pragma once

#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "glog/logging.h"

#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "distributed/remote_updates_rpc_messages.hpp"
#include "mvcc/version_list.hpp"
#include "storage/gid.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "threading/sync/lock_timeout_exception.hpp"
#include "threading/sync/spinlock.hpp"
#include "transactions/tx_end_listener.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// An RPC server that accepts and holds deferred updates (deltas) until it's
/// told to apply or discard them. The updates are organized and applied per
/// transaction in this single updates server.
///
/// Attempts to get serialization and update-after-delete errors to happen as
/// soon as possible during query execution (fail fast).
class RemoteUpdatesRpcServer {
  // Remote updates for one transaction.
  template <typename TRecordAccessor>
  class TransactionUpdates {
   public:
    TransactionUpdates(database::GraphDb &db, tx::transaction_id_t tx_id)
        : db_accessor_(db, tx_id) {}

    /// Adds a delta and returns the result. Does not modify the state (data) of
    /// the graph element the update is for, but calls the `update` method to
    /// fail-fast on serialization and update-after-delete errors.
    RemoteUpdateResult Emplace(const database::StateDelta &delta) {
      auto gid = std::is_same<TRecordAccessor, VertexAccessor>::value
                     ? delta.vertex_id
                     : delta.edge_id;
      std::lock_guard<SpinLock> guard{lock_};
      auto found = deltas_.find(gid);
      if (found == deltas_.end()) {
        found = deltas_
                    .emplace(gid, std::make_pair(
                                      FindAccessor(gid),
                                      std::vector<database::StateDelta>{}))
                    .first;
      }

      found->second.second.emplace_back(delta);

      // TODO call `RecordAccessor::update` to force serialization errors to
      // fail-fast (as opposed to when all the deltas get applied).
      //
      // This is problematic because `VersionList::update` needs to become
      // thread-safe within the same transaction. Note that the concurrency is
      // possible both between the owner worker interpretation thread and an RPC
      // thread (current thread), as well as multiple RPC threads if this
      // object's lock is released (perhaps desirable).
      //
      // A potential solution *might* be that `LockStore::Lock` returns a `bool`
      // indicating if the caller was the one obtaining the lock (not the same
      // as lock already being held by the same transaction).
      //
      // Another thing that needs to be done (if we do this) is ensuring that
      // `LockStore::Take` is thread-safe when called in parallel in the same
      // transaction. Currently it's thread-safe only when called in parallel
      // from different transactions (only one manages to take the RecordLock).
      //
      // Deferring the implementation of this as it's tricky, and essentially an
      // optimization.
      //
      // try {
      //   found->second.first.update();
      // } catch (const mvcc::SerializationError &) {
      //   return RemoteUpdateResult::SERIALIZATION_ERROR;
      // } catch (const RecordDeletedError &) {
      //   return RemoteUpdateResult::UPDATE_DELETED_ERROR;
      // } catch (const LockTimeoutException &) {
      //   return RemoteUpdateResult::LOCK_TIMEOUT_ERROR;
      // }
      return RemoteUpdateResult::DONE;
    }

    /// Applies all the deltas on the record.
    RemoteUpdateResult Apply() {
      std::lock_guard<SpinLock> guard{lock_};
      for (auto &kv : deltas_) {
        for (database::StateDelta &delta : kv.second.second) {
          try {
            kv.second.first.ProcessDelta(delta);
          } catch (const mvcc::SerializationError &) {
            return RemoteUpdateResult::SERIALIZATION_ERROR;
          } catch (const RecordDeletedError &) {
            return RemoteUpdateResult::UPDATE_DELETED_ERROR;
          } catch (const LockTimeoutException &) {
            return RemoteUpdateResult::LOCK_TIMEOUT_ERROR;
          }
        }
      }
      return RemoteUpdateResult::DONE;
    }

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
  RemoteUpdatesRpcServer(database::GraphDb &db, tx::Engine &engine,
                         communication::rpc::System &system)
      : db_(db), engine_(engine), server_(system, kRemoteUpdatesRpc) {
    server_.Register<RemoteUpdateRpc>([this](const RemoteUpdateReq &req) {
      using DeltaType = database::StateDelta::Type;
      switch (req.member.type) {
        case DeltaType::SET_PROPERTY_VERTEX:
        case DeltaType::ADD_LABEL:
        case DeltaType::REMOVE_LABEL:
          return std::make_unique<RemoteUpdateRes>(
              Process(vertex_updates_, req.member));
        case DeltaType::SET_PROPERTY_EDGE:
          return std::make_unique<RemoteUpdateRes>(
              Process(edge_updates_, req.member));
        default:
          LOG(FATAL) << "Can't perform a remote update with delta type: "
                     << static_cast<int>(req.member.type);
      }
    });

    server_.Register<RemoteUpdateApplyRpc>(
        [this](const RemoteUpdateApplyReq &req) {
          return std::make_unique<RemoteUpdateApplyRes>(Apply(req.member));
        });
  }

  /// Applies all existsing updates for the given transaction ID. If there are
  /// no updates for that transaction, nothing happens. Clears the updates cache
  /// after applying them, regardless of the result.
  RemoteUpdateResult Apply(tx::transaction_id_t tx_id) {
    auto apply = [tx_id](auto &collection) {
      auto access = collection.access();
      auto found = access.find(tx_id);
      if (found == access.end()) {
        return RemoteUpdateResult::DONE;
      }
      auto result = found->second.Apply();
      access.remove(tx_id);
      return result;
    };

    auto vertex_result = apply(vertex_updates_);
    auto edge_result = apply(edge_updates_);
    if (vertex_result != RemoteUpdateResult::DONE) return vertex_result;
    if (edge_result != RemoteUpdateResult::DONE) return edge_result;
    return RemoteUpdateResult::DONE;
  }

 private:
  database::GraphDb &db_;
  tx::Engine &engine_;
  communication::rpc::Server server_;
  ConcurrentMap<tx::transaction_id_t, TransactionUpdates<VertexAccessor>>
      vertex_updates_;
  ConcurrentMap<tx::transaction_id_t, TransactionUpdates<EdgeAccessor>>
      edge_updates_;
  tx::TxEndListener tx_end_listener_{engine_,
                                     [this](tx::transaction_id_t tx_id) {
                                       vertex_updates_.access().remove(tx_id);
                                       edge_updates_.access().remove(tx_id);
                                     }};

  // Processes a single delta recieved in the RPC request.
  template <typename TCollection>
  RemoteUpdateResult Process(TCollection &updates,
                             const database::StateDelta &delta) {
    auto tx_id = delta.transaction_id;
    auto access = updates.access();
    auto &transaction_updates =
        access
            .emplace(tx_id, std::make_tuple(tx_id),
                     std::make_tuple(std::ref(db_), tx_id))
            .first->second;

    return transaction_updates.Emplace(delta);
  }
};

template <>
inline VertexAccessor
RemoteUpdatesRpcServer::TransactionUpdates<VertexAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_.FindVertexChecked(gid, false);
}

template <>
inline EdgeAccessor
RemoteUpdatesRpcServer::TransactionUpdates<EdgeAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_.FindEdgeChecked(gid, false);
}
}  // namespace distributed
