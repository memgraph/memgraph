#pragma once

#include <mutex>
#include <unordered_map>
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
#include "query/typed_value.hpp"
#include "storage/gid.hpp"
#include "storage/record_accessor.hpp"
#include "storage/types.hpp"
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

    /// Creates a new vertex and returns it's gid.
    RemoteCreateResult CreateVertex(
        const std::vector<storage::Label> &labels,
        const std::unordered_map<storage::Property, query::TypedValue>
            &properties) {
      auto result = db_accessor_.InsertVertex();
      for (auto &label : labels) result.add_label(label);
      for (auto &kv : properties) result.PropsSet(kv.first, kv.second);
      std::lock_guard<SpinLock> guard{lock_};
      deltas_.emplace(
          result.gid(),
          std::make_pair(result, std::vector<database::StateDelta>{}));
      return {RemoteUpdateResult::DONE, result.gid()};
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
  RemoteUpdatesRpcServer(database::GraphDb &db, tx::Engine &engine,
                         communication::rpc::Server &server)
      : db_(db), engine_(engine), server_(server) {
    server_.Register<RemoteUpdateRpc>([this](const RemoteUpdateReq &req) {
      using DeltaType = database::StateDelta::Type;
      auto &delta = req.member;
      switch (delta.type) {
        case DeltaType::SET_PROPERTY_VERTEX:
        case DeltaType::ADD_LABEL:
        case DeltaType::REMOVE_LABEL:
          return std::make_unique<RemoteUpdateRes>(
              GetUpdates(vertex_updates_, delta.transaction_id).Emplace(delta));
        case DeltaType::SET_PROPERTY_EDGE:
          return std::make_unique<RemoteUpdateRes>(
              GetUpdates(edge_updates_, delta.transaction_id).Emplace(delta));
        default:
          LOG(FATAL) << "Can't perform a remote update with delta type: "
                     << static_cast<int>(req.member.type);
      }
    });

    server_.Register<RemoteUpdateApplyRpc>(
        [this](const RemoteUpdateApplyReq &req) {
          return std::make_unique<RemoteUpdateApplyRes>(Apply(req.member));
        });

    server_.Register<RemoteCreateVertexRpc>(
        [this](const RemoteCreateVertexReq &req) {
          return std::make_unique<RemoteCreateVertexRes>(
              GetUpdates(vertex_updates_, req.member.tx_id)
                  .CreateVertex(req.member.labels, req.member.properties));
        });

    server_.Register<RemoteCreateEdgeRpc>(
        [this](const RemoteCreateEdgeReq &req) {
          auto data = req.member;
          auto creation_result = CreateEdge(data);

          // If `from` and `to` are both on this worker, we handle it in this
          // RPC call. Do it only if CreateEdge succeeded.
          if (creation_result.result == RemoteUpdateResult::DONE &&
              data.to.worker_id() == db_.WorkerId()) {
            auto to_delta = database::StateDelta::AddInEdge(
                data.tx_id, data.to.gid(), {data.from, db_.WorkerId()},
                {creation_result.gid, db_.WorkerId()}, data.edge_type);
            creation_result.result =
                GetUpdates(vertex_updates_, data.tx_id).Emplace(to_delta);
          }

          return std::make_unique<RemoteCreateEdgeRes>(creation_result);
        });

    server_.Register<RemoteAddInEdgeRpc>([this](const RemoteAddInEdgeReq &req) {
      auto to_delta = database::StateDelta::AddInEdge(
          req.member.tx_id, req.member.to, req.member.from,
          req.member.edge_address, req.member.edge_type);
      auto result =
          GetUpdates(vertex_updates_, req.member.tx_id).Emplace(to_delta);
      return std::make_unique<RemoteAddInEdgeRes>(result);
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
  communication::rpc::Server &server_;
  tx::TxEndListener tx_end_listener_{engine_,
                                     [this](tx::transaction_id_t tx_id) {
                                       vertex_updates_.access().remove(tx_id);
                                       edge_updates_.access().remove(tx_id);
                                     }};
  template <typename TAccessor>
  using MapT =
      ConcurrentMap<tx::transaction_id_t, TransactionUpdates<TAccessor>>;
  MapT<VertexAccessor> vertex_updates_;
  MapT<EdgeAccessor> edge_updates_;

  // Gets/creates the TransactionUpdates for the given transaction.
  template <typename TAccessor>
  TransactionUpdates<TAccessor> &GetUpdates(MapT<TAccessor> &updates,
                                            tx::transaction_id_t tx_id) {
    return updates.access()
        .emplace(tx_id, std::make_tuple(tx_id),
                 std::make_tuple(std::ref(db_), tx_id))
        .first->second;
  }

  RemoteCreateResult CreateEdge(const RemoteCreateEdgeReqData &req) {
    auto &dba = GetUpdates(edge_updates_, req.tx_id).db_accessor();

    auto edge = dba.InsertOnlyEdge({req.from, db_.WorkerId()},
                                   dba.LocalizedAddress(req.to), req.edge_type);

    auto from_delta = database::StateDelta::AddOutEdge(
        req.tx_id, req.from, req.to, dba.GlobalizedAddress(edge.address()),
        req.edge_type);

    auto result = GetUpdates(vertex_updates_, req.tx_id).Emplace(from_delta);
    return {result, edge.gid()};
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
