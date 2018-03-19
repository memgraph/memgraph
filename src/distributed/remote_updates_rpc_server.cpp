#include <utility>

#include "glog/logging.h"

#include "distributed/remote_updates_rpc_server.hpp"
#include "threading/sync/lock_timeout_exception.hpp"

namespace distributed {

template <typename TRecordAccessor>
RemoteUpdateResult
RemoteUpdatesRpcServer::TransactionUpdates<TRecordAccessor>::Emplace(
    const database::StateDelta &delta) {
  auto gid = std::is_same<TRecordAccessor, VertexAccessor>::value
                 ? delta.vertex_id
                 : delta.edge_id;
  std::lock_guard<SpinLock> guard{lock_};
  auto found = deltas_.find(gid);
  if (found == deltas_.end()) {
    found =
        deltas_
            .emplace(gid, std::make_pair(FindAccessor(gid),
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

template <typename TRecordAccessor>
gid::Gid
RemoteUpdatesRpcServer::TransactionUpdates<TRecordAccessor>::CreateVertex(
    const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, query::TypedValue>
        &properties) {
  auto result = db_accessor_.InsertVertex();
  for (auto &label : labels) result.add_label(label);
  for (auto &kv : properties) result.PropsSet(kv.first, kv.second);
  std::lock_guard<SpinLock> guard{lock_};
  deltas_.emplace(result.gid(),
                  std::make_pair(result, std::vector<database::StateDelta>{}));
  return result.gid();
}

template <typename TRecordAccessor>
gid::Gid
RemoteUpdatesRpcServer::TransactionUpdates<TRecordAccessor>::CreateEdge(
    gid::Gid from, storage::VertexAddress to, storage::EdgeType edge_type) {
  auto &db = db_accessor_.db();
  auto edge = db_accessor_.InsertOnlyEdge(
      {from, db.WorkerId()}, db.storage().LocalizedAddressIfPossible(to),
      edge_type);
  std::lock_guard<SpinLock> guard{lock_};
  deltas_.emplace(edge.gid(),
                  std::make_pair(edge, std::vector<database::StateDelta>{}));
  return edge.gid();
}

template <typename TRecordAccessor>
RemoteUpdateResult
RemoteUpdatesRpcServer::TransactionUpdates<TRecordAccessor>::Apply() {
  std::lock_guard<SpinLock> guard{lock_};
  for (auto &kv : deltas_) {
    auto &record_accessor = kv.second.first;
    // We need to reconstruct the record as in the meantime some local
    // update might have updated it.
    record_accessor.Reconstruct();
    for (database::StateDelta &delta : kv.second.second) {
      try {
        auto &dba = db_accessor_;
        switch (delta.type) {
          case database::StateDelta::Type::TRANSACTION_BEGIN:
          case database::StateDelta::Type::TRANSACTION_COMMIT:
          case database::StateDelta::Type::TRANSACTION_ABORT:
          case database::StateDelta::Type::CREATE_VERTEX:
          case database::StateDelta::Type::CREATE_EDGE:
          case database::StateDelta::Type::BUILD_INDEX:
            LOG(FATAL) << "Can only apply record update deltas for remote "
                          "graph element";
          case database::StateDelta::Type::REMOVE_VERTEX:
            if (!db_accessor().RemoveVertex(
                    reinterpret_cast<VertexAccessor &>(record_accessor),
                    delta.check_empty)) {
              return RemoteUpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR;
            }
            break;
          case database::StateDelta::Type::SET_PROPERTY_VERTEX:
          case database::StateDelta::Type::SET_PROPERTY_EDGE:
            record_accessor.PropsSet(delta.property, delta.value);
            break;
          case database::StateDelta::Type::ADD_LABEL:
            reinterpret_cast<VertexAccessor &>(record_accessor)
                .add_label(delta.label);
            break;
          case database::StateDelta::Type::REMOVE_LABEL:
            reinterpret_cast<VertexAccessor &>(record_accessor)
                .remove_label(delta.label);
            break;
          case database::StateDelta::Type::ADD_OUT_EDGE:
            reinterpret_cast<Vertex &>(record_accessor.update())
                .out_.emplace(dba.db().storage().LocalizedAddressIfPossible(
                                  delta.vertex_to_address),
                              dba.db().storage().LocalizedAddressIfPossible(
                                  delta.edge_address),
                              delta.edge_type);
            dba.wal().Emplace(delta);
            break;
          case database::StateDelta::Type::ADD_IN_EDGE:
            reinterpret_cast<Vertex &>(record_accessor.update())
                .in_.emplace(dba.db().storage().LocalizedAddressIfPossible(
                                 delta.vertex_from_address),
                             dba.db().storage().LocalizedAddressIfPossible(
                                 delta.edge_address),
                             delta.edge_type);
            dba.wal().Emplace(delta);
            break;
          case database::StateDelta::Type::REMOVE_EDGE:
            // We only remove the edge as a result of this StateDelta,
            // because the removal of edge from vertex in/out is performed
            // in REMOVE_[IN/OUT]_EDGE deltas.
            db_accessor_.RemoveEdge(
                reinterpret_cast<EdgeAccessor &>(record_accessor), false,
                false);
            break;
          case database::StateDelta::Type::REMOVE_OUT_EDGE:
            reinterpret_cast<VertexAccessor &>(record_accessor)
                .RemoveOutEdge(delta.edge_address);
            break;
          case database::StateDelta::Type::REMOVE_IN_EDGE:
            reinterpret_cast<VertexAccessor &>(record_accessor)
                .RemoveInEdge(delta.edge_address);
            break;
        }
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

RemoteUpdatesRpcServer::RemoteUpdatesRpcServer(
    database::GraphDb &db, communication::rpc::Server &server)
    : db_(db) {
  server.Register<RemoteUpdateRpc>([this](const RemoteUpdateReq &req) {
    using DeltaType = database::StateDelta::Type;
    auto &delta = req.member;
    switch (delta.type) {
      case DeltaType::SET_PROPERTY_VERTEX:
      case DeltaType::ADD_LABEL:
      case DeltaType::REMOVE_LABEL:
      case database::StateDelta::Type::REMOVE_OUT_EDGE:
      case database::StateDelta::Type::REMOVE_IN_EDGE:
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

  server.Register<RemoteUpdateApplyRpc>(
      [this](const RemoteUpdateApplyReq &req) {
        return std::make_unique<RemoteUpdateApplyRes>(Apply(req.member));
      });

  server.Register<RemoteCreateVertexRpc>([this](
      const RemoteCreateVertexReq &req) {
    gid::Gid gid = GetUpdates(vertex_updates_, req.member.tx_id)
                       .CreateVertex(req.member.labels, req.member.properties);
    return std::make_unique<RemoteCreateVertexRes>(
        RemoteCreateResult{RemoteUpdateResult::DONE, gid});
  });

  server.Register<RemoteCreateEdgeRpc>([this](const RemoteCreateEdgeReq &req) {
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

  server.Register<RemoteAddInEdgeRpc>([this](const RemoteAddInEdgeReq &req) {
    auto to_delta = database::StateDelta::AddInEdge(
        req.member.tx_id, req.member.to, req.member.from,
        req.member.edge_address, req.member.edge_type);
    auto result =
        GetUpdates(vertex_updates_, req.member.tx_id).Emplace(to_delta);
    return std::make_unique<RemoteAddInEdgeRes>(result);
  });

  server.Register<RemoteRemoveVertexRpc>(
      [this](const RemoteRemoveVertexReq &req) {
        auto to_delta = database::StateDelta::RemoveVertex(
            req.member.tx_id, req.member.gid, req.member.check_empty);
        auto result =
            GetUpdates(vertex_updates_, req.member.tx_id).Emplace(to_delta);
        return std::make_unique<RemoteRemoveVertexRes>(result);
      });

  server.Register<RemoteRemoveEdgeRpc>([this](const RemoteRemoveEdgeReq &req) {
    return std::make_unique<RemoteRemoveEdgeRes>(RemoveEdge(req.member));
  });

  server.Register<RemoteRemoveInEdgeRpc>(
      [this](const RemoteRemoveInEdgeReq &req) {
        auto data = req.member;
        return std::make_unique<RemoteRemoveInEdgeRes>(
            GetUpdates(vertex_updates_, data.tx_id)
                .Emplace(database::StateDelta::RemoveInEdge(
                    data.tx_id, data.vertex, data.edge_address)));
      });
}

RemoteUpdateResult RemoteUpdatesRpcServer::Apply(tx::transaction_id_t tx_id) {
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

void RemoteUpdatesRpcServer::ClearTransactionalCache(
    tx::transaction_id_t oldest_active) {
  auto vertex_access = vertex_updates_.access();
  for (auto &kv : vertex_access) {
    if (kv.first < oldest_active) {
      vertex_access.remove(kv.first);
    }
  }
  auto edge_access = edge_updates_.access();
  for (auto &kv : edge_access) {
    if (kv.first < oldest_active) {
      edge_access.remove(kv.first);
    }
  }
}

// Gets/creates the TransactionUpdates for the given transaction.
template <typename TAccessor>
RemoteUpdatesRpcServer::TransactionUpdates<TAccessor>
    &RemoteUpdatesRpcServer::GetUpdates(MapT<TAccessor> &updates,
                                        tx::transaction_id_t tx_id) {
  return updates.access()
      .emplace(tx_id, std::make_tuple(tx_id),
               std::make_tuple(std::ref(db_), tx_id))
      .first->second;
}

RemoteCreateResult RemoteUpdatesRpcServer::CreateEdge(
    const RemoteCreateEdgeReqData &req) {
  auto gid = GetUpdates(edge_updates_, req.tx_id)
                 .CreateEdge(req.from, req.to, req.edge_type);

  auto from_delta = database::StateDelta::AddOutEdge(
      req.tx_id, req.from, req.to, {gid, db_.WorkerId()}, req.edge_type);

  auto result = GetUpdates(vertex_updates_, req.tx_id).Emplace(from_delta);
  return {result, gid};
}

RemoteUpdateResult RemoteUpdatesRpcServer::RemoveEdge(
    const RemoteRemoveEdgeData &data) {
  // Edge removal.
  auto deletion_delta =
      database::StateDelta::RemoveEdge(data.tx_id, data.edge_id);
  auto result = GetUpdates(edge_updates_, data.tx_id).Emplace(deletion_delta);

  // Out-edge removal, for sure is local.
  if (result == RemoteUpdateResult::DONE) {
    auto remove_out_delta = database::StateDelta::RemoveOutEdge(
        data.tx_id, data.vertex_from_id, {data.edge_id, db_.WorkerId()});
    result = GetUpdates(vertex_updates_, data.tx_id).Emplace(remove_out_delta);
  }

  // In-edge removal, might not be local.
  if (result == RemoteUpdateResult::DONE &&
      data.vertex_to_address.worker_id() == db_.WorkerId()) {
    auto remove_in_delta = database::StateDelta::RemoveInEdge(
        data.tx_id, data.vertex_to_address.gid(),
        {data.edge_id, db_.WorkerId()});
    result = GetUpdates(vertex_updates_, data.tx_id).Emplace(remove_in_delta);
  }

  return result;
}

template <>
VertexAccessor
RemoteUpdatesRpcServer::TransactionUpdates<VertexAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_.FindVertexChecked(gid, false);
}

template <>
EdgeAccessor
RemoteUpdatesRpcServer::TransactionUpdates<EdgeAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_.FindEdgeChecked(gid, false);
}

}  // namespace distributed
