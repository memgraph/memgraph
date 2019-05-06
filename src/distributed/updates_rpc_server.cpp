#include "distributed/updates_rpc_server.hpp"

#include <utility>

#include <glog/logging.h>

#include "utils/thread/sync.hpp"

namespace distributed {

template <typename TRecordAccessor>
UpdateResult UpdatesRpcServer::TransactionUpdates<TRecordAccessor>::Emplace(
    const database::StateDelta &delta, int worker_id) {
  auto gid = std::is_same<TRecordAccessor, VertexAccessor>::value
                 ? delta.vertex_id
                 : delta.edge_id;
  std::lock_guard<utils::SpinLock> guard{lock_};
  auto found = deltas_.find(gid);
  if (found == deltas_.end()) {
    found = deltas_
                .emplace(gid, std::make_pair(FindAccessor(gid),
                                             std::vector<DeltaPair>{}))
                .first;
  }

  found->second.second.emplace_back(delta, worker_id);

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
  //   return UpdateResult::SERIALIZATION_ERROR;
  // } catch (const RecordDeletedError &) {
  //   return UpdateResult::UPDATE_DELETED_ERROR;
  // } catch (const utils::LockTimeoutException &) {
  //   return UpdateResult::LOCK_TIMEOUT_ERROR;
  // }
  return UpdateResult::DONE;
}

template <typename TRecordAccessor>
CreatedInfo UpdatesRpcServer::TransactionUpdates<TRecordAccessor>::CreateVertex(
    const std::vector<storage::Label> &labels,
    const std::unordered_map<storage::Property, PropertyValue> &properties,
    std::optional<int64_t> cypher_id) {
  auto result = db_accessor_->InsertVertex(std::nullopt, cypher_id);
  for (auto &label : labels) result.add_label(label);
  for (auto &kv : properties) result.PropsSet(kv.first, kv.second);
  std::lock_guard<utils::SpinLock> guard{lock_};
  deltas_.emplace(result.gid(),
                  std::make_pair(result, std::vector<DeltaPair>{}));
  return CreatedInfo(result.CypherId(), result.gid());
}

template <typename TRecordAccessor>
CreatedInfo UpdatesRpcServer::TransactionUpdates<TRecordAccessor>::CreateEdge(
    gid::Gid from, storage::VertexAddress to, storage::EdgeType edge_type,
    int worker_id, std::optional<int64_t> cypher_id) {
  auto &db = db_accessor_->db();
  auto from_addr = db.storage().LocalizedAddressIfPossible(
      storage::VertexAddress(from, worker_id));
  auto to_addr = db.storage().LocalizedAddressIfPossible(to);
  auto edge = db_accessor_->InsertOnlyEdge(from_addr, to_addr, edge_type,
                                           std::nullopt, cypher_id);
  std::lock_guard<utils::SpinLock> guard{lock_};
  deltas_.emplace(edge.gid(),
                  std::make_pair(edge, std::vector<DeltaPair>{}));
  return CreatedInfo(edge.CypherId(), edge.gid());
}

template <typename TRecordAccessor>
UpdateResult UpdatesRpcServer::TransactionUpdates<TRecordAccessor>::Apply() {
  std::lock_guard<utils::SpinLock> guard{lock_};
  for (auto &kv : deltas_) {
    auto &record_accessor = kv.second.first;
    // We need to reconstruct the record as in the meantime some local
    // update might have updated it.
    record_accessor.Reconstruct();
    for (auto &pair : kv.second.second) {
      auto delta = pair.delta;
      try {
        auto &dba = *db_accessor_;
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
              return UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR;
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
            record_accessor.update();
            reinterpret_cast<Vertex &>(*record_accessor.GetNew())
                .out_.emplace(dba.db().storage().LocalizedAddressIfPossible(
                                  delta.vertex_to_address),
                              dba.db().storage().LocalizedAddressIfPossible(
                                  delta.edge_address),
                              delta.edge_type);
            dba.wal().Emplace(delta);
            break;
          case database::StateDelta::Type::ADD_IN_EDGE:
            record_accessor.update();
            reinterpret_cast<Vertex &>(*record_accessor.GetNew())
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
            db_accessor_->RemoveEdge(
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
        return UpdateResult::SERIALIZATION_ERROR;
      } catch (const RecordDeletedError &) {
        return UpdateResult::UPDATE_DELETED_ERROR;
      } catch (const utils::LockTimeoutException &) {
        return UpdateResult::LOCK_TIMEOUT_ERROR;
      }
    }
  }
  return UpdateResult::DONE;
}

template <typename TRecordAccessor>
void UpdatesRpcServer::TransactionUpdates<TRecordAccessor>::ApplyDeltasToRecord(
    gid::Gid gid, int worker_id, TRecord **old, TRecord **newr) {
  std::lock_guard<utils::SpinLock> guard{lock_};
  auto found = deltas_.find(gid);
  if (found == deltas_.end()) return;

  auto update = [](auto **old, auto **newr) {
    if (!*newr) {
      DCHECK(*old) << "Trying to create new record but pointer to old record "
                      "is nullptr.";

      *newr = (*old)->CloneData();
    }
  };

  for (auto &pair : found->second.second) {
    auto delta = pair.delta;
    if (worker_id != pair.worker_id) continue;

    switch (delta.type) {
      case database::StateDelta::Type::SET_PROPERTY_VERTEX:
      case database::StateDelta::Type::SET_PROPERTY_EDGE:
        update(old, newr);
        (*newr)->properties_.set(delta.property, delta.value);
        break;
      case database::StateDelta::Type::ADD_LABEL: {
        update(old, newr);
        auto &labels = reinterpret_cast<Vertex *>(*newr)->labels_;
        if (!utils::Contains(labels, delta.label)) {
          labels.emplace_back(delta.label);
        }
        break;
      }
      case database::StateDelta::Type::REMOVE_LABEL: {
        update(old, newr);
        auto &labels = reinterpret_cast<Vertex *>(*newr)->labels_;
        auto found = std::find(labels.begin(), labels.end(), delta.label);
        if (found == labels.end()) continue;
        std::swap(*found, labels.back());
        labels.pop_back();
        break;
      }
      case database::StateDelta::Type::ADD_OUT_EDGE:
        update(old, newr);
        reinterpret_cast<Vertex *>(*newr)->out_.emplace(
            delta.vertex_to_address, delta.edge_address, delta.edge_type);
        break;
      case database::StateDelta::Type::ADD_IN_EDGE:
        update(old, newr);
        reinterpret_cast<Vertex *>(*newr)->in_.emplace(
            delta.vertex_from_address, delta.edge_address, delta.edge_type);
        break;
      case database::StateDelta::Type::REMOVE_OUT_EDGE:
        update(old, newr);
        reinterpret_cast<Vertex *>(*newr)->out_.RemoveEdge(delta.edge_address);
        break;
      case database::StateDelta::Type::REMOVE_IN_EDGE:
        update(old, newr);
        reinterpret_cast<Vertex *>(*newr)->in_.RemoveEdge(delta.edge_address);
        break;
      default:
        // Effects of REMOVE VERTEX and REMOVE EDGE aren't visible in the
        // current command id so we can safely ignore this case.
        // Other deltas we're ignoring don't update record.
        break;
    }
  }
}

UpdatesRpcServer::UpdatesRpcServer(database::GraphDb *db,
                                   distributed::Coordination *coordination)
    : db_(db) {
  coordination->Register<UpdateRpc>([this](auto *req_reader,
                                           auto *res_builder) {
    UpdateReq req;
    slk::Load(&req, req_reader);
    using DeltaType = database::StateDelta::Type;
    auto &delta = req.member;
    switch (delta.type) {
      case DeltaType::SET_PROPERTY_VERTEX:
      case DeltaType::ADD_LABEL:
      case DeltaType::REMOVE_LABEL:
      case database::StateDelta::Type::REMOVE_OUT_EDGE:
      case database::StateDelta::Type::REMOVE_IN_EDGE: {
        UpdateRes res(GetUpdates(vertex_updates_, delta.transaction_id)
                          .Emplace(delta, req.worker_id));
        slk::Save(res, res_builder);
        return;
      }
      case DeltaType::SET_PROPERTY_EDGE: {
        UpdateRes res(GetUpdates(edge_updates_, delta.transaction_id)
                          .Emplace(delta, req.worker_id));
        slk::Save(res, res_builder);
        return;
      }
      default:
        LOG(FATAL) << "Can't perform a remote update with delta type: "
                   << static_cast<int>(req.member.type);
    }
  });

  coordination->Register<UpdateApplyRpc>(
      [this](auto *req_reader, auto *res_builder) {
        UpdateApplyReq req;
        slk::Load(&req, req_reader);
        UpdateApplyRes res(Apply(req.member));
        slk::Save(res, res_builder);
      });

  coordination->Register<CreateVertexRpc>([this](auto *req_reader,
                                                 auto *res_builder) {
    CreateVertexReq req;
    slk::Load(&req, req_reader);
    auto result = GetUpdates(vertex_updates_, req.member.tx_id)
                      .CreateVertex(req.member.labels, req.member.properties,
                                    req.member.cypher_id);
    CreateVertexRes res(
        CreateResult{UpdateResult::DONE, result.cypher_id, result.gid});
    slk::Save(res, res_builder);
  });

  coordination->Register<CreateEdgeRpc>(
      [this](auto *req_reader, auto *res_builder) {
        CreateEdgeReq req;
        slk::Load(&req, req_reader);
        auto data = req.member;
        auto creation_result = CreateEdge(data);

        // If `from` and `to` are both on this worker, we handle it in this
        // RPC call. Do it only if CreateEdge succeeded.
        if (creation_result.result == UpdateResult::DONE &&
            data.to.worker_id() == db_->WorkerId()) {
          auto to_delta = database::StateDelta::AddInEdge(
              data.tx_id, data.to.gid(), {data.from, db_->WorkerId()},
              {creation_result.gid, db_->WorkerId()}, data.edge_type);
          creation_result.result =
              GetUpdates(vertex_updates_, data.tx_id)
                  .Emplace(to_delta, data.worker_id);
        }

        CreateEdgeRes res(creation_result);
        slk::Save(res, res_builder);
      });

  coordination->Register<AddInEdgeRpc>(
      [this](auto *req_reader, auto *res_builder) {
        AddInEdgeReq req;
        slk::Load(&req, req_reader);
        auto to_delta = database::StateDelta::AddInEdge(
            req.member.tx_id, req.member.to, req.member.from,
            req.member.edge_address, req.member.edge_type);
        auto result = GetUpdates(vertex_updates_, req.member.tx_id)
                          .Emplace(to_delta, req.member.worker_id);
        AddInEdgeRes res(result);
        slk::Save(res, res_builder);
      });

  coordination->Register<RemoveVertexRpc>(
      [this](auto *req_reader, auto *res_builder) {
        RemoveVertexReq req;
        slk::Load(&req, req_reader);
        auto to_delta = database::StateDelta::RemoveVertex(
            req.member.tx_id, req.member.gid, req.member.check_empty);
        auto result = GetUpdates(vertex_updates_, req.member.tx_id)
                          .Emplace(to_delta, req.member.worker_id);
        RemoveVertexRes res(result);
        slk::Save(res, res_builder);
      });

  coordination->Register<RemoveEdgeRpc>(
      [this](auto *req_reader, auto *res_builder) {
        RemoveEdgeReq req;
        slk::Load(&req, req_reader);
        RemoveEdgeRes res(RemoveEdge(req.member));
        slk::Save(res, res_builder);
      });

  coordination->Register<RemoveInEdgeRpc>([this](auto *req_reader,
                                                 auto *res_builder) {
    RemoveInEdgeReq req;
    slk::Load(&req, req_reader);
    auto data = req.member;
    RemoveInEdgeRes res(
        GetUpdates(vertex_updates_, data.tx_id)
            .Emplace(database::StateDelta::RemoveInEdge(data.tx_id, data.vertex,
                                                        data.edge_address),
                     data.worker_id));
    slk::Save(res, res_builder);
  });
}

UpdateResult UpdatesRpcServer::Apply(tx::TransactionId tx_id) {
  auto apply = [tx_id](auto &collection) {
    auto access = collection.access();
    auto found = access.find(tx_id);
    if (found == access.end()) {
      return UpdateResult::DONE;
    }
    auto result = found->second.Apply();
    access.remove(tx_id);
    return result;
  };

  auto vertex_result = apply(vertex_updates_);
  auto edge_result = apply(edge_updates_);
  if (vertex_result != UpdateResult::DONE) return vertex_result;
  if (edge_result != UpdateResult::DONE) return edge_result;
  return UpdateResult::DONE;
}

template <>
void UpdatesRpcServer::ApplyDeltasToRecord<Vertex>(tx::TransactionId tx_id,
                                                   gid::Gid gid, int worker_id,
                                                   Vertex **old,
                                                   Vertex **newr) {
  auto access = vertex_updates_.access();
  auto found = access.find(tx_id);
  if (found != access.end())
    found->second.ApplyDeltasToRecord(gid, worker_id, old, newr);
}

template <>
void UpdatesRpcServer::ApplyDeltasToRecord<Edge>(tx::TransactionId tx_id,
                                                   gid::Gid gid, int worker_id,
                                                   Edge **old,
                                                   Edge **newr) {
  auto access = edge_updates_.access();
  auto found = access.find(tx_id);
  if (found != access.end())
    found->second.ApplyDeltasToRecord(gid, worker_id, old, newr);
}

void UpdatesRpcServer::ClearTransactionalCache(
    tx::TransactionId oldest_active) {
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
UpdatesRpcServer::TransactionUpdates<TAccessor> &UpdatesRpcServer::GetUpdates(
    MapT<TAccessor> &updates, tx::TransactionId tx_id) {
  return updates.access()
      .emplace(tx_id, std::make_tuple(tx_id),
               std::make_tuple(std::ref(db_), tx_id))
      .first->second;
}

CreateResult UpdatesRpcServer::CreateEdge(const CreateEdgeReqData &req) {
  auto ids = GetUpdates(edge_updates_, req.tx_id)
                 .CreateEdge(req.from, req.to, req.edge_type, db_->WorkerId(),
                             req.cypher_id);

  // cypher_id doesn't have to be inserted because edge is stored
  // somewhere else in the cluster. Here is only vertex update.
  auto from_delta = database::StateDelta::AddOutEdge(
      req.tx_id, req.from, req.to, {ids.gid, db_->WorkerId()}, req.edge_type);

  auto result = GetUpdates(vertex_updates_, req.tx_id)
                    .Emplace(from_delta, req.worker_id);
  return {result, ids.cypher_id, ids.gid};
}

UpdateResult UpdatesRpcServer::RemoveEdge(const RemoveEdgeData &data) {
  // Edge removal.
  auto deletion_delta =
      database::StateDelta::RemoveEdge(data.tx_id, data.edge_id);
  auto result = GetUpdates(edge_updates_, data.tx_id)
                    .Emplace(deletion_delta, data.worker_id);

  // Out-edge removal, for sure is local.
  if (result == UpdateResult::DONE) {
    auto remove_out_delta = database::StateDelta::RemoveOutEdge(
        data.tx_id, data.vertex_from_id, {data.edge_id, db_->WorkerId()});
    result = GetUpdates(vertex_updates_, data.tx_id)
                 .Emplace(remove_out_delta, data.worker_id);
  }

  // In-edge removal, might not be local.
  if (result == UpdateResult::DONE &&
      data.vertex_to_address.worker_id() == db_->WorkerId()) {
    auto remove_in_delta = database::StateDelta::RemoveInEdge(
        data.tx_id, data.vertex_to_address.gid(),
        {data.edge_id, db_->WorkerId()});
    result = GetUpdates(vertex_updates_, data.tx_id)
                 .Emplace(remove_in_delta, data.worker_id);
  }

  return result;
}

template <>
VertexAccessor
UpdatesRpcServer::TransactionUpdates<VertexAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_->FindVertex(gid, false);
}

template <>
EdgeAccessor UpdatesRpcServer::TransactionUpdates<EdgeAccessor>::FindAccessor(
    gid::Gid gid) {
  return db_accessor_->FindEdge(gid, false);
}

}  // namespace distributed
