// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "replication/replication_server.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

using memgraph::storage::Delta;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::EdgeRef;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::LabelIndexStats;
using memgraph::storage::LabelPropertyIndexStats;
using memgraph::storage::PropertyId;
using memgraph::storage::UniqueConstraints;
using memgraph::storage::View;
using memgraph::storage::durability::WalDeltaData;

namespace memgraph::dbms {
namespace {
std::pair<uint64_t, WalDeltaData> ReadDelta(storage::durability::BaseDecoder *decoder) {
  try {
    auto timestamp = ReadWalDeltaHeader(decoder);
    SPDLOG_INFO("       Timestamp {}", timestamp);
    auto delta = ReadWalDeltaData(decoder);
    return {timestamp, delta};
  } catch (const slk::SlkReaderException &) {
    throw utils::BasicException("Missing data!");
  } catch (const storage::durability::RecoveryFailure &) {
    throw utils::BasicException("Invalid data!");
  }
};

std::optional<DatabaseAccess> GetDatabaseAccessor(dbms::DbmsHandler *dbms_handler, std::string_view db_name) {
  try {
#ifdef MG_ENTERPRISE
    auto acc = dbms_handler->Get(db_name);
#else
    if (db_name != dbms::kDefaultDB) {
      spdlog::warn("Trying to replicate a non-default database on a community replica.");
      return std::nullopt;
    }
    auto acc = dbms_handler->Get();
#endif
    if (!acc) {
      spdlog::error("Failed to get access to ", db_name);
      return std::nullopt;
    }
    auto *inmem_storage = dynamic_cast<storage::InMemoryStorage *>(acc.get()->storage());
    if (!inmem_storage || inmem_storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      spdlog::error("Database \"{}\" is not IN_MEMORY_TRANSACTIONAL.", db_name);
      return std::nullopt;
    }
    return std::optional{std::move(acc)};
  } catch (const dbms::UnknownDatabaseException &e) {
    spdlog::warn("No database \"{}\" on replica!", db_name);
    return std::nullopt;
  }
}
}  // namespace

void InMemoryReplicationHandlers::Register(dbms::DbmsHandler *dbms_handler, replication::ReplicationServer &server) {
  server.rpc_server_.Register<storage::replication::HeartbeatRpc>([dbms_handler](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received HeartbeatRpc");
    InMemoryReplicationHandlers::HeartbeatHandler(dbms_handler, req_reader, res_builder);
  });
  server.rpc_server_.Register<storage::replication::AppendDeltasRpc>(
      [dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received AppendDeltasRpc");
        InMemoryReplicationHandlers::AppendDeltasHandler(dbms_handler, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::SnapshotRpc>([dbms_handler](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received SnapshotRpc");
    InMemoryReplicationHandlers::SnapshotHandler(dbms_handler, req_reader, res_builder);
  });
  server.rpc_server_.Register<storage::replication::WalFilesRpc>([dbms_handler](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received WalFilesRpc");
    InMemoryReplicationHandlers::WalFilesHandler(dbms_handler, req_reader, res_builder);
  });
  server.rpc_server_.Register<storage::replication::CurrentWalRpc>([dbms_handler](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received CurrentWalRpc");
    InMemoryReplicationHandlers::CurrentWalHandler(dbms_handler, req_reader, res_builder);
  });
  server.rpc_server_.Register<storage::replication::TimestampRpc>([dbms_handler](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received TimestampRpc");
    InMemoryReplicationHandlers::TimestampHandler(dbms_handler, req_reader, res_builder);
  });
}

void InMemoryReplicationHandlers::HeartbeatHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                   slk::Builder *res_builder) {
  storage::replication::HeartbeatReq req;
  slk::Load(&req, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  auto const *storage = db_acc->get()->storage();
  storage::replication::HeartbeatRes res{storage->id(), true,
                                         storage->repl_storage_state_.last_commit_timestamp_.load(),
                                         std::string{storage->repl_storage_state_.epoch_.id()}};
  slk::Save(res, res_builder);
}

void InMemoryReplicationHandlers::AppendDeltasHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                      slk::Builder *res_builder) {
  storage::replication::AppendDeltasReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  storage::replication::Decoder decoder(req_reader);

  auto maybe_epoch_id = decoder.ReadString();
  MG_ASSERT(maybe_epoch_id, "Invalid replication message");

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  auto &repl_storage_state = storage->repl_storage_state_;
  if (*maybe_epoch_id != storage->repl_storage_state_.epoch_.id()) {
    auto prev_epoch = storage->repl_storage_state_.epoch_.SetEpoch(*maybe_epoch_id);
    repl_storage_state.AddEpochToHistoryForce(prev_epoch);
  }

  if (storage->wal_file_) {
    if (req.seq_num > storage->wal_file_->SequenceNumber() ||
        *maybe_epoch_id != storage->repl_storage_state_.epoch_.id()) {
      storage->wal_file_->FinalizeWal();
      storage->wal_file_.reset();
      storage->wal_seq_num_ = req.seq_num;
      spdlog::trace("Finalized WAL file");
    } else {
      MG_ASSERT(storage->wal_file_->SequenceNumber() == req.seq_num, "Invalid sequence number of current wal file");
      storage->wal_seq_num_ = req.seq_num + 1;
    }
  } else {
    storage->wal_seq_num_ = req.seq_num;
  }

  if (req.previous_commit_timestamp != repl_storage_state.last_commit_timestamp_.load()) {
    // Empty the stream
    bool transaction_complete = false;
    while (!transaction_complete) {
      SPDLOG_INFO("Skipping delta");
      const auto [timestamp, delta] = ReadDelta(&decoder);
      transaction_complete = storage::durability::IsWalDeltaDataTypeTransactionEnd(
          delta.type,
          storage::durability::kVersion);  // TODO: Check if we are always using the latest version when replicating
    }

    storage::replication::AppendDeltasRes res{storage->id(), false, repl_storage_state.last_commit_timestamp_.load()};
    slk::Save(res, res_builder);
    return;
  }

  ReadAndApplyDelta(
      storage, &decoder,
      storage::durability::kVersion);  // TODO: Check if we are always using the latest version when replicating

  storage::replication::AppendDeltasRes res{storage->id(), true, repl_storage_state.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from append deltas finished, replica is now up to date!");
}

void InMemoryReplicationHandlers::SnapshotHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                  slk::Builder *res_builder) {
  storage::replication::SnapshotReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->snapshot_directory_);

  const auto maybe_snapshot_path = decoder.ReadFile(storage->snapshot_directory_);
  MG_ASSERT(maybe_snapshot_path, "Failed to load snapshot!");
  spdlog::info("Received snapshot saved to {}", *maybe_snapshot_path);

  auto storage_guard = std::unique_lock{storage->main_lock_};
  spdlog::trace("Clearing database since recovering from snapshot.");
  // Clear the database
  storage->vertices_.clear();
  storage->edges_.clear();

  storage->constraints_.existence_constraints_ = std::make_unique<storage::ExistenceConstraints>();
  storage->constraints_.unique_constraints_ = std::make_unique<storage::InMemoryUniqueConstraints>();
  storage->indices_.label_index_ = std::make_unique<storage::InMemoryLabelIndex>();
  storage->indices_.label_property_index_ = std::make_unique<storage::InMemoryLabelPropertyIndex>();
  try {
    spdlog::debug("Loading snapshot");
    auto recovered_snapshot = storage::durability::LoadSnapshot(
        *maybe_snapshot_path, &storage->vertices_, &storage->edges_, &storage->repl_storage_state_.history,
        storage->name_id_mapper_.get(), &storage->edge_count_, storage->config_);
    spdlog::debug("Snapshot loaded successfully");
    // If this step is present it should always be the first step of
    // the recovery so we use the UUID we read from snasphost
    storage->uuid_ = std::move(recovered_snapshot.snapshot_info.uuid);
    storage->repl_storage_state_.epoch_.SetEpoch(std::move(recovered_snapshot.snapshot_info.epoch_id));
    const auto &recovery_info = recovered_snapshot.recovery_info;
    storage->vertex_id_ = recovery_info.next_vertex_id;
    storage->edge_id_ = recovery_info.next_edge_id;
    storage->timestamp_ = std::max(storage->timestamp_, recovery_info.next_timestamp);

    spdlog::trace("Recovering indices and constraints from snapshot.");
    storage::durability::RecoverIndicesAndConstraints(recovered_snapshot.indices_constraints, &storage->indices_,
                                                      &storage->constraints_, &storage->vertices_,
                                                      storage->name_id_mapper_.get());
  } catch (const storage::durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't load the snapshot because of: {}", e.what());
  }
  storage_guard.unlock();

  storage::replication::SnapshotRes res{storage->id(), true,
                                        storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files due to snapshot recovery.");
  // Delete other durability files
  auto snapshot_files = storage::durability::GetSnapshotFiles(storage->snapshot_directory_, storage->uuid_);
  for (const auto &[path, uuid, _] : snapshot_files) {
    if (path != *maybe_snapshot_path) {
      spdlog::trace("Deleting snapshot file {}", path);
      storage->file_retainer_.DeleteFile(path);
    }
  }

  spdlog::trace("Deleting old WAL files due to snapshot recovery.");
  auto wal_files = storage::durability::GetWalFiles(storage->wal_directory_, storage->uuid_);
  if (wal_files) {
    for (const auto &wal_file : *wal_files) {
      spdlog::trace("Deleting WAL file {}", wal_file.path);
      storage->file_retainer_.DeleteFile(wal_file.path);
    }

    storage->wal_file_.reset();
  }
  spdlog::debug("Replication recovery from snapshot finished!");
}

void InMemoryReplicationHandlers::WalFilesHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                  slk::Builder *res_builder) {
  storage::replication::WalFilesReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received WAL files: {}", wal_file_number);

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->wal_directory_);

  for (auto i = 0; i < wal_file_number; ++i) {
    LoadWal(storage, &decoder);
  }

  storage::replication::WalFilesRes res{storage->id(), true,
                                        storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from WAL files ended successfully, replica is now up to date!");
}

void InMemoryReplicationHandlers::CurrentWalHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                    slk::Builder *res_builder) {
  storage::replication::CurrentWalReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->wal_directory_);

  LoadWal(storage, &decoder);

  storage::replication::CurrentWalRes res{storage->id(), true,
                                          storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from current WAL ended successfully, replica is now up to date!");
}

void InMemoryReplicationHandlers::LoadWal(storage::InMemoryStorage *storage, storage::replication::Decoder *decoder) {
  const auto temp_wal_directory =
      std::filesystem::temp_directory_path() / "memgraph" / storage::durability::kWalDirectory;
  utils::EnsureDir(temp_wal_directory);
  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  MG_ASSERT(maybe_wal_path, "Failed to load WAL!");
  spdlog::trace("Received WAL saved to {}", *maybe_wal_path);
  try {
    auto wal_info = storage::durability::ReadWalInfo(*maybe_wal_path);
    if (wal_info.seq_num == 0) {
      storage->uuid_ = wal_info.uuid;
    }
    auto &replica_epoch = storage->repl_storage_state_.epoch_;
    if (wal_info.epoch_id != replica_epoch.id()) {
      auto prev_epoch = replica_epoch.SetEpoch(wal_info.epoch_id);
      storage->repl_storage_state_.AddEpochToHistoryForce(prev_epoch);
    }

    if (storage->wal_file_) {
      if (storage->wal_file_->SequenceNumber() != wal_info.seq_num) {
        storage->wal_file_->FinalizeWal();
        storage->wal_seq_num_ = wal_info.seq_num;
        storage->wal_file_.reset();
        spdlog::trace("WAL file {} finalized successfully", *maybe_wal_path);
      }
    } else {
      storage->wal_seq_num_ = wal_info.seq_num;
    }
    spdlog::trace("Loading WAL deltas from {}", *maybe_wal_path);
    storage::durability::Decoder wal;
    const auto version = wal.Initialize(*maybe_wal_path, storage::durability::kWalMagic);
    spdlog::debug("WAL file {} loaded successfully", *maybe_wal_path);
    if (!version) throw storage::durability::RecoveryFailure("Couldn't read WAL magic and/or version!");
    if (!storage::durability::IsVersionSupported(*version))
      throw storage::durability::RecoveryFailure("Invalid WAL version!");
    wal.SetPosition(wal_info.offset_deltas);

    for (size_t i = 0; i < wal_info.num_deltas;) {
      i += ReadAndApplyDelta(storage, &wal, *version);
    }

    spdlog::debug("Replication from current WAL successful!");
  } catch (const storage::durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", *maybe_wal_path, e.what());
  }
}

void InMemoryReplicationHandlers::TimestampHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader,
                                                   slk::Builder *res_builder) {
  storage::replication::TimestampReq req;
  slk::Load(&req, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.db_name);
  if (!db_acc) return;

  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  auto const *storage = db_acc->get()->storage();
  storage::replication::TimestampRes res{storage->id(), true,
                                         storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

uint64_t InMemoryReplicationHandlers::ReadAndApplyDelta(storage::InMemoryStorage *storage,
                                                        storage::durability::BaseDecoder *decoder,
                                                        const uint64_t version) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr bool kUniqueAccess = true;
  constexpr bool kSharedAccess = false;

  std::optional<std::pair<uint64_t, storage::InMemoryStorage::ReplicationAccessor>> commit_timestamp_and_accessor;
  auto get_transaction = [storage, &commit_timestamp_and_accessor](uint64_t commit_timestamp,
                                                                   bool unique = kSharedAccess) {
    if (!commit_timestamp_and_accessor) {
      std::unique_ptr<storage::Storage::Accessor> acc = nullptr;
      if (unique) {
        acc = storage->UniqueAccess(std::nullopt, false /*not main*/);
      } else {
        acc = storage->Access(std::nullopt, false /*not main*/);
      }
      auto inmem_acc = std::unique_ptr<storage::InMemoryStorage::InMemoryAccessor>(
          static_cast<storage::InMemoryStorage::InMemoryAccessor *>(acc.release()));
      commit_timestamp_and_accessor.emplace(commit_timestamp, std::move(*inmem_acc));
    } else if (commit_timestamp_and_accessor->first != commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return &commit_timestamp_and_accessor->second;
  };

  uint64_t applied_deltas = 0;
  auto max_commit_timestamp = storage->repl_storage_state_.last_commit_timestamp_.load();

  for (bool transaction_complete = false; !transaction_complete; ++applied_deltas) {
    const auto [timestamp, delta] = ReadDelta(decoder);
    if (timestamp > max_commit_timestamp) {
      max_commit_timestamp = timestamp;
    }

    transaction_complete = storage::durability::IsWalDeltaDataTypeTransactionEnd(delta.type, version);

    if (timestamp < storage->timestamp_) {
      continue;
    }

    SPDLOG_INFO("  Delta {}", applied_deltas);
    switch (delta.type) {
      case WalDeltaData::Type::VERTEX_CREATE: {
        spdlog::trace("       Create vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        transaction->CreateVertexEx(delta.vertex_create_delete.gid);
        break;
      }
      case WalDeltaData::Type::VERTEX_DELETE: {
        spdlog::trace("       Delete vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_create_delete.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = transaction->DeleteVertex(&*vertex);
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::VERTEX_ADD_LABEL: {
        spdlog::trace("       Vertex {} add label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->AddLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
        spdlog::trace("       Vertex {} remove label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->RemoveLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::VERTEX_SET_PROPERTY: {
        spdlog::trace("       Vertex {} set property {} to {}", delta.vertex_edge_set_property.gid.AsUint(),
                      delta.vertex_edge_set_property.property, delta.vertex_edge_set_property.value);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_edge_set_property.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->SetProperty(transaction->NameToProperty(delta.vertex_edge_set_property.property),
                                       delta.vertex_edge_set_property.value);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::EDGE_CREATE: {
        spdlog::trace("       Create edge {} of type {} from vertex {} to vertex {}",
                      delta.edge_create_delete.gid.AsUint(), delta.edge_create_delete.edge_type,
                      delta.edge_create_delete.from_vertex.AsUint(), delta.edge_create_delete.to_vertex.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, View::NEW);
        if (!from_vertex) throw utils::BasicException("Invalid transaction!");
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, View::NEW);
        if (!to_vertex) throw utils::BasicException("Invalid transaction!");
        auto edge = transaction->CreateEdgeEx(&*from_vertex, &*to_vertex,
                                              transaction->NameToEdgeType(delta.edge_create_delete.edge_type),
                                              delta.edge_create_delete.gid);
        if (edge.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::EDGE_DELETE: {
        spdlog::trace("       Delete edge {} of type {} from vertex {} to vertex {}",
                      delta.edge_create_delete.gid.AsUint(), delta.edge_create_delete.edge_type,
                      delta.edge_create_delete.from_vertex.AsUint(), delta.edge_create_delete.to_vertex.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, View::NEW);
        if (!from_vertex) throw utils::BasicException("Invalid transaction!");
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, View::NEW);
        if (!to_vertex) throw utils::BasicException("Invalid transaction!");
        auto edges = from_vertex->OutEdges(View::NEW, {transaction->NameToEdgeType(delta.edge_create_delete.edge_type)},
                                           &*to_vertex);
        if (edges.HasError()) throw utils::BasicException("Invalid transaction!");
        if (edges->edges.size() != 1) throw utils::BasicException("Invalid transaction!");
        auto &edge = (*edges).edges[0];
        auto ret = transaction->DeleteEdge(&edge);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::EDGE_SET_PROPERTY: {
        spdlog::trace("       Edge {} set property {} to {}", delta.vertex_edge_set_property.gid.AsUint(),
                      delta.vertex_edge_set_property.property, delta.vertex_edge_set_property.value);
        if (!storage->config_.items.properties_on_edges)
          throw utils::BasicException(
              "Can't set properties on edges because properties on edges "
              "are disabled!");

        auto *transaction = get_transaction(timestamp);

        // The following block of code effectively implements `FindEdge` and
        // yields an accessor that is only valid for managing the edge's
        // properties.
        auto edge = edge_acc.find(delta.vertex_edge_set_property.gid);
        if (edge == edge_acc.end()) throw utils::BasicException("Invalid transaction!");
        // The edge visibility check must be done here manually because we
        // don't allow direct access to the edges through the public API.
        {
          bool is_visible = true;
          Delta *delta = nullptr;
          {
            auto guard = std::shared_lock{edge->lock};
            is_visible = !edge->deleted;
            delta = edge->delta;
          }
          ApplyDeltasForRead(&transaction->GetTransaction(), delta, View::NEW, [&is_visible](const Delta &delta) {
            switch (delta.action) {
              case Delta::Action::ADD_LABEL:
              case Delta::Action::REMOVE_LABEL:
              case Delta::Action::SET_PROPERTY:
              case Delta::Action::ADD_IN_EDGE:
              case Delta::Action::ADD_OUT_EDGE:
              case Delta::Action::REMOVE_IN_EDGE:
              case Delta::Action::REMOVE_OUT_EDGE:
                break;
              case Delta::Action::RECREATE_OBJECT: {
                is_visible = true;
                break;
              }
              case Delta::Action::DELETE_DESERIALIZED_OBJECT:
              case Delta::Action::DELETE_OBJECT: {
                is_visible = false;
                break;
              }
            }
          });
          if (!is_visible) throw utils::BasicException("Invalid transaction!");
        }
        EdgeRef edge_ref(&*edge);
        // Here we create an edge accessor that we will use to get the
        // properties of the edge. The accessor is created with an invalid
        // type and invalid from/to pointers because we don't know them
        // here, but that isn't an issue because we won't use that part of
        // the API here.
        auto ea = EdgeAccessor{edge_ref, EdgeTypeId::FromUint(0UL),     nullptr, nullptr,
                               storage,  &transaction->GetTransaction()};

        auto ret = ea.SetProperty(transaction->NameToProperty(delta.vertex_edge_set_property.property),
                                  delta.vertex_edge_set_property.value);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }

      case WalDeltaData::Type::TRANSACTION_END: {
        spdlog::trace("       Transaction end");
        if (!commit_timestamp_and_accessor || commit_timestamp_and_accessor->first != timestamp)
          throw utils::BasicException("Invalid commit data!");
        auto ret =
            commit_timestamp_and_accessor->second.Commit(commit_timestamp_and_accessor->first, false /* not main */);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        commit_timestamp_and_accessor = std::nullopt;
        break;
      }

      case WalDeltaData::Type::LABEL_INDEX_CREATE: {
        spdlog::trace("       Create label index on :{}", delta.operation_label.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->CreateIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_DROP: {
        spdlog::trace("       Drop label index on :{}", delta.operation_label.label);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->DropIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_STATS_SET: {
        spdlog::trace("       Set label index statistics on :{}", delta.operation_label_stats.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        const auto label = storage->NameToLabel(delta.operation_label_stats.label);
        LabelIndexStats stats{};
        if (!FromJson(delta.operation_label_stats.stats, stats)) {
          throw utils::BasicException("Failed to read statistics!");
        }
        transaction->SetIndexStats(label, stats);
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
        spdlog::trace("       Create label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->CreateIndex(storage->NameToLabel(delta.operation_label_property.label),
                              storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
        spdlog::trace("       Drop label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->DropIndex(storage->NameToLabel(delta.operation_label_property.label),
                            storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET: {
        const auto &info = delta.operation_label_property_stats;
        spdlog::trace("       Set label-property index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        const auto label = storage->NameToLabel(info.label);
        const auto property = storage->NameToProperty(info.property);
        LabelPropertyIndexStats stats{};
        if (!FromJson(info.stats, stats)) {
          throw utils::BasicException("Failed to read statistics!");
        }
        transaction->SetIndexStats(label, property, stats);
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label-property index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelPropertyIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
        spdlog::trace("       Create existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret =
            transaction->CreateExistenceConstraint(storage->NameToLabel(delta.operation_label_property.label),
                                                   storage->NameToProperty(delta.operation_label_property.property));
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
        spdlog::trace("       Drop existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->DropExistenceConstraint(storage->NameToLabel(delta.operation_label_property.label),
                                          storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Create unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage->NameToProperty(prop));
        }
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret = transaction->CreateUniqueConstraint(storage->NameToLabel(delta.operation_label_properties.label),
                                                       properties);
        if (!ret.HasValue() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Drop unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage->NameToProperty(prop));
        }
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret =
            transaction->DropUniqueConstraint(storage->NameToLabel(delta.operation_label_properties.label), properties);
        if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
          throw utils::BasicException("Invalid transaction!");
        }
        break;
      }
    }
  }

  if (commit_timestamp_and_accessor) throw utils::BasicException("Did not finish the transaction!");

  storage->repl_storage_state_.last_commit_timestamp_ = max_commit_timestamp;

  spdlog::debug("Applied {} deltas", applied_deltas);
  return applied_deltas;
}

}  // namespace memgraph::dbms
