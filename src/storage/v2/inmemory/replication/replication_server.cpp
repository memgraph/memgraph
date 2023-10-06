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

#include "storage/v2/inmemory/replication/replication_server.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

namespace memgraph::storage {
namespace {
std::pair<uint64_t, durability::WalDeltaData> ReadDelta(durability::BaseDecoder *decoder) {
  try {
    auto timestamp = ReadWalDeltaHeader(decoder);
    SPDLOG_INFO("       Timestamp {}", timestamp);
    auto delta = ReadWalDeltaData(decoder);
    return {timestamp, delta};
  } catch (const slk::SlkReaderException &) {
    throw utils::BasicException("Missing data!");
  } catch (const durability::RecoveryFailure &) {
    throw utils::BasicException("Invalid data!");
  }
};
}  // namespace

InMemoryReplicationServer::InMemoryReplicationServer(InMemoryStorage *storage,
                                                     const replication::ReplicationServerConfig &config)
    : ReplicationServer{config}, storage_(storage) {
  rpc_server_.Register<replication::HeartbeatRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received HeartbeatRpc");
    this->HeartbeatHandler(req_reader, res_builder);
  });

  rpc_server_.Register<replication::AppendDeltasRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received AppendDeltasRpc");
    this->AppendDeltasHandler(req_reader, res_builder);
  });
  rpc_server_.Register<replication::SnapshotRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received SnapshotRpc");
    this->SnapshotHandler(req_reader, res_builder);
  });
  rpc_server_.Register<replication::WalFilesRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received WalFilesRpc");
    this->WalFilesHandler(req_reader, res_builder);
  });
  rpc_server_.Register<replication::CurrentWalRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received CurrentWalRpc");
    this->CurrentWalHandler(req_reader, res_builder);
  });
  rpc_server_.Register<replication::TimestampRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received TimestampRpc");
    this->TimestampHandler(req_reader, res_builder);
  });
}

void InMemoryReplicationServer::HeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::HeartbeatReq req;
  slk::Load(&req, req_reader);
  replication::HeartbeatRes res{true, storage_->replication_state_.last_commit_timestamp_.load(),
                                storage_->replication_state_.GetEpoch().id};
  slk::Save(res, res_builder);
}

void InMemoryReplicationServer::AppendDeltasHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::AppendDeltasReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  auto maybe_epoch_id = decoder.ReadString();
  MG_ASSERT(maybe_epoch_id, "Invalid replication message");

  if (*maybe_epoch_id != storage_->replication_state_.GetEpoch().id) {
    storage_->replication_state_.AppendEpoch(*maybe_epoch_id);
  }

  if (storage_->wal_file_) {
    if (req.seq_num > storage_->wal_file_->SequenceNumber() ||
        *maybe_epoch_id != storage_->replication_state_.GetEpoch().id) {
      storage_->wal_file_->FinalizeWal();
      storage_->wal_file_.reset();
      storage_->wal_seq_num_ = req.seq_num;
      spdlog::trace("Finalized WAL file");
    } else {
      MG_ASSERT(storage_->wal_file_->SequenceNumber() == req.seq_num, "Invalid sequence number of current wal file");
      storage_->wal_seq_num_ = req.seq_num + 1;
    }
  } else {
    storage_->wal_seq_num_ = req.seq_num;
  }

  if (req.previous_commit_timestamp != storage_->replication_state_.last_commit_timestamp_.load()) {
    // Empty the stream
    bool transaction_complete = false;
    while (!transaction_complete) {
      SPDLOG_INFO("Skipping delta");
      const auto [timestamp, delta] = ReadDelta(&decoder);
      transaction_complete = durability::IsWalDeltaDataTypeTransactionEnd(
          delta.type, durability::kVersion);  // TODO: Check if we are always using the latest version when replicating
    }

    replication::AppendDeltasRes res{false, storage_->replication_state_.last_commit_timestamp_.load()};
    slk::Save(res, res_builder);
    return;
  }

  ReadAndApplyDelta(storage_, &decoder,
                    durability::kVersion);  // TODO: Check if we are always using the latest version when replicating

  replication::AppendDeltasRes res{true, storage_->replication_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from append deltas finished, replica is now up to date!");
}

void InMemoryReplicationServer::SnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::SnapshotReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->snapshot_directory_);

  const auto maybe_snapshot_path = decoder.ReadFile(storage_->snapshot_directory_);
  MG_ASSERT(maybe_snapshot_path, "Failed to load snapshot!");
  spdlog::info("Received snapshot saved to {}", *maybe_snapshot_path);

  auto storage_guard = std::unique_lock{storage_->main_lock_};
  spdlog::trace("Clearing database since recovering from snapshot.");
  // Clear the database
  storage_->vertices_.clear();
  storage_->edges_.clear();

  storage_->constraints_.existence_constraints_ = std::make_unique<ExistenceConstraints>();
  storage_->constraints_.unique_constraints_ = std::make_unique<InMemoryUniqueConstraints>();
  storage_->indices_.label_index_ = std::make_unique<InMemoryLabelIndex>();
  storage_->indices_.label_property_index_ = std::make_unique<InMemoryLabelPropertyIndex>();
  try {
    spdlog::debug("Loading snapshot");
    auto &epoch =
        storage_->replication_state_
            .GetEpoch();  // This needs to be a non-const ref since we are updating it in LoadSnapshot TODO fix
    auto recovered_snapshot = durability::LoadSnapshot(
        *maybe_snapshot_path, &storage_->vertices_, &storage_->edges_, &storage_->replication_state_.history,
        storage_->name_id_mapper_.get(), &storage_->edge_count_, storage_->config_);
    spdlog::debug("Snapshot loaded successfully");
    // If this step is present it should always be the first step of
    // the recovery so we use the UUID we read from snasphost
    storage_->uuid_ = std::move(recovered_snapshot.snapshot_info.uuid);
    epoch.id = std::move(recovered_snapshot.snapshot_info.epoch_id);
    const auto &recovery_info = recovered_snapshot.recovery_info;
    storage_->vertex_id_ = recovery_info.next_vertex_id;
    storage_->edge_id_ = recovery_info.next_edge_id;
    storage_->timestamp_ = std::max(storage_->timestamp_, recovery_info.next_timestamp);

    spdlog::trace("Recovering indices and constraints from snapshot.");
    durability::RecoverIndicesAndConstraints(recovered_snapshot.indices_constraints, &storage_->indices_,
                                             &storage_->constraints_, &storage_->vertices_);
  } catch (const durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't load the snapshot because of: {}", e.what());
  }
  storage_guard.unlock();

  replication::SnapshotRes res{true, storage_->replication_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files due to snapshot recovery.");
  // Delete other durability files
  auto snapshot_files = durability::GetSnapshotFiles(storage_->snapshot_directory_, storage_->uuid_);
  for (const auto &[path, uuid, _] : snapshot_files) {
    if (path != *maybe_snapshot_path) {
      spdlog::trace("Deleting snapshot file {}", path);
      storage_->file_retainer_.DeleteFile(path);
    }
  }

  spdlog::trace("Deleting old WAL files due to snapshot recovery.");
  auto wal_files = durability::GetWalFiles(storage_->wal_directory_, storage_->uuid_);
  if (wal_files) {
    for (const auto &wal_file : *wal_files) {
      spdlog::trace("Deleting WAL file {}", wal_file.path);
      storage_->file_retainer_.DeleteFile(wal_file.path);
    }

    storage_->wal_file_.reset();
  }
  spdlog::debug("Replication recovery from snapshot finished!");
}

void InMemoryReplicationServer::WalFilesHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::WalFilesReq req;
  slk::Load(&req, req_reader);

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received WAL files: {}", wal_file_number);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->wal_directory_);

  for (auto i = 0; i < wal_file_number; ++i) {
    LoadWal(storage_, &decoder);
  }

  replication::WalFilesRes res{true, storage_->replication_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from WAL files ended successfully, replica is now up to date!");
}

void InMemoryReplicationServer::CurrentWalHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::CurrentWalReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->wal_directory_);

  LoadWal(storage_, &decoder);

  replication::CurrentWalRes res{true, storage_->replication_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from current WAL ended successfully, replica is now up to date!");
}

void InMemoryReplicationServer::LoadWal(InMemoryStorage *storage, replication::Decoder *decoder) {
  const auto temp_wal_directory = std::filesystem::temp_directory_path() / "memgraph" / durability::kWalDirectory;
  utils::EnsureDir(temp_wal_directory);
  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  MG_ASSERT(maybe_wal_path, "Failed to load WAL!");
  spdlog::trace("Received WAL saved to {}", *maybe_wal_path);
  try {
    auto wal_info = durability::ReadWalInfo(*maybe_wal_path);
    if (wal_info.seq_num == 0) {
      storage->uuid_ = wal_info.uuid;
    }

    if (wal_info.epoch_id != storage->replication_state_.GetEpoch().id) {
      storage->replication_state_.AppendEpoch(wal_info.epoch_id);
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
    durability::Decoder wal;
    const auto version = wal.Initialize(*maybe_wal_path, durability::kWalMagic);
    spdlog::debug("WAL file {} loaded successfully", *maybe_wal_path);
    if (!version) throw durability::RecoveryFailure("Couldn't read WAL magic and/or version!");
    if (!durability::IsVersionSupported(*version)) throw durability::RecoveryFailure("Invalid WAL version!");
    wal.SetPosition(wal_info.offset_deltas);

    for (size_t i = 0; i < wal_info.num_deltas;) {
      i += ReadAndApplyDelta(storage, &wal, *version);
    }

    spdlog::debug("Replication from current WAL successful!");
  } catch (const durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", *maybe_wal_path, e.what());
  }
}

void InMemoryReplicationServer::TimestampHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::TimestampReq req;
  slk::Load(&req, req_reader);

  replication::TimestampRes res{true, storage_->replication_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

uint64_t InMemoryReplicationServer::ReadAndApplyDelta(InMemoryStorage *storage, durability::BaseDecoder *decoder,
                                                      const uint64_t version) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr bool kUniqueAccess = true;

  std::optional<std::pair<uint64_t, InMemoryStorage::ReplicationAccessor>> commit_timestamp_and_accessor;
  auto get_transaction = [storage, &commit_timestamp_and_accessor](uint64_t commit_timestamp,
                                                                   bool unique = !kUniqueAccess) {
    if (!commit_timestamp_and_accessor) {
      std::unique_ptr<Storage::Accessor> acc = nullptr;
      if (unique) {
        acc = storage->UniqueAccess(std::nullopt);
      } else {
        acc = storage->Access(std::nullopt);
      }
      auto inmem_acc = std::unique_ptr<InMemoryStorage::InMemoryAccessor>(
          static_cast<InMemoryStorage::InMemoryAccessor *>(acc.release()));
      commit_timestamp_and_accessor.emplace(commit_timestamp, std::move(*inmem_acc));
    } else if (commit_timestamp_and_accessor->first != commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return &commit_timestamp_and_accessor->second;
  };

  uint64_t applied_deltas = 0;
  auto max_commit_timestamp = storage->replication_state_.last_commit_timestamp_.load();

  for (bool transaction_complete = false; !transaction_complete; ++applied_deltas) {
    const auto [timestamp, delta] = ReadDelta(decoder);
    if (timestamp > max_commit_timestamp) {
      max_commit_timestamp = timestamp;
    }

    transaction_complete = durability::IsWalDeltaDataTypeTransactionEnd(delta.type, version);

    if (timestamp < storage->timestamp_) {
      continue;
    }

    SPDLOG_INFO("  Delta {}", applied_deltas);
    switch (delta.type) {
      case durability::WalDeltaData::Type::VERTEX_CREATE: {
        spdlog::trace("       Create vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        transaction->CreateVertexEx(delta.vertex_create_delete.gid);
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_DELETE: {
        spdlog::trace("       Delete vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_create_delete.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = transaction->DeleteVertex(&*vertex);
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_ADD_LABEL: {
        spdlog::trace("       Vertex {} add label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->AddLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
        spdlog::trace("       Vertex {} remove label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->RemoveLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_SET_PROPERTY: {
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
      case durability::WalDeltaData::Type::EDGE_CREATE: {
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
      case durability::WalDeltaData::Type::EDGE_DELETE: {
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
      case durability::WalDeltaData::Type::EDGE_SET_PROPERTY: {
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

      case durability::WalDeltaData::Type::TRANSACTION_END: {
        spdlog::trace("       Transaction end");
        if (!commit_timestamp_and_accessor || commit_timestamp_and_accessor->first != timestamp)
          throw utils::BasicException("Invalid commit data!");
        auto ret = commit_timestamp_and_accessor->second.Commit(commit_timestamp_and_accessor->first);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        commit_timestamp_and_accessor = std::nullopt;
        break;
      }

      case durability::WalDeltaData::Type::LABEL_INDEX_CREATE: {
        spdlog::trace("       Create label index on :{}", delta.operation_label.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->CreateIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::LABEL_INDEX_DROP: {
        spdlog::trace("       Drop label index on :{}", delta.operation_label.label);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->DropIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::LABEL_INDEX_STATS_SET: {
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
      case durability::WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
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
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
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
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET: {
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
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label-property index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelPropertyIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
        spdlog::trace("       Create existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret =
            transaction->CreateExistenceConstraint(storage->NameToLabel(delta.operation_label_property.label),
                                                   storage->NameToProperty(delta.operation_label_property.property));
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
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
      case durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
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
      case durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
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

  storage->replication_state_.last_commit_timestamp_ = max_commit_timestamp;

  spdlog::debug("Applied {} deltas", applied_deltas);
  return applied_deltas;
}

}  // namespace memgraph::storage
