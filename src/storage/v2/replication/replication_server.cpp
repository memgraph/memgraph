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

#include "storage/v2/replication/replication_server.hpp"
#include <atomic>
#include <filesystem>

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/exceptions.hpp"

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

Storage::ReplicationServer::ReplicationServer(Storage *storage, io::network::Endpoint endpoint,
                                              const replication::ReplicationServerConfig &config)
    : storage_(storage) {
  // Create RPC server.
  if (config.ssl) {
    rpc_server_context_.emplace(config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                config.ssl->verify_peer);
  } else {
    rpc_server_context_.emplace();
  }
  // NOTE: The replication server must have a single thread for processing
  // because there is no need for more processing threads - each replica can
  // have only a single main server. Also, the single-threaded guarantee
  // simplifies the rest of the implementation.
  rpc_server_.emplace(std::move(endpoint), &*rpc_server_context_,
                      /* workers_count = */ 1);

  rpc_server_->Register<replication::HeartbeatRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received HeartbeatRpc");
    this->HeartbeatHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::FrequentHeartbeatRpc>([](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received FrequentHeartbeatRpc");
    FrequentHeartbeatHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::AppendDeltasRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received AppendDeltasRpc");
    this->AppendDeltasHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::SnapshotRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received SnapshotRpc");
    this->SnapshotHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::WalFilesRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received WalFilesRpc");
    this->WalFilesHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::CurrentWalRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received CurrentWalRpc");
    this->CurrentWalHandler(req_reader, res_builder);
  });
  rpc_server_->Register<replication::TimestampRpc>([this](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received TimestampRpc");
    this->TimestampHandler(req_reader, res_builder);
  });
  rpc_server_->Start();
}

void Storage::ReplicationServer::HeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::HeartbeatReq req;
  slk::Load(&req, req_reader);
  replication::HeartbeatRes res{true, storage_->last_commit_timestamp_.load(), storage_->epoch_id_};
  slk::Save(res, res_builder);
}

void Storage::ReplicationServer::FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::FrequentHeartbeatReq req;
  slk::Load(&req, req_reader);
  replication::FrequentHeartbeatRes res{true};
  slk::Save(res, res_builder);
}

void Storage::ReplicationServer::AppendDeltasHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::AppendDeltasReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  auto maybe_epoch_id = decoder.ReadString();
  MG_ASSERT(maybe_epoch_id, "Invalid replication message");

  if (*maybe_epoch_id != storage_->epoch_id_) {
    storage_->epoch_history_.emplace_back(std::move(storage_->epoch_id_), storage_->last_commit_timestamp_);
    storage_->epoch_id_ = std::move(*maybe_epoch_id);
  }

  if (storage_->wal_file_) {
    if (req.seq_num > storage_->wal_file_->SequenceNumber() || *maybe_epoch_id != storage_->epoch_id_) {
      storage_->wal_file_->FinalizeWal();
      storage_->wal_file_.reset();
      storage_->wal_seq_num_ = req.seq_num;
    } else {
      MG_ASSERT(storage_->wal_file_->SequenceNumber() == req.seq_num, "Invalid sequence number of current wal file");
      storage_->wal_seq_num_ = req.seq_num + 1;
    }
  } else {
    storage_->wal_seq_num_ = req.seq_num;
  }

  if (req.previous_commit_timestamp != storage_->last_commit_timestamp_.load()) {
    // Empty the stream
    bool transaction_complete = false;
    while (!transaction_complete) {
      SPDLOG_INFO("Skipping delta");
      const auto [timestamp, delta] = ReadDelta(&decoder);
      transaction_complete = durability::IsWalDeltaDataTypeTransactionEnd(delta.type);
    }

    replication::AppendDeltasRes res{false, storage_->last_commit_timestamp_.load()};
    slk::Save(res, res_builder);
    return;
  }

  ReadAndApplyDelta(&decoder);

  replication::AppendDeltasRes res{true, storage_->last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

void Storage::ReplicationServer::SnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::SnapshotReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->snapshot_directory_);

  const auto maybe_snapshot_path = decoder.ReadFile(storage_->snapshot_directory_);
  MG_ASSERT(maybe_snapshot_path, "Failed to load snapshot!");
  spdlog::info("Received snapshot saved to {}", *maybe_snapshot_path);

  std::unique_lock<utils::RWLock> storage_guard(storage_->main_lock_);
  // Clear the database
  storage_->vertices_.clear();
  storage_->edges_.clear();

  storage_->constraints_ = Constraints();
  storage_->indices_.label_index = LabelIndex(&storage_->indices_, &storage_->constraints_, storage_->config_.items);
  storage_->indices_.label_property_index =
      LabelPropertyIndex(&storage_->indices_, &storage_->constraints_, storage_->config_.items);
  try {
    spdlog::debug("Loading snapshot");
    auto recovered_snapshot = durability::LoadSnapshot(*maybe_snapshot_path, &storage_->vertices_, &storage_->edges_,
                                                       &storage_->epoch_history_, &storage_->name_id_mapper_,
                                                       &storage_->edge_count_, storage_->config_);
    spdlog::debug("Snapshot loaded successfully");
    // If this step is present it should always be the first step of
    // the recovery so we use the UUID we read from snapshot
    storage_->uuid_ = std::move(recovered_snapshot.snapshot_info.uuid);
    storage_->epoch_id_ = std::move(recovered_snapshot.snapshot_info.epoch_id);
    const auto &recovery_info = recovered_snapshot.recovery_info;
    storage_->vertex_id_ = recovery_info.next_vertex_id;
    storage_->edge_id_ = recovery_info.next_edge_id;
    storage_->timestamp_ = std::max(storage_->timestamp_, recovery_info.next_timestamp);

    durability::RecoverIndicesAndConstraints(recovered_snapshot.indices_constraints, &storage_->indices_,
                                             &storage_->constraints_, &storage_->vertices_);
  } catch (const durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't load the snapshot because of: {}", e.what());
  }
  storage_guard.unlock();

  replication::SnapshotRes res{true, storage_->last_commit_timestamp_.load()};
  slk::Save(res, res_builder);

  // Delete other durability files
  auto snapshot_files = durability::GetSnapshotFiles(storage_->snapshot_directory_, storage_->uuid_);
  for (const auto &[path, uuid, _] : snapshot_files) {
    if (path != *maybe_snapshot_path) {
      storage_->file_retainer_.DeleteFile(path);
    }
  }

  auto wal_files = durability::GetWalFiles(storage_->wal_directory_, storage_->uuid_);
  if (wal_files) {
    for (const auto &wal_file : *wal_files) {
      storage_->file_retainer_.DeleteFile(wal_file.path);
    }

    storage_->wal_file_.reset();
  }
}

void Storage::ReplicationServer::WalFilesHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::WalFilesReq req;
  slk::Load(&req, req_reader);

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received WAL files: {}", wal_file_number);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->wal_directory_);

  for (auto i = 0; i < wal_file_number; ++i) {
    LoadWal(&decoder);
  }

  replication::WalFilesRes res{true, storage_->last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

void Storage::ReplicationServer::CurrentWalHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::CurrentWalReq req;
  slk::Load(&req, req_reader);

  replication::Decoder decoder(req_reader);

  utils::EnsureDirOrDie(storage_->wal_directory_);

  LoadWal(&decoder);

  replication::CurrentWalRes res{true, storage_->last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

void Storage::ReplicationServer::LoadWal(replication::Decoder *decoder) {
  const auto temp_wal_directory = std::filesystem::temp_directory_path() / "memgraph" / durability::kWalDirectory;
  utils::EnsureDir(temp_wal_directory);
  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  MG_ASSERT(maybe_wal_path, "Failed to load WAL!");
  spdlog::trace("Received WAL saved to {}", *maybe_wal_path);
  try {
    auto wal_info = durability::ReadWalInfo(*maybe_wal_path);
    if (wal_info.seq_num == 0) {
      storage_->uuid_ = wal_info.uuid;
    }

    if (wal_info.epoch_id != storage_->epoch_id_) {
      storage_->epoch_history_.emplace_back(wal_info.epoch_id, storage_->last_commit_timestamp_);
      storage_->epoch_id_ = std::move(wal_info.epoch_id);
    }

    if (storage_->wal_file_) {
      if (storage_->wal_file_->SequenceNumber() != wal_info.seq_num) {
        storage_->wal_file_->FinalizeWal();
        storage_->wal_seq_num_ = wal_info.seq_num;
        storage_->wal_file_.reset();
      }
    } else {
      storage_->wal_seq_num_ = wal_info.seq_num;
    }

    durability::Decoder wal;
    const auto version = wal.Initialize(*maybe_wal_path, durability::kWalMagic);
    if (!version) throw durability::RecoveryFailure("Couldn't read WAL magic and/or version!");
    if (!durability::IsVersionSupported(*version)) throw durability::RecoveryFailure("Invalid WAL version!");
    wal.SetPosition(wal_info.offset_deltas);

    for (size_t i = 0; i < wal_info.num_deltas;) {
      i += ReadAndApplyDelta(&wal);
    }

    spdlog::debug("{} loaded successfully", *maybe_wal_path);
  } catch (const durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", *maybe_wal_path, e.what());
  }
}

void Storage::ReplicationServer::TimestampHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::TimestampReq req;
  slk::Load(&req, req_reader);

  replication::TimestampRes res{true, storage_->last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

Storage::ReplicationServer::~ReplicationServer() {
  if (rpc_server_) {
    rpc_server_->Shutdown();
    rpc_server_->AwaitShutdown();
  }
}
uint64_t Storage::ReplicationServer::ReadAndApplyDelta(durability::BaseDecoder *decoder) {
  auto edge_acc = storage_->edges_.access();
  auto vertex_acc = storage_->vertices_.access();

  std::optional<std::pair<uint64_t, storage::Storage::Accessor>> commit_timestamp_and_accessor;
  auto get_transaction = [this, &commit_timestamp_and_accessor](uint64_t commit_timestamp) {
    if (!commit_timestamp_and_accessor) {
      commit_timestamp_and_accessor.emplace(commit_timestamp, storage_->Access());
    } else if (commit_timestamp_and_accessor->first != commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return &commit_timestamp_and_accessor->second;
  };

  uint64_t applied_deltas = 0;
  auto max_commit_timestamp = storage_->last_commit_timestamp_.load();

  for (bool transaction_complete = false; !transaction_complete; ++applied_deltas) {
    const auto [timestamp, delta] = ReadDelta(decoder);
    if (timestamp > max_commit_timestamp) {
      max_commit_timestamp = timestamp;
    }

    transaction_complete = durability::IsWalDeltaDataTypeTransactionEnd(delta.type);

    if (timestamp < storage_->timestamp_) {
      continue;
    }

    SPDLOG_INFO("  Delta {}", applied_deltas);
    switch (delta.type) {
      case durability::WalDeltaData::Type::VERTEX_CREATE: {
        spdlog::trace("       Create vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto transaction = get_transaction(timestamp);
        transaction->CreateVertex(delta.vertex_create_delete.gid);
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_DELETE: {
        spdlog::trace("       Delete vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_create_delete.gid, storage::View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = transaction->DeleteVertex(&*vertex);
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_ADD_LABEL: {
        spdlog::trace("       Vertex {} add label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, storage::View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->AddLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
        spdlog::trace("       Vertex {} remove label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, storage::View::NEW);
        if (!vertex) throw utils::BasicException("Invalid transaction!");
        auto ret = vertex->RemoveLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::VERTEX_SET_PROPERTY: {
        spdlog::trace("       Vertex {} set property {} to {}", delta.vertex_edge_set_property.gid.AsUint(),
                      delta.vertex_edge_set_property.property, delta.vertex_edge_set_property.value);
        auto transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_edge_set_property.gid, storage::View::NEW);
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
        auto transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, storage::View::NEW);
        if (!from_vertex) throw utils::BasicException("Invalid transaction!");
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, storage::View::NEW);
        if (!to_vertex) throw utils::BasicException("Invalid transaction!");
        auto edge = transaction->CreateEdge(&*from_vertex, &*to_vertex,
                                            transaction->NameToEdgeType(delta.edge_create_delete.edge_type),
                                            delta.edge_create_delete.gid);
        if (edge.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::EDGE_DELETE: {
        spdlog::trace("       Delete edge {} of type {} from vertex {} to vertex {}",
                      delta.edge_create_delete.gid.AsUint(), delta.edge_create_delete.edge_type,
                      delta.edge_create_delete.from_vertex.AsUint(), delta.edge_create_delete.to_vertex.AsUint());
        auto transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, storage::View::NEW);
        if (!from_vertex) throw utils::BasicException("Invalid transaction!");
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, storage::View::NEW);
        if (!to_vertex) throw utils::BasicException("Invalid transaction!");
        auto edges = from_vertex->OutEdges(
            storage::View::NEW, {transaction->NameToEdgeType(delta.edge_create_delete.edge_type)}, &*to_vertex);
        if (edges.HasError()) throw utils::BasicException("Invalid transaction!");
        if (edges->size() != 1) throw utils::BasicException("Invalid transaction!");
        auto &edge = (*edges)[0];
        auto ret = transaction->DeleteEdge(&edge);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::EDGE_SET_PROPERTY: {
        spdlog::trace("       Edge {} set property {} to {}", delta.vertex_edge_set_property.gid.AsUint(),
                      delta.vertex_edge_set_property.property, delta.vertex_edge_set_property.value);

        if (!storage_->config_.items.properties_on_edges)
          throw utils::BasicException(
              "Can't set properties on edges because properties on edges "
              "are disabled!");

        auto transaction = get_transaction(timestamp);

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
            std::lock_guard<utils::SpinLock> guard(edge->lock);
            is_visible = !edge->deleted;
            delta = edge->delta;
          }
          ApplyDeltasForRead(&transaction->transaction_, delta, View::NEW, [&is_visible](const Delta &delta) {
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
        auto ea = EdgeAccessor{edge_ref,
                               EdgeTypeId::FromUint(0UL),
                               nullptr,
                               nullptr,
                               &transaction->transaction_,
                               &storage_->indices_,
                               &storage_->constraints_,
                               storage_->config_.items};

        auto ret = ea.SetProperty(transaction->NameToProperty(delta.vertex_edge_set_property.property),
                                  delta.vertex_edge_set_property.value);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }

      case durability::WalDeltaData::Type::TRANSACTION_END: {
        spdlog::trace("       Transaction end");
        if (!commit_timestamp_and_accessor || commit_timestamp_and_accessor->first != timestamp)
          throw utils::BasicException("Invalid data!");
        auto ret = commit_timestamp_and_accessor->second.Commit(commit_timestamp_and_accessor->first);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        commit_timestamp_and_accessor = std::nullopt;
        break;
      }

      case durability::WalDeltaData::Type::LABEL_INDEX_CREATE: {
        spdlog::trace("       Create label index on :{}", delta.operation_label.label);
        // Need to send the timestamp
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        if (storage_->CreateIndex(storage_->NameToLabel(delta.operation_label.label), timestamp).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::LABEL_INDEX_DROP: {
        spdlog::trace("       Drop label index on :{}", delta.operation_label.label);
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        if (storage_->DropIndex(storage_->NameToLabel(delta.operation_label.label), timestamp).HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
        spdlog::trace("       Create label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        if (storage_
                ->CreateIndex(storage_->NameToLabel(delta.operation_label_property.label),
                              storage_->NameToProperty(delta.operation_label_property.property), timestamp)
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
        spdlog::trace("       Drop label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        if (storage_
                ->DropIndex(storage_->NameToLabel(delta.operation_label_property.label),
                            storage_->NameToProperty(delta.operation_label_property.property), timestamp)
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
        spdlog::trace("       Create existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        auto ret = storage_->CreateExistenceConstraint(
            storage_->NameToLabel(delta.operation_label_property.label),
            storage_->NameToProperty(delta.operation_label_property.property), timestamp);
        if (ret.HasError()) throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
        spdlog::trace("       Drop existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        if (storage_
                ->DropExistenceConstraint(storage_->NameToLabel(delta.operation_label_property.label),
                                          storage_->NameToProperty(delta.operation_label_property.property), timestamp)
                .HasError())
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Create unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage_->NameToProperty(prop));
        }
        auto ret = storage_->CreateUniqueConstraint(storage_->NameToLabel(delta.operation_label_properties.label),
                                                    properties, timestamp);
        if (!ret.HasValue() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
          throw utils::BasicException("Invalid transaction!");
        break;
      }
      case durability::WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Drop unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid transaction!");
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage_->NameToProperty(prop));
        }
        auto ret = storage_->DropUniqueConstraint(storage_->NameToLabel(delta.operation_label_properties.label),
                                                  properties, timestamp);
        if (ret.HasError() || ret.GetValue() != UniqueConstraints::DeletionStatus::SUCCESS)
          throw utils::BasicException("Invalid transaction!");
        break;
      }
    }
  }

  if (commit_timestamp_and_accessor) throw utils::BasicException("Invalid data!");

  storage_->last_commit_timestamp_ = max_commit_timestamp;

  return applied_deltas;
}
}  // namespace memgraph::storage
