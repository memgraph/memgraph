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

#include "storage/v2/storage.hpp"
#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>
#include "storage/v2/disk/disk_edge.hpp"
#include "storage/v2/disk/disk_vertex.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/result.hpp"

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include "io/network/endpoint.hpp"
#include "storage/v2/disk/edge_accessor.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/disk/vertex_accessor.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/rw_lock.hpp"
#include "utils/spin_lock.hpp"
#include "utils/stat.hpp"
#include "utils/uuid.hpp"

/// REPLICATION ///
#include "storage/v2/replication/replication_client.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "storage/v2/storage_error.hpp"

/// RocksDB
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

constexpr const char *outEdgeDirection = "0";
constexpr const char *inEdgeDirection = "1";

inline constexpr uint16_t kEpochHistoryRetention = 1000;

// /// Use it for operations that must successfully finish.
inline void AssertRocksDBStatus(const rocksdb::Status &status) {
  MG_ASSERT(status.ok(), "rocksdb: {}", status.ToString());
}

inline bool CheckRocksDBStatus(const rocksdb::Status &status) {
  if (!status.ok()) [[unlikely]] {
    spdlog::error("rocksdb: {}", status.ToString());
  }
  return status.ok();
}

}  // namespace

/// TODO: indices should be initialized at some point after
DiskStorage::DiskStorage(Config config)
    : indices_(&constraints_, config.items),
      isolation_level_(IsolationLevel::SNAPSHOT_ISOLATION),
      config_(config),
      snapshot_directory_(config_.durability.storage_directory / durability::kSnapshotDirectory),
      wal_directory_(config_.durability.storage_directory / durability::kWalDirectory),
      lock_file_path_(config_.durability.storage_directory / durability::kLockFile),
      uuid_(utils::GenerateUUID()),
      epoch_id_(utils::GenerateUUID()),
      global_locker_(file_retainer_.AddLocker()) {
  if (config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::DISABLED
      /// TODO(andi): When replication support will be added, uncomment this.
      // && replication_role_ == ReplicationRole::MAIN) {
  ) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit || config_.durability.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(config_.durability.storage_directory);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_.Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    MG_ASSERT(lock_file_handle_.AcquireLock(),
              "Couldn't acquire lock on the storage directory {}"
              "!\nAnother Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              config_.durability.storage_directory);
  }
  /// TODO(andi): Return capabilities for recovering data
  // if (config_.durability.recover_on_startup) {
  //   auto info = durability::RecoverData(snapshot_directory_, wal_directory_, &uuid_, &epoch_id_, &epoch_history_,
  //                                       & vertices_, &edges_, &edge_count_, &name_id_mapper_, &indices_,
  //                                       &constraints_, config_.items, &wal_seq_num_);
  //   if (info) {
  //     vertex_id_ = info->next_vertex_id;
  //     edge_id_ = info->next_edge_id;
  //     timestamp_ = std::max(timestamp_, info->next_timestamp);
  //     if (info->last_commit_timestamp) {
  //       last_commit_timestamp_ = *info->last_commit_timestamp;
  //     }
  //   }
  // } else if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
  //            config_.durability.snapshot_on_exit) {
  //   bool files_moved = false;
  //   auto backup_root = config_.durability.storage_directory / durability::kBackupDirectory;
  //   for (const auto &[path, dirname, what] :
  //        {std::make_tuple(snapshot_directory_, durability::kSnapshotDirectory, "snapshot"),
  //         std::make_tuple(wal_directory_, durability::kWalDirectory, "WAL")}) {
  //     if (!utils::DirExists(path)) continue;
  //     auto backup_curr = backup_root / dirname;
  //     std::error_code error_code;
  //     for (const auto &item : std::filesystem::directory_iterator(path, error_code)) {
  //       utils::EnsureDirOrDie(backup_root);
  //       utils::EnsureDirOrDie(backup_curr);
  //       std::error_code item_error_code;
  //       std::filesystem::rename(item.path(), backup_curr / item.path().filename(), item_error_code);
  //       MG_ASSERT(!item_error_code, "Couldn't move {} file {} because of: {}", what, item.path(),
  //                 item_error_code.message());
  //       files_moved = true;
  //     }
  //     MG_ASSERT(!error_code, "Couldn't backup {} files because of: {}", what, error_code.message());
  //   }
  //   if (files_moved) {
  //     spdlog::warn(
  //         "Since Memgraph was not supposed to recover on startup and "
  //         "durability is enabled, your current durability files will likely "
  //         "be overridden. To prevent important data loss, Memgraph has stored "
  //         "those files into a .backup directory inside the storage directory.");
  //   }
  // }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval, [this] {
      if (auto maybe_error = this->CreateSnapshot(); maybe_error.HasError()) {
        switch (maybe_error.GetError()) {
          case CreateSnapshotError::DisabledForReplica:
            spdlog::warn(
                utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
            break;
        }
      }
    });
  }
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    gc_runner_.Run("Storage GC", config_.gc.interval, [this] { this->CollectGarbage<false>(); });
  }

  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }

  /// TODO(andi): Return capabilities for restoring replicas
  // if (config_.durability.restore_replicas_on_startup) {
  //   spdlog::info("Replica's configuration will be stored and will be automatically restored in case of a crash.");
  //   utils::EnsureDirOrDie(config_.durability.storage_directory / durability::kReplicationDirectory);
  //   storage_ =
  //       std::make_unique<kvstore::KVStore>(config_.durability.storage_directory / durability::kReplicationDirectory);
  //   RestoreReplicas();
  // } else {
  //   spdlog::warn("Replicas' configuration will NOT be stored. When the server restarts, replicas will be
  //   forgotten.");
  // }
}

DiskStorage::~DiskStorage() {}

DiskStorage::DiskAccessor::DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction(isolation_level)),
      is_transaction_active_(true),
      config_(storage->config_.items) {}

DiskStorage::DiskAccessor::DiskAccessor(DiskAccessor &&other) noexcept
    : storage_(other.storage_),
      storage_guard_(std::move(other.storage_guard_)),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_),
      config_(other.config_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

DiskStorage::DiskAccessor::~DiskAccessor() {
  if (is_transaction_active_) {
    Abort();
  }

  FinalizeTransaction();
}

/// (De)serialization utilities

std::string DiskStorage::DiskAccessor::SerializeIdType(const auto &id) const { return std::to_string(id.AsUint()); }

auto DiskStorage::DiskAccessor::DeserializeIdType(const std::string &str) { return Gid::FromUint(std::stoull(str)); }

std::string DiskStorage::DiskAccessor::SerializeTimestamp(const uint64_t ts) { return std::to_string(ts); }

std::string DiskStorage::DiskAccessor::SerializeLabels(const std::vector<LabelId> &labels) {
  std::string result = std::to_string(labels[0].AsUint());
  std::string ser_labels = std::accumulate(
      std::next(labels.begin()), labels.end(), result,
      [](const std::string &join, const auto &label_id) { return join + "," + std::to_string(label_id.AsUint()); });
  return ser_labels;
}

std::string DiskStorage::DiskAccessor::SerializeProperties(PropertyStore &properties) {
  return properties.StringBuffer();
}

std::string DiskStorage::DiskAccessor::SerializeVertex(const VertexAccessor *vertex_acc) const {
  MG_ASSERT(commit_timestamp_.has_value(), "Transaction must be committed to serialize vertex.");
  auto labels = vertex_acc->Labels(storage::View::OLD);
  std::string result = labels.HasError() || (*labels).empty() ? "" : SerializeLabels(*labels) + "|";
  result += SerializeIdType(vertex_acc->Gid()) + "|";
  result += SerializeTimestamp(*commit_timestamp_);
  return result;
}

std::string DiskStorage::DiskAccessor::SerializeVertex(const Vertex &vertex) const {
  MG_ASSERT(commit_timestamp_.has_value(), "Transaction must be committed to serialize vertex.");
  std::string result = SerializeLabels(vertex.labels) + "|";
  result += SerializeIdType(vertex.gid) + "|";
  result += SerializeTimestamp(*commit_timestamp_);
  return result;
}

std::pair<std::string, std::string> DiskStorage::DiskAccessor::SerializeEdge(EdgeAccessor *edge_acc) const {
  MG_ASSERT(commit_timestamp_.has_value(), "Transaction must be committed to serialize edge.");
  // Serialized objects
  auto from_gid = SerializeIdType(edge_acc->FromVertex()->Gid());
  auto to_gid = SerializeIdType(edge_acc->ToVertex()->Gid());
  auto edge_type = SerializeIdType(edge_acc->EdgeType());
  auto edge_gid = SerializeIdType(edge_acc->Gid());
  // source->destination key
  std::string src_dest_key = from_gid + "|";
  src_dest_key += to_gid + "|";
  src_dest_key += outEdgeDirection;
  src_dest_key += "|" + edge_type + "|";
  src_dest_key += edge_gid + "|";
  src_dest_key += SerializeTimestamp(*commit_timestamp_);
  // destination->source key
  std::string dest_src_key = to_gid + "|";
  dest_src_key += from_gid + "|";
  dest_src_key += inEdgeDirection;
  dest_src_key += "|" + edge_type + "|";
  dest_src_key += edge_gid + "|";
  dest_src_key += SerializeTimestamp(*commit_timestamp_);
  return {src_dest_key, dest_src_key};
}

std::pair<std::string, std::string> DiskStorage::DiskAccessor::SerializeEdge(const Gid src_vertex_gid,
                                                                             const Gid dest_vertex_gid,
                                                                             EdgeTypeId edge_type_id,
                                                                             const Edge *edge) const {
  MG_ASSERT(commit_timestamp_.has_value(), "Transaction must be committed to serialize edge.");
  // Serialized objects
  auto from_gid = SerializeIdType(src_vertex_gid);
  auto to_gid = SerializeIdType(dest_vertex_gid);
  auto edge_type = SerializeIdType(edge_type_id);
  auto edge_gid = SerializeIdType(edge->gid);
  // source->destination key
  std::string src_dest_key = from_gid + "|";
  src_dest_key += to_gid + "|";
  src_dest_key += outEdgeDirection;
  src_dest_key += "|" + edge_type + "|";
  src_dest_key += edge_gid + "|";
  src_dest_key += SerializeTimestamp(*commit_timestamp_);
  // destination->source key
  std::string dest_src_key = to_gid + "|";
  dest_src_key += from_gid + "|";
  dest_src_key += inEdgeDirection;
  dest_src_key += "|" + edge_type + "|";
  dest_src_key += edge_gid + "|";
  dest_src_key += SerializeTimestamp(*commit_timestamp_);
  return {src_dest_key, dest_src_key};
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::DeserializeVertex(const std::string_view key,
                                                                             const std::string_view value) {
  /// Create vertex
  const std::vector<std::string> vertex_parts = utils::Split(key, "|");
  auto impl = CreateVertex(storage::Gid::FromUint(std::stoull(vertex_parts[1])));
  // Deserialize labels
  if (!vertex_parts[0].empty()) {
    auto labels = utils::Split(vertex_parts[0], ",");
    for (auto it = labels.begin(); it != labels.end(); it++) {
      // TODO(andi): Introduce error handling of adding labels
      impl->AddLabel(storage::LabelId::FromUint(std::stoull(*it)));
    }
  }
  impl->SetPropertyStore(value);
  /// Completely ignored for now
  // bool is_valid_entry = std::stoull(vertex_parts[2]) < transaction_.start_timestamp;
  return impl;
}

std::unique_ptr<EdgeAccessor> DiskStorage::DiskAccessor::DeserializeEdge(const std::string_view key,
                                                                         const std::string_view value) {
  const auto edge_parts = utils::Split(key, "|");
  auto [from_gid, to_gid] = std::invoke(
      [&](const auto &edge_parts) {
        if (edge_parts[2] == "0") {  // out edge
          return std::make_pair(edge_parts[0], edge_parts[1]);
        }
        // in edge
        return std::make_pair(edge_parts[1], edge_parts[0]);
      },
      edge_parts);
  // load vertex accessors
  auto from_acc = FindVertex(DeserializeIdType(from_gid), View::OLD);
  auto to_acc = FindVertex(DeserializeIdType(to_gid), View::OLD);
  if (!from_acc || !to_acc) {
    throw utils::BasicException("Non-existing vertices found during edge deserialization");
  }
  const auto edge_type_id = storage::EdgeTypeId::FromUint(std::stoull(edge_parts[3]));
  auto maybe_edge = CreateEdge(&*from_acc, &*to_acc, edge_type_id, DeserializeIdType(edge_parts[4]));
  // bool is_valid_entry = std::stoull(vertex_parts[5]) < transaction_.start_timestamp;
  MG_ASSERT(maybe_edge.HasValue());
  (*maybe_edge)->SetPropertyStore(value);
  // TODO: why is here explicitly deleted constructor and not in DeserializeVertex?
  return std::move(*maybe_edge);
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(View view) {
  auto it =
      std::unique_ptr<rocksdb::Iterator>(storage_->db_->NewIterator(rocksdb::ReadOptions(), storage_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    DeserializeVertex(it->key().ToStringView(), it->value().ToStringView());
  }
  return VerticesIterable(AllVerticesIterable(vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label)");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label, property)");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value,
                                                     View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label, property, value)");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property,
                                                     const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                     const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                     View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label, property, lower_bound, upper_bound)");
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::CreateVertex() {
  OOMExceptionEnabler oom_exception;
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(DiskVertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  lru_vertices_.insert(static_cast<DiskVertex *>(&*it));
  /// TODO(andi): it shoud be safe to do static_cast here since the resolvement can be done at compile time. But maybe,
  /// std::variant of vertices on query engine level is better? If we use std::variant, we can remove the static_cast
  /// here and for inserting into the lru cache also.
  return std::make_unique<DiskVertexAccessor>(static_cast<DiskVertex *>(&*it), &transaction_, &storage_->indices_,
                                              &storage_->constraints_, config_, storage::Gid::FromUint(gid));
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::CreateVertex(storage::Gid gid) {
  OOMExceptionEnabler oom_exception;
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  storage_->vertex_id_.store(std::max(storage_->vertex_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                             std::memory_order_release);
  auto acc = vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(DiskVertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  lru_vertices_.insert(static_cast<DiskVertex *>(&*it));
  /// TODO(andi): it shoud be safe to do static_cast here since the resolvement can be done at compile time. But maybe,
  /// std::variant of vertices on query engine level is better? If we use std::variant, we can remove the static_cast
  /// here and for inserting into the lru cache also.
  return std::make_unique<DiskVertexAccessor>(static_cast<DiskVertex *>(&*it), &transaction_, &storage_->indices_,
                                              &storage_->constraints_, config_, gid);
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::FindVertex(storage::Gid gid, View view) {
  /// Check if the vertex is in the cache.
  auto acc = vertices_.access();
  auto vertex_it = acc.find(gid);
  if (vertex_it != acc.end()) {
    return DiskVertexAccessor::Create(&*vertex_it, &transaction_, &storage_->indices_, &storage_->constraints_, config_,
                                      view);
  }
  /// If not in the memory, check whether it exists in RocksDB.
  auto it =
      std::unique_ptr<rocksdb::Iterator>(storage_->db_->NewIterator(rocksdb::ReadOptions(), storage_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto &key = it->key().ToString();
    if (const auto vertex_parts = utils::Split(key, "|"); vertex_parts[1] == SerializeIdType(gid)) {
      return DeserializeVertex(key, it->value().ToStringView());
    }
  }
  return nullptr;
}

Result<std::unique_ptr<VertexAccessor>> DiskStorage::DiskAccessor::DeleteVertex(VertexAccessor *vertex) {
  if (!CheckRocksDBStatus(
          storage_->db_->Delete(rocksdb::WriteOptions(), storage_->vertex_chandle, SerializeVertex(vertex)))) {
    return Error::SERIALIZATION_ERROR;
  }
  return Result<std::unique_ptr<VertexAccessor>>{std::unique_ptr<VertexAccessor>(vertex)};
}

Result<std::vector<std::unique_ptr<EdgeAccessor>>> DeleteEdges(const auto &edge_accessors) {
  // using ReturnType = std::vector<std::unique_ptr<EdgeAccessor>>;
  // ReturnType edge_accs;
  // for (auto &&it : edge_accessors) {
  //   if (const auto deleted_edge_res = DeleteEdge(it); !deleted_edge_res.HasError()) {
  //     return deleted_edge_res.GetError();
  //   }
  //   // edge_accs.push_back(std::make_unique<DiskEdgeAccessor>(std::move(it), nullptr, nullptr, nullptr));
  // }
  // return edge_accs;
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::DeleteEdges");
}

Result<std::optional<std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>>>
DiskStorage::DiskAccessor::DetachDeleteVertex(VertexAccessor *vertex) {
  // using ReturnType = std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>;
  // auto *disk_vertex_acc = dynamic_cast<DiskVertexAccessor *>(vertex);
  // MG_ASSERT(disk_vertex_acc,
  //           "VertexAccessor must be from the same storage as the storage accessor when deleting a vertex!");
  // MG_ASSERT(disk_vertex_acc->transaction_ == &transaction_,
  //           "VertexAccessor must be from the same transaction as the storage "
  //           "accessor when deleting a vertex!");
  // auto *vertex_ptr = disk_vertex_acc->vertex_;

  // auto del_vertex = DeleteVertex(vertex);
  // if (del_vertex.HasError()) {
  //   return del_vertex.GetError();
  // }

  // auto out_edges = vertex->OutEdges(storage::View::OLD);
  // auto in_edges = vertex->InEdges(storage::View::OLD);
  // if (out_edges.HasError() || in_edges.HasError()) {
  //   return out_edges.GetError();
  // }

  // if (auto del_edges = DeleteEdges(*out_edges), del_in_edges = DeleteEdges(*in_edges);
  //     del_edges.HasError() && del_in_edges.HasError()) {
  //   // TODO: optimize this using splice
  //   del_edges->insert(del_in_edges->end(), std::make_move_iterator(del_in_edges->begin()),
  //                     std::make_move_iterator(del_in_edges->end()));
  //   return std::make_optional<ReturnType>(
  //       std::make_unique<DiskVertexAccessor>(vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_,
  //                                            config_, vertex_ptr->gid, true),
  //       del_edges.GetValue());
  // }
  // return Error::SERIALIZATION_ERROR;
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::DetachDeleteVertex");
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                            EdgeTypeId edge_type, storage::Gid gid) {
  auto *disk_va_from = dynamic_cast<DiskVertexAccessor *>(from);
  auto *disk_va_to = dynamic_cast<DiskVertexAccessor *>(to);
  MG_ASSERT(disk_va_from,
            "Source VertexAccessor must be from the same storage as the storage accessor when creating an edge!");
  MG_ASSERT(disk_va_to,
            "Target VertexAccessor must be from the same storage as the storage accessor when creating an edge!");

  OOMExceptionEnabler oom_exception;
  MG_ASSERT(disk_va_from->transaction_ == disk_va_to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(disk_va_from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto from_vertex = disk_va_from->vertex_;
  auto to_vertex = disk_va_to->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock, std::defer_lock);
  std::unique_lock<utils::SpinLock> guard_to(to_vertex->lock, std::defer_lock);
  if (from_vertex->gid < to_vertex->gid) {
    guard_from.lock();
    guard_to.lock();
  } else if (from_vertex->gid > to_vertex->gid) {
    guard_to.lock();
    guard_from.lock();
  } else {
    // The vertices are the same vertex, only lock one.
    guard_from.lock();
  }

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  storage_->edge_id_.store(std::max(storage_->edge_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                           std::memory_order_release);

  /// TODO(andi): Remove this once we add full support for edges.
  MG_ASSERT(config_.properties_on_edges, "Properties on edges must be enabled currently for Disk version!");
  // if (config_.properties_on_edges) {
  auto acc = edges_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(DiskEdge(gid, delta));
  MG_ASSERT(inserted, "The edge must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
  auto edge = EdgeRef(&*it);
  delta->prev.Set(&*it);
  // }
  lru_edges_.insert(static_cast<DiskEdge *>(&*it));
  /// TODO(andi): it shoud be safe to do static_cast here since the resolvement can be done at compile time. But same as
  /// for the vertices, we should change query engine code to handle that properly.

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return Result<std::unique_ptr<EdgeAccessor>>{
      std::make_unique<DiskEdgeAccessor>(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                                         &storage_->constraints_, config_, gid)};
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                            EdgeTypeId edge_type) {
  auto *from_disk_vertex_acc = dynamic_cast<DiskVertexAccessor *>(from);
  MG_ASSERT(from_disk_vertex_acc,
            "VertexAccessor must be from the same storage as the storage accessor when deleting a vertex!");
  MG_ASSERT(from_disk_vertex_acc->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *to_disk_vertex_acc = dynamic_cast<DiskVertexAccessor *>(to);
  MG_ASSERT(to_disk_vertex_acc,
            "VertexAccessor must be from the same storage as the storage accessor when deleting a vertex!");
  MG_ASSERT(to_disk_vertex_acc->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");

  auto from_vertex = from_disk_vertex_acc->vertex_;
  auto to_vertex = to_disk_vertex_acc->vertex_;

  // Obtain the locks by `gid` order to avoid lock cycles.
  std::unique_lock<utils::SpinLock> guard_from(from_vertex->lock, std::defer_lock);
  std::unique_lock<utils::SpinLock> guard_to(to_vertex->lock, std::defer_lock);
  if (from_vertex->gid < to_vertex->gid) {
    guard_from.lock();
    guard_to.lock();
  } else if (from_vertex->gid > to_vertex->gid) {
    guard_to.lock();
    guard_from.lock();
  } else {
    // The vertices are the same vertex, only lock one.
    guard_from.lock();
  }

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  auto gid = storage::Gid::FromUint(storage_->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  /// TODO(andi): Remove this once we add full support for edges.
  MG_ASSERT(config_.properties_on_edges, "Properties on edges must be enabled currently for Disk version!");
  // if (config_.properties_on_edges) {
  auto acc = edges_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Edge(gid, delta));
  MG_ASSERT(inserted, "The edge must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
  auto edge = EdgeRef(&*it);
  delta->prev.Set(&*it);
  lru_edges_.insert(static_cast<DiskEdge *>(&*it));
  /// TODO(andi): it shoud be safe to do static_cast here since the resolvement can be done at compile time. But same
  /// as for the vertices, we should change query engine code to handle that properly.
  // }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return Result<std::unique_ptr<EdgeAccessor>>{
      std::make_unique<DiskEdgeAccessor>(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                                         &storage_->constraints_, config_, gid)};
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::DeleteEdge(EdgeAccessor *edge) {
  auto *disk_edge_acc = dynamic_cast<DiskEdgeAccessor *>(edge);
  MG_ASSERT(disk_edge_acc, "EdgeAccessor must be from the same storage as the storage accessor when deleting an edge!");
  MG_ASSERT(disk_edge_acc->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when deleting an edge!");
  auto edge_ref = disk_edge_acc->edge_;
  auto edge_type = disk_edge_acc->edge_type_;

  auto *from_vertex = disk_edge_acc->from_vertex_;
  auto *to_vertex = disk_edge_acc->to_vertex_;

  auto [src_dest_key, dest_src_key] = SerializeEdge(edge);
  if (!CheckRocksDBStatus(storage_->db_->Delete(rocksdb::WriteOptions(), storage_->edge_chandle, src_dest_key)) ||
      !CheckRocksDBStatus(storage_->db_->Delete(rocksdb::WriteOptions(), storage_->edge_chandle, dest_src_key))) {
    return Error::SERIALIZATION_ERROR;
  }

  return Result<std::unique_ptr<EdgeAccessor>>{
      std::make_unique<DiskEdgeAccessor>(edge_ref, edge_type, from_vertex, to_vertex, &transaction_,
                                         &storage_->indices_, &storage_->constraints_, config_, edge->Gid(), true)};
}

// this should be handled on an above level of abstraction
const std::string &DiskStorage::DiskAccessor::LabelToName(LabelId label) const { return storage_->LabelToName(label); }

// this should be handled on an above level of abstraction
const std::string &DiskStorage::DiskAccessor::PropertyToName(PropertyId property) const {
  return storage_->PropertyToName(property);
}

// this should be handled on an above level of abstraction
const std::string &DiskStorage::DiskAccessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return storage_->EdgeTypeToName(edge_type);
}

// this should be handled on an above level of abstraction
LabelId DiskStorage::DiskAccessor::NameToLabel(const std::string_view name) { return storage_->NameToLabel(name); }

// this should be handled on an above level of abstraction
PropertyId DiskStorage::DiskAccessor::NameToProperty(const std::string_view name) {
  return storage_->NameToProperty(name);
}

// this should be handled on an above level of abstraction
EdgeTypeId DiskStorage::DiskAccessor::NameToEdgeType(const std::string_view name) {
  return storage_->NameToEdgeType(name);
}

// this should be handled on an above level of abstraction
void DiskStorage::DiskAccessor::AdvanceCommand() { ++transaction_.command_id; }

void DiskStorage::DiskAccessor::FlushCache() {
  /// Flush vertex cache.
  auto vertex_acc = vertices_.access();
  uint64_t num_ser_edges = 0;
  std::for_each(vertex_acc.begin(), vertex_acc.end(), [this, &num_ser_edges](Vertex &vertex) {
    // if in the cache, serialize it.
    AssertRocksDBStatus(storage_->db_->Put(rocksdb::WriteOptions(), storage_->vertex_chandle, SerializeVertex(vertex),
                                           SerializeProperties(vertex.properties)));
    std::for_each(vertex.out_edges.begin(), vertex.out_edges.end(),
                  [this, &vertex, &num_ser_edges](std::tuple<EdgeTypeId, Vertex *, EdgeRef> &edge_entry) {
                    Edge *edge_ptr = std::get<2>(edge_entry).ptr;
                    auto [src_dest_key, dest_src_key] =
                        SerializeEdge(vertex.gid, std::get<1>(edge_entry)->gid, std::get<0>(edge_entry), edge_ptr);
                    AssertRocksDBStatus(storage_->db_->Put(rocksdb::WriteOptions(), storage_->edge_chandle,
                                                           src_dest_key, SerializeProperties(edge_ptr->properties)));
                    AssertRocksDBStatus(storage_->db_->Put(rocksdb::WriteOptions(), storage_->edge_chandle,
                                                           dest_src_key, SerializeProperties(edge_ptr->properties)));
                    num_ser_edges++;
                  });
  });
  vertices_.clear();
  MG_ASSERT(vertices_.size() == 0, "The vertex cache must be empty after flushing!");
  MG_ASSERT(num_ser_edges == edges_.size(),
            "The number of serialized edges must match the number of edges in the cache!");
  edges_.clear();
  MG_ASSERT(edges_.size() == 0, "The edge cache must be empty after flushing!");
}

// this will be modified here for a disk-based storage
utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto could_replicate_all_sync_replicas = true;

  if (transaction_.deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    storage_->commit_log_->MarkFinished(transaction_.start_timestamp);
  } else {
    // Validate that existence constraints are satisfied for all modified
    // vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      auto validation_result = ValidateExistenceConstraints(*prev.vertex, storage_->constraints_);
      if (validation_result) {
        Abort();
        return StorageDataManipulationError{*validation_result};
      }
    }

    // Result of validating the vertex against unqiue constraints. It has to be
    // declared outside of the critical section scope because its value is
    // tested for Abort call which has to be done out of the scope.
    std::optional<ConstraintViolation> unique_constraint_violation;

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp_.emplace(storage_->CommitTimestamp(desired_commit_timestamp));

      // Before committing and validating vertices against unique constraints,
      // we have to update unique constraints with the vertices that are going
      // to be validated/committed.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }
        storage_->constraints_.unique_constraints.UpdateBeforeCommit(prev.vertex, transaction_);
      }

      // Validate that unique constraints are satisfied for all modified
      // vertices.
      for (const auto &delta : transaction_.deltas) {
        auto prev = delta.prev.Get();
        MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
        if (prev.type != PreviousPtr::Type::VERTEX) {
          continue;
        }

        // No need to take any locks here because we modified this vertex and no
        // one else can touch it until we commit.
        unique_constraint_violation =
            storage_->constraints_.unique_constraints.Validate(*prev.vertex, transaction_, *commit_timestamp_);
        if (unique_constraint_violation) {
          break;
        }
      }

      if (!unique_constraint_violation) {
        // Write transaction to WAL while holding the engine lock to make sure
        // that committed transactions are sorted by the commit timestamp in the
        // WAL files. We supply the new commit timestamp to the function so that
        // it knows what will be the final commit timestamp. The WAL must be
        // written before actually committing the transaction (before setting
        // the commit timestamp) so that no other transaction can see the
        // commits before they are written to disk.
        // Replica can log only the write transaction received from Main
        // so the Wal files are consistent
        // if (storage_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
        if (desired_commit_timestamp.has_value()) {
          could_replicate_all_sync_replicas = storage_->AppendToWalDataManipulation(transaction_, *commit_timestamp_);
        }

        // Take committed_transactions lock while holding the engine lock to
        // make sure that committed transactions are sorted by the commit
        // timestamp in the list.
        storage_->committed_transactions_.WithLock([&](auto &committed_transactions) {
          // TODO: release lock, and update all deltas to have a local copy
          // of the commit timestamp
          MG_ASSERT(transaction_.commit_timestamp != nullptr, "Invalid database state!");
          transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);
          // Replica can only update the last commit timestamp with
          // the commits received from main.
          // if (storage_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
          if (desired_commit_timestamp.has_value()) {
            // Update the last commit timestamp
            storage_->last_commit_timestamp_.store(*commit_timestamp_);
          }
          // Release engine lock because we don't have to hold it anymore
          // and emplace back could take a long time.
          engine_guard.unlock();
        });

        storage_->commit_log_->MarkFinished(start_timestamp);
      }
      // Write cache to the disk
      FlushCache();
    }

    if (unique_constraint_violation) {
      Abort();
      return StorageDataManipulationError{*unique_constraint_violation};
    }
  }
  is_transaction_active_ = false;

  if (!could_replicate_all_sync_replicas) {
    return StorageDataManipulationError{ReplicationError{}};
  }

  return {};
}

/// TODO(andi): I think we should have one Abort method per storage.
void DiskStorage::DiskAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");

  // We collect vertices and edges we've created here and then splice them into
  // `deleted_vertices_` and `deleted_edges_` lists, instead of adding them one
  // by one and acquiring lock every time.
  std::list<Gid> my_deleted_vertices;
  std::list<Gid> my_deleted_edges;

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto vertex = prev.vertex;
        std::lock_guard<utils::SpinLock> guard(vertex->lock);
        Delta *current = vertex->delta;
        while (current != nullptr && current->timestamp->load(std::memory_order_acquire) ==
                                         transaction_.transaction_id.load(std::memory_order_acquire)) {
          switch (current->action) {
            case Delta::Action::REMOVE_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it != vertex->labels.end(), "Invalid database state!");
              std::swap(*it, *vertex->labels.rbegin());
              vertex->labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it == vertex->labels.end(), "Invalid database state!");
              vertex->labels.push_back(current->label);
              break;
            }
            case Delta::Action::SET_PROPERTY: {
              vertex->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::ADD_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it == vertex->in_edges.end(), "Invalid database state!");
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it == vertex->out_edges.end(), "Invalid database state!");
              vertex->out_edges.push_back(link);
              // Increment edge count. We only increment the count here because
              // the information in `ADD_IN_EDGE` and `Edge/RECREATE_OBJECT` is
              // redundant. Also, `Edge/RECREATE_OBJECT` isn't available when
              // edge properties are disabled.
              storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it != vertex->in_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->in_edges.rbegin());
              vertex->in_edges.pop_back();
              break;
            }
            case Delta::Action::REMOVE_OUT_EDGE: {
              std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{current->vertex_edge.edge_type,
                                                             current->vertex_edge.vertex, current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it != vertex->out_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->out_edges.rbegin());
              vertex->out_edges.pop_back();
              // Decrement edge count. We only decrement the count here because
              // the information in `REMOVE_IN_EDGE` and `Edge/DELETE_OBJECT` is
              // redundant. Also, `Edge/DELETE_OBJECT` isn't available when edge
              // properties are disabled.
              storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              vertex->deleted = true;
              my_deleted_vertices.push_back(vertex->gid);
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              vertex->deleted = false;
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
        }
        vertex->delta = current;
        if (current != nullptr) {
          current->prev.Set(vertex);
        }

        break;
      }
      case PreviousPtr::Type::EDGE: {
        auto edge = prev.edge;
        std::lock_guard<utils::SpinLock> guard(edge->lock);
        Delta *current = edge->delta;
        while (current != nullptr && current->timestamp->load(std::memory_order_acquire) ==
                                         transaction_.transaction_id.load(std::memory_order_acquire)) {
          switch (current->action) {
            case Delta::Action::SET_PROPERTY: {
              edge->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              edge->deleted = true;
              my_deleted_edges.push_back(edge->gid);
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              edge->deleted = false;
              break;
            }
            case Delta::Action::REMOVE_LABEL:
            case Delta::Action::ADD_LABEL:
            case Delta::Action::ADD_IN_EDGE:
            case Delta::Action::ADD_OUT_EDGE:
            case Delta::Action::REMOVE_IN_EDGE:
            case Delta::Action::REMOVE_OUT_EDGE: {
              LOG_FATAL("Invalid database state!");
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
        }
        edge->delta = current;
        if (current != nullptr) {
          current->prev.Set(edge);
        }

        break;
      }
      case PreviousPtr::Type::DELTA:
      // pointer probably couldn't be set because allocation failed
      case PreviousPtr::Type::NULLPTR:
        break;
    }
  }

  {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    uint64_t mark_timestamp = storage_->timestamp_;
    // Take garbage_undo_buffers lock while holding the engine lock to make
    // sure that entries are sorted by mark timestamp in the list.
    storage_->garbage_undo_buffers_.WithLock([&](auto &garbage_undo_buffers) {
      // Release engine lock because we don't have to hold it anymore and
      // emplace back could take a long time.
      engine_guard.unlock();
      garbage_undo_buffers.emplace_back(mark_timestamp, std::move(transaction_.deltas));
    });
    storage_->deleted_vertices_.WithLock(
        [&](auto &deleted_vertices) { deleted_vertices.splice(deleted_vertices.begin(), my_deleted_vertices); });
    storage_->deleted_edges_.WithLock(
        [&](auto &deleted_edges) { deleted_edges.splice(deleted_edges.begin(), my_deleted_edges); });
  }

  storage_->commit_log_->MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
}

// maybe will need some usages here
void DiskStorage::DiskAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    storage_->commit_log_->MarkFinished(*commit_timestamp_);
    storage_->committed_transactions_.WithLock(
        [&](auto &committed_transactions) { committed_transactions.emplace_back(std::move(transaction_)); });
    commit_timestamp_.reset();
  }
}

// this should be handled on an above level of abstraction
std::optional<uint64_t> DiskStorage::DiskAccessor::GetTransactionId() const {
  throw utils::NotYetImplemented("GetTransactionId");
}

// this should be handled on an above level of abstraction
const std::string &DiskStorage::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

// this should be handled on an above level of abstraction
const std::string &DiskStorage::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

// this should be handled on an above level of abstraction
const std::string &DiskStorage::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

// this should be handled on an above level of abstraction
LabelId DiskStorage::NameToLabel(const std::string_view name) {
  return LabelId::FromUint(name_id_mapper_.NameToId(name));
}

// this should be handled on an above level of abstraction
PropertyId DiskStorage::NameToProperty(const std::string_view name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

// this should be handled on an above level of abstraction
EdgeTypeId DiskStorage::NameToEdgeType(const std::string_view name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateIndex");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateIndex");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropIndex");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropIndex");
}

// this should be handled on an above level of abstraction
IndicesInfo DiskStorage::ListAllIndices() const { throw utils::NotYetImplemented("ListAllIndices"); }

// this should be handled on an above level of abstraction
utils::BasicResult<StorageExistenceConstraintDefinitionError, void> DiskStorage::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateExistenceConstraint");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageExistenceConstraintDroppingError, void> DiskStorage::DropExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropExistenceConstraint");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
DiskStorage::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                    const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateUniqueConstraint");
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
DiskStorage::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                  const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropUniqueConstraint");
}

// this should be handled on an above level of abstraction
ConstraintsInfo DiskStorage::ListAllConstraints() const { throw utils::NotYetImplemented("ListAllConstraints"); }

// this should be handled on an above level of abstraction
StorageInfo DiskStorage::GetInfo() const { throw utils::NotYetImplemented("GetInfo"); }

/// TODO(andi): When we add support for replication, we should have one CreateTransaction method per
/// storage.
Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level) {
  /// We acquire the transaction engine lock here because we access (and
  /// modify) the transaction engine variables (`transaction_id` and
  /// `timestamp`) below.
  uint64_t transaction_id;
  uint64_t start_timestamp;
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    transaction_id = transaction_id_++;
    /// TODO: when we introduce replication to the disk storage, take care of start_timestamp
    start_timestamp = timestamp_++;
  }
  return {transaction_id, start_timestamp, isolation_level};
}

template <bool force>
void DiskStorage::CollectGarbage() {
  throw utils::NotYetImplemented("CollectGarbage");
}

// tell the linker he can find the CollectGarbage definitions here
template void DiskStorage::CollectGarbage<true>();
template void DiskStorage::CollectGarbage<false>();

bool DiskStorage::InitializeWalFile() {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, epoch_id_, config_.items, &name_id_mapper_, wal_seq_num_++,
                      &file_retainer_);
  }
  return true;
}

void DiskStorage::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >= config_.durability.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >= config_.durability.wal_file_size_kibibytes) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
    wal_unsynced_transactions_ = 0;
  } else {
    // Try writing the internal buffer if possible, if not
    // the data should be written as soon as it's possible
    // (triggered by the new transaction commit, or some
    // reading thread EnabledFlushing)
    wal_file_->TryFlushing();
  }
}

bool DiskStorage::AppendToWalDataManipulation(const Transaction &transaction, uint64_t final_commit_timestamp) {
  throw utils::NotYetImplemented("AppendToWalDataManipulation");
}

bool DiskStorage::AppendToWalDataDefinition(durability::StorageGlobalOperation operation, LabelId label,
                                            const std::set<PropertyId> &properties, uint64_t final_commit_timestamp) {
  throw utils::NotYetImplemented("AppendToWalDataDefinition");
}

utils::BasicResult<DiskStorage::CreateSnapshotError> DiskStorage::CreateSnapshot() {
  throw utils::NotYetImplemented("CreateSnapshot");
}

bool DiskStorage::LockPath() { throw utils::NotYetImplemented("LockPath"); }

bool DiskStorage::UnlockPath() { throw utils::NotYetImplemented("UnlockPath"); }

void DiskStorage::FreeMemory() { throw utils::NotYetImplemented("FreeMemory"); }

uint64_t DiskStorage::CommitTimestamp(const std::optional<uint64_t> desired_commit_timestamp) {
  if (!desired_commit_timestamp) {
    return timestamp_++;
  }
  timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
  return *desired_commit_timestamp;
}

bool DiskStorage::SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config) {
  throw utils::NotYetImplemented("SetReplicaRole");
}

bool DiskStorage::SetMainReplicationRole() { throw utils::NotYetImplemented("SetMainReplicationRole"); }

utils::BasicResult<DiskStorage::RegisterReplicaError> DiskStorage::RegisterReplica(
    std::string name, io::network::Endpoint endpoint, const replication::ReplicationMode replication_mode,
    const replication::RegistrationMode registration_mode, const replication::ReplicationClientConfig &config) {
  throw utils::NotYetImplemented("RegisterReplica");
}

bool DiskStorage::UnregisterReplica(const std::string &name) { throw utils::NotYetImplemented("UnregisterReplica"); }

std::optional<replication::ReplicaState> DiskStorage::GetReplicaState(const std::string_view name) {
  throw utils::NotYetImplemented("GetReplicaState");
}

ReplicationRole DiskStorage::GetReplicationRole() const { throw utils::NotYetImplemented("GetReplicationRole"); }

std::vector<DiskStorage::ReplicaInfo> DiskStorage::ReplicasInfo() { throw utils::NotYetImplemented("ReplicasInfo"); }

void DiskStorage::SetIsolationLevel(IsolationLevel isolation_level) {
  std::unique_lock main_guard{main_lock_};
  isolation_level_ = isolation_level;
}

void DiskStorage::RestoreReplicas() { throw utils::NotYetImplemented("RestoreReplicas"); }

bool DiskStorage::ShouldStoreAndRestoreReplicas() const {
  throw utils::NotYetImplemented("ShouldStoreAndRestoreReplicas");
}

}  // namespace memgraph::storage
