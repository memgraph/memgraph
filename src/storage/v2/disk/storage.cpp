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

#include <limits>
#include <stdexcept>

#include <rocksdb/slice.h>

#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/rocksdb_serialization.hpp"
#include "utils/skip_list.hpp"
#include "utils/stat.hpp"
#include "utils/string.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

constexpr const char *vertexHandle = "vertex";
constexpr const char *edgeHandle = "edge";
constexpr const char *defaultHandle = "default";

bool VertexExistsInCache(const utils::SkipList<Vertex>::Accessor &accessor, Gid gid) {
  return accessor.find(gid) != accessor.end();
}

bool VertexHasLabel(const Vertex &vertex, LabelId label, Transaction *transaction, View view) {
  bool deleted = false;
  bool has_label = false;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex.lock);
    deleted = vertex.deleted;
    has_label = std::find(vertex.labels.begin(), vertex.labels.end(), label) != vertex.labels.end();
    delta = vertex.delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&deleted, &has_label, label](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta.label == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  return has_label && !deleted;
}

PropertyValue GetVertexProperty(const Vertex &vertex, PropertyId property, Transaction *transaction, View view) {
  bool deleted = false;
  PropertyValue value;
  Delta *delta = nullptr;
  {
    std::lock_guard<utils::SpinLock> guard(vertex.lock);
    deleted = vertex.deleted;
    value = vertex.properties.GetProperty(property);
    delta = vertex.delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&deleted, &value, property](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta.property.key == property) {
          value = delta.property.value;
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
    }
  });
  if (deleted) {
    return {};
  }
  return value;
}

bool HasVertexProperty(const Vertex &vertex, PropertyId property, Transaction *transaction, View view) {
  return !GetVertexProperty(vertex, property, transaction, view).IsNull();
}

bool HasVertexEqualPropertyValue(const Vertex &vertex, PropertyId property_id, PropertyValue property_value,
                                 Transaction *transaction, View view) {
  return GetVertexProperty(vertex, property_id, transaction, view) == property_value;
}

bool IsPropertyValueWithinInterval(const PropertyValue &value,
                                   const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                   const std::optional<utils::Bound<PropertyValue>> &upper_bound) {
  if (lower_bound && (!PropertyValue::AreComparableTypes(value.type(), lower_bound->value().type()) ||
                      value < lower_bound->value() || (lower_bound->IsExclusive() && value == lower_bound->value()))) {
    return false;
  }
  if (upper_bound && (!PropertyValue::AreComparableTypes(value.type(), upper_bound->value().type()) ||
                      value > upper_bound->value() || (upper_bound->IsExclusive() && value == upper_bound->value()))) {
    return false;
  }
  return true;
}

}  // namespace

/// TODO: remove constraints from Storage
DiskStorage::DiskStorage(Config config) : Storage(config, StorageMode::ON_DISK_TRANSACTIONAL) {
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
      if (auto maybe_error = this->CreateSnapshot({true}); maybe_error.HasError()) {
        switch (maybe_error.GetError()) {
          case CreateSnapshotError::DisabledForReplica:
            spdlog::warn(
                utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
            break;
          case CreateSnapshotError::DisabledForAnalyticsPeriodicCommit:
            spdlog::warn(utils::MessageWithLink("Periodic snapshots are disabled for analytical mode.",
                                                "https://memgr.ph/durability"));
            break;
          case storage::Storage::CreateSnapshotError::ReachedMaxNumTries:
            spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
            break;
        }
      }
    });
  }
  if (config_.gc.type == Config::Gc::Type::PERIODIC) {
    // We remove data on each tx commit. Now we don't need GC.
    // gc_runner_.Run("Storage GC", config_.gc.interval, [this] { this->CollectGarbage<false>(); });
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
  //       std::make_unique<kvstore_::kvstore_>(config_.durability.storage_directory /
  //       durability::kReplicationDirectory);
  //   RestoreReplicas();
  // } else {
  //   spdlog::warn("Replicas' configuration will NOT be stored. When the server restarts, replicas will be
  //   forgotten.");
  // }

  kvstore_ = std::make_unique<RocksDBStorage>();
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  std::vector<rocksdb::ColumnFamilyHandle *> column_handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  if (utils::DirExists(config.disk.main_storage_directory)) {
    column_families.emplace_back(vertexHandle, kvstore_->options_);
    column_families.emplace_back(edgeHandle, kvstore_->options_);
    column_families.emplace_back(defaultHandle, kvstore_->options_);
    logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                              config.disk.main_storage_directory, column_families,
                                                              &column_handles, &kvstore_->db_));
    kvstore_->vertex_chandle = column_handles[0];
    kvstore_->edge_chandle = column_handles[1];
    kvstore_->default_chandle = column_handles[2];
  } else {
    logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                              config.disk.main_storage_directory, &kvstore_->db_));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, vertexHandle, &kvstore_->vertex_chandle));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, edgeHandle, &kvstore_->edge_chandle));
  }
}

DiskStorage::~DiskStorage() {
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->vertex_chandle));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->edge_chandle));
  if (kvstore_->default_chandle) {
    // We must destroy default column family handle only if it was read from existing database.
    // https://github.com/facebook/rocksdb/issues/5006#issuecomment-1003154821
    logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->default_chandle));
  }
  delete kvstore_->options_.comparator;
  kvstore_->options_.comparator = nullptr;
  spdlog::debug("Tracker when destroying storage: {}", utils::GetReadableSize(utils::total_memory_tracker.Amount()));
}

DiskStorage::DiskAccessor::DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : Accessor(storage, isolation_level, storage_mode), config_(storage->config_.items) {
  spdlog::debug("Tracker size when creating accessor: {}",
                utils::GetReadableSize(utils::total_memory_tracker.Amount()));
  spdlog::debug("Process size when creating accessor {}", utils::GetReadableSize(utils::GetMemoryUsage()));
  rocksdb::WriteOptions write_options;
  // auto strTs = utils::StringTimestamp(transaction_.start_timestamp);
  // rocksdb::Slice ts(strTs);
  // write_options.timestamp = &ts;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  disk_transaction_ = storage->kvstore_->db_->BeginTransaction(write_options, txOptions);
  disk_transaction_->SetReadTimestampForValidation(transaction_.start_timestamp);
}

DiskStorage::DiskAccessor::DiskAccessor(DiskAccessor &&other) noexcept
    : Accessor(std::move(other)), config_(other.config_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  spdlog::debug("Tracker size when creating accessor: {}",
                utils::GetReadableSize(utils::total_memory_tracker.Amount()));
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

DiskStorage::DiskAccessor::~DiskAccessor() {
  if (is_transaction_active_) {
    Abort();
  }

  FinalizeTransaction();
  spdlog::debug("Tracker when destroying accessor: {}", utils::GetReadableSize(utils::total_memory_tracker.Amount()));
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToMainMemoryCache(
    const rocksdb::Slice &key, const rocksdb::Slice &value) {
  auto main_storage_accessor = vertices_.access();

  const std::string key_str = key.ToString();
  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromKey(key_str)));
  if (VertexExistsInCache(main_storage_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id = utils::DeserializeLabelsFromMainDiskStorage(key_str);
  return CreateVertex(main_storage_accessor, gid, labels_id,
                      utils::DeserializePropertiesFromMainDiskStorage(value.ToStringView()),
                      CreateDeleteDeserializedObjectDelta(&transaction_));
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToLabelIndexCache(
    const rocksdb::Slice &key, const rocksdb::Slice &value) {
  auto index_accessor = indexed_vertices_.access();

  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelIndexStorage(key.ToString())));
  if (VertexExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }

  const std::string value_str = value.ToString();
  std::vector<LabelId> labels = utils::DeserializeLabelsFromLabelIndexStorage(value_str);
  return CreateVertex(index_accessor, gid, labels, utils::DeserializePropertiesFromLabelIndexStorage(value_str),
                      CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas_));
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToLabelPropertyIndexCache(
    const rocksdb::Slice &key, const rocksdb::Slice &value) {
  auto index_accessor = indexed_vertices_.access();

  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key.ToString())));
  if (VertexExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }

  const std::string value_str = value.ToString();
  std::vector<LabelId> labels = utils::DeserializeLabelsFromLabelPropertyIndexStorage(value_str);
  return CreateVertex(index_accessor, gid, labels,
                      utils::DeserializePropertiesFromLabelPropertyIndexStorage(value.ToString()),
                      CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas_));
}

std::optional<EdgeAccessor> DiskStorage::DiskAccessor::DeserializeEdge(const rocksdb::Slice &key,
                                                                       const rocksdb::Slice &value) {
  const auto edge_parts = utils::Split(key.ToStringView(), "|");
  const Gid edge_gid = Gid::FromUint(std::stoull(edge_parts[4]));

  auto edge_acc = edges_.access();
  auto res = edge_acc.find(edge_gid);
  if (res != edge_acc.end()) {
    // spdlog::debug("Edge with gid {} already exists in the cache!", edge_parts[4]);
    return std::nullopt;
  }
  // spdlog::debug("Edge with gid {} doesn't exist in the cache!", edge_parts[4]);

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
  auto from_acc = FindVertex(Gid::FromUint(std::stoull(from_gid)), View::OLD);
  auto to_acc = FindVertex(Gid::FromUint(std::stoull(to_gid)), View::OLD);
  if (!from_acc || !to_acc) {
    throw utils::BasicException("Non-existing vertices found during edge deserialization");
  }
  const auto edge_type_id = storage::EdgeTypeId::FromUint(std::stoull(edge_parts[3]));
  auto maybe_edge = CreateEdge(&*from_acc, &*to_acc, edge_type_id, edge_gid, value.ToStringView());
  MG_ASSERT(maybe_edge.HasValue());

  return *maybe_edge;
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it =
      std::unique_ptr<rocksdb::Iterator>(disk_transaction_->GetIterator(ro, disk_storage->kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    LoadVertexToMainMemoryCache(it->key(), it->value());
  }
  return VerticesIterable(AllVerticesIterable(vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, View view) {
  auto *disk_label_index = static_cast<DiskLabelIndex *>(storage_->indices_.label_index_.get());
  auto disk_index_transaction = disk_label_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  spdlog::debug("Reading from main disk transaction");
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids(main_cache_acc.size());
  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    if (VertexHasLabel(vertex, label, &transaction_, view)) {
      spdlog::debug("Loaded vertex with gid {} from main cache to index cache", utils::SerializeIdType(vertex.gid));
      LoadVertexToLabelIndexCache(utils::SerializeVertexAsKeyForLabelIndex(label, vertex.gid),
                                  utils::SerializeVertexAsValueForLabelIndex(label, vertex.labels, vertex.properties));
    }
  }

  spdlog::debug("Reading from label index");
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelIndexStorage(key)));
    // TODO: andi this will be optimized bla bla
    if (key.starts_with(utils::SerializeIdType(label)) && !utils::Contains(gids, curr_gid)) {
      spdlog::debug("Loaded vertex with gid {} from disk label index", utils::ExtractGidFromLabelIndexStorage(key));
      /// TODO: against every law of programming
      LoadVertexToLabelIndexCache(index_it->key(), index_it->value());
    }
  }

  return VerticesIterable(AllVerticesIterable(indexed_vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, View view) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  spdlog::debug("Reading from main disk transaction");
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids(main_cache_acc.size());
  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    /// TODO: refactor in one method
    if (VertexHasLabel(vertex, label, &transaction_, view) &&
        HasVertexProperty(vertex, property, &transaction_, view)) {
      spdlog::debug("Loaded vertex with gid {} from main cache to label-property index cache",
                    utils::SerializeIdType(vertex.gid));
      LoadVertexToLabelPropertyIndexCache(
          utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties));
    }
  }

  spdlog::debug("Reading from label-property index");
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key)));
    // TODO: andi this will be optimized bla bla
    /// TODO: couple in one method
    if (key.starts_with(utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property)) &&
        !utils::Contains(gids, curr_gid)) {
      spdlog::debug("Loaded vertex with gid {} from disk label index",
                    utils::ExtractGidFromLabelPropertyIndexStorage(key));
      /// TODO: against every law of programming
      LoadVertexToLabelPropertyIndexCache(index_it->key(), index_it->value());
    }
  }

  return VerticesIterable(AllVerticesIterable(indexed_vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value,
                                                     View view) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  spdlog::debug("Reading from main disk transaction");
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids(main_cache_acc.size());
  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    /// TODO: refactor in one method
    if (VertexHasLabel(vertex, label, &transaction_, view) &&
        HasVertexEqualPropertyValue(vertex, property, value, &transaction_, view)) {
      spdlog::debug("Loaded vertex with gid {} from main cache to label-property index cache",
                    utils::SerializeIdType(vertex.gid));
      LoadVertexToLabelPropertyIndexCache(
          utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties));
    }
  }

  spdlog::debug("Reading from label-property index");
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key_str = index_it->key().ToString();
    std::string it_value_str = index_it->value().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key_str)));
    // TODO: andi this will be optimized bla bla
    /// TODO: couple this condition
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value_str);
    if (key_str.starts_with(utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property)) &&
        !utils::Contains(gids, curr_gid) && properties.IsPropertyEqual(property, value)) {
      spdlog::debug("Loaded vertex with gid {} from disk label index",
                    utils::ExtractGidFromLabelPropertyIndexStorage(key_str));
      /// TODO: against every law of programming
      LoadVertexToLabelPropertyIndexCache(index_it->key(), index_it->value());
    }
  }

  return VerticesIterable(AllVerticesIterable(indexed_vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property,
                                                     const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                     const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                     View view) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  spdlog::debug("Reading from main disk transaction");
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids(main_cache_acc.size());
  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    /// TODO: refactor in one method
    auto prop_value = GetVertexProperty(vertex, property, &transaction_, view);
    if (VertexHasLabel(vertex, label, &transaction_, view) &&
        IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      spdlog::debug("Loaded vertex with gid {} from main cache to label-property index cache",
                    utils::SerializeIdType(vertex.gid));
      LoadVertexToLabelPropertyIndexCache(
          utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties));
    }
  }

  spdlog::debug("Reading from label-property index");
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key_str = index_it->key().ToString();
    std::string it_value_str = index_it->value().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key_str)));
    // TODO: andi this will be optimized bla bla
    /// TODO: couple this condition
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value_str);
    auto prop_value = properties.GetProperty(property);
    if (key_str.starts_with(utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property)) &&
        !utils::Contains(gids, curr_gid) && IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      spdlog::debug("Loaded vertex with gid {} from disk label index",
                    utils::ExtractGidFromLabelPropertyIndexStorage(key_str));
      /// TODO: against every law of programming
      LoadVertexToLabelPropertyIndexCache(index_it->key(), index_it->value());
    }
  }

  return VerticesIterable(AllVerticesIterable(indexed_vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

uint64_t DiskStorage::DiskAccessor::ApproximateVertexCount() const {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto estimatedCount = disk_storage->kvstore_->ApproximateVertexCount();
  if (estimatedCount == 0) {
    estimatedCount = vertices_.size();
  }
  return estimatedCount;
}

VertexAccessor DiskStorage::DiskAccessor::CreateVertex() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto gid = disk_storage->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = vertices_.access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");

  if (delta) {
    delta->prev.Set(&*it);
  }

  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

StorageInfo DiskStorage::GetInfo() const {
  auto vertex_count = kvstore_->ApproximateVertexCount();
  auto edge_count = edge_count_.load(std::memory_order_acquire);
  double average_degree = 0.0;
  if (vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    average_degree = 2.0 * static_cast<double>(edge_count) / vertex_count;
  }
  return {vertex_count, edge_count, average_degree, utils::GetMemoryUsage(),
          utils::GetDirDiskUsage(config_.durability.storage_directory)};
}

VertexAccessor DiskStorage::DiskAccessor::CreateVertex(storage::Gid gid) {
  OOMExceptionEnabler oom_exception;
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  // spdlog::debug("Vertex with gid {} doesn't exist in the cache, creating it!", gid.AsUint());
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto acc = vertices_.access();
  disk_storage->vertex_id_.store(std::max(disk_storage->vertex_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                                 std::memory_order_release);

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  /// if the vertex with the given gid doesn't exist on the disk, it must be inserted here.
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  if (delta) {
    delta->prev.Set(&*it);
  }
  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

VertexAccessor DiskStorage::DiskAccessor::CreateVertex(utils::SkipList<Vertex>::Accessor &accessor, storage::Gid gid,
                                                       const std::vector<LabelId> &label_ids,
                                                       PropertyStore &&properties, Delta *delta) {
  OOMExceptionEnabler oom_exception;
  // NOTE: When we update the next `vertex_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  disk_storage->vertex_id_.store(std::max(disk_storage->vertex_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                                 std::memory_order_release);
  auto [it, inserted] = accessor.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != accessor.end(), "Invalid Vertex accessor!");
  /// TODO: move
  for (auto label_id : label_ids) {
    it->labels.push_back(label_id);
  }
  it->properties = std::move(properties);
  if (delta) {
    delta->prev.Set(&*it);
  }
  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

std::optional<VertexAccessor> DiskStorage::DiskAccessor::FindVertex(storage::Gid gid, View view) {
  /// Check if the vertex is in the cache.
  auto acc = vertices_.access();
  auto vertex_it = acc.find(gid);
  if (vertex_it != acc.end()) {
    return VertexAccessor::Create(&*vertex_it, &transaction_, &storage_->indices_, &storage_->constraints_, config_,
                                  view);
  }
  rocksdb::ReadOptions read_opts;
  auto strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  read_opts.timestamp = &ts;
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto it = std::unique_ptr<rocksdb::Iterator>(
      disk_transaction_->GetIterator(read_opts, disk_storage->kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto &key = it->key();
    if (Gid::FromUint(std::stoull(utils::ExtractGidFromKey(key.ToString()))) == gid) {
      return LoadVertexToMainMemoryCache(key, it->value());
    }
  }
  return std::nullopt;
}

Result<std::optional<VertexAccessor>> DiskStorage::DiskAccessor::DeleteVertex(VertexAccessor *vertex) {
  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  if (vertex_ptr->deleted) {
    return std::optional<VertexAccessor>{};
  }

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty()) return Error::VERTEX_HAS_EDGES;

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;
  vertices_to_delete_.emplace_back(utils::SerializeIdType(vertex_ptr->gid), utils::SerializeVertex(*vertex_ptr));

  return std::make_optional<VertexAccessor>(vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_,
                                            config_, true);
}

Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
DiskStorage::DiskAccessor::DetachDeleteVertex(VertexAccessor *vertex) {
  using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;
  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  {
    std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

    if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

    if (vertex_ptr->deleted) return std::optional<ReturnType>{};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  std::vector<EdgeAccessor> deleted_edges;
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, from_vertex, vertex_ptr, &transaction_, &storage_->indices_,
                   &storage_->constraints_, config_);
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, vertex_ptr, to_vertex, &transaction_, &storage_->indices_, &storage_->constraints_,
                   config_);
    auto ret = DeleteEdge(&e);
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  // We need to check again for serialization errors because we unlocked the
  // vertex. Some other transaction could have modified the vertex in the
  // meantime if we didn't have any edges to delete.

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;
  vertices_to_delete_.emplace_back(utils::SerializeIdType(vertex_ptr->gid), utils::SerializeVertex(*vertex_ptr));

  return std::make_optional<ReturnType>(
      VertexAccessor{vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_, config_, true},
      std::move(deleted_edges));
}

void DiskStorage::DiskAccessor::PrefetchEdges(const auto &prefetch_edge_filter) {
  rocksdb::ReadOptions read_opts;
  auto strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  read_opts.timestamp = &ts;
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto it = std::unique_ptr<rocksdb::Iterator>(
      disk_transaction_->GetIterator(read_opts, disk_storage->kvstore_->edge_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const rocksdb::Slice &key = it->key();
    const auto edge_parts = utils::Split(key.ToStringView(), "|");
    if (prefetch_edge_filter(edge_parts[0], edge_parts[2])) {
      DeserializeEdge(key, it->value());
    }
  }
}

void DiskStorage::DiskAccessor::PrefetchInEdges(const VertexAccessor &vertex_acc) {
  PrefetchEdges([&vertex_acc](const std::string_view disk_edge_gid,
                              const std::string_view disk_edge_direction) -> bool {
    return disk_edge_gid == utils::SerializeIdType(vertex_acc.Gid()) && disk_edge_direction == utils::inEdgeDirection;
  });
}

void DiskStorage::DiskAccessor::PrefetchOutEdges(const VertexAccessor &vertex_acc) {
  PrefetchEdges([&vertex_acc](const std::string_view disk_edge_gid,
                              const std::string_view disk_edge_direction) -> bool {
    return disk_edge_gid == utils::SerializeIdType(vertex_acc.Gid()) && disk_edge_direction == utils::outEdgeDirection;
  });
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                           EdgeTypeId edge_type, storage::Gid gid,
                                                           std::string_view properties) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto from_vertex = from->vertex_;
  auto to_vertex = to->vertex_;

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
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  disk_storage->edge_id_.store(std::max(disk_storage->edge_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                               std::memory_order_release);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto *delta = CreateDeleteDeserializedObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    if (delta) {
      delta->prev.Set(&*it);
    }
    edge.ptr->properties.SetBuffer(properties);
  }

  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                      &storage_->constraints_, config_);
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                           EdgeTypeId edge_type, storage::Gid gid) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto from_vertex = from->vertex_;
  auto to_vertex = to->vertex_;

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
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  disk_storage->edge_id_.store(std::max(disk_storage->edge_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                               std::memory_order_release);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                      &storage_->constraints_, config_);
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                           EdgeTypeId edge_type) {
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  MG_ASSERT(to->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");

  auto from_vertex = from->vertex_;
  auto to_vertex = to->vertex_;

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

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto gid = storage::Gid::FromUint(disk_storage->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  // Increment edge count.
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                      &storage_->constraints_, config_);
}

Result<std::optional<EdgeAccessor>> DiskStorage::DiskAccessor::DeleteEdge(EdgeAccessor *edge) {
  MG_ASSERT(edge->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when deleting an edge!");
  auto edge_ref = edge->edge_;
  auto edge_type = edge->edge_type_;

  std::unique_lock<utils::SpinLock> guard;
  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    guard = std::unique_lock<utils::SpinLock>(edge_ptr->lock);

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return std::optional<EdgeAccessor>{};
  }

  auto *from_vertex = edge->from_vertex_;
  auto *to_vertex = edge->to_vertex_;

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
  MG_ASSERT(!from_vertex->deleted, "Invalid database state!");

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    MG_ASSERT(!to_vertex->deleted, "Invalid database state!");
  }

  auto delete_edge_from_storage = [&edge_type, &edge_ref, this](auto *vertex, auto *edges) {
    std::tuple<EdgeTypeId, Vertex *, EdgeRef> link(edge_type, vertex, edge_ref);
    auto it = std::find(edges->begin(), edges->end(), link);
    if (config_.properties_on_edges) {
      MG_ASSERT(it != edges->end(), "Invalid database state!");
    } else if (it == edges->end()) {
      return false;
    }
    std::swap(*it, *edges->rbegin());
    edges->pop_back();
    return true;
  };

  auto op1 = delete_edge_from_storage(to_vertex, &from_vertex->out_edges);
  auto op2 = delete_edge_from_storage(from_vertex, &to_vertex->in_edges);
  auto [src_dest_del_key, dest_src_del_key] =
      utils::SerializeEdge(from_vertex->gid, to_vertex->gid, edge_type, edge_ref, config_.properties_on_edges);
  edges_to_delete_.emplace_back(src_dest_del_key);
  edges_to_delete_.emplace_back(dest_src_del_key);

  if (config_.properties_on_edges) {
    MG_ASSERT((op1 && op2), "Invalid database state!");
  } else {
    MG_ASSERT((op1 && op2) || (!op1 && !op2), "Invalid database state!");
    if (!op1 && !op2) {
      // The edge is already deleted.
      return std::optional<EdgeAccessor>{};
    }
  }

  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    CreateAndLinkDelta(&transaction_, edge_ptr, Delta::RecreateObjectTag());
    edge_ptr->deleted = true;
  }

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(), edge_type, to_vertex, edge_ref);
  CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type, from_vertex, edge_ref);

  // Decrement edge count.
  storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);

  return std::make_optional<EdgeAccessor>(edge_ref, edge_type, from_vertex, to_vertex, &transaction_,
                                          &storage_->indices_, &storage_->constraints_, config_, true);
}

/// TODO: at which storage naming
bool DiskStorage::DiskAccessor::WriteVertexToDisk(const Vertex &vertex) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto status = disk_transaction_->Put(disk_storage->kvstore_->vertex_chandle, utils::SerializeVertex(vertex),
                                       utils::SerializeProperties(vertex.properties));
  if (status.ok()) {
    spdlog::debug("rocksdb: Saved vertex with key {} and ts {}", utils::SerializeVertex(vertex), *commit_timestamp_);
  } else if (status.IsBusy()) {
    spdlog::error("rocksdb: Vertex with key {} and ts {} was changed and committed in another transaction",
                  utils::SerializeVertex(vertex), *commit_timestamp_);
    return false;
  } else {
    spdlog::error("rocksdb: Failed to save vertex with key {} and ts {}", utils::SerializeVertex(vertex),
                  *commit_timestamp_);
    return false;
  }
  return true;
}

/// TODO: at which storage naming
bool DiskStorage::DiskAccessor::WriteEdgeToDisk(const EdgeRef edge, const std::string &serializedEdgeKey) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  rocksdb::Status status;
  if (config_.properties_on_edges) {
    status = disk_transaction_->Put(disk_storage->kvstore_->edge_chandle, serializedEdgeKey,
                                    utils::SerializeProperties(edge.ptr->properties));
  } else {
    status = disk_transaction_->Put(disk_storage->kvstore_->edge_chandle, serializedEdgeKey, "");
  }
  if (status.ok()) {
    spdlog::debug("rocksdb: Saved edge with key {} and ts {}", serializedEdgeKey, *commit_timestamp_);
  } else if (status.IsBusy()) {
    spdlog::error("rocksdb: Edge with key {} and ts {} was changed and committed in another transaction",
                  serializedEdgeKey, *commit_timestamp_);
    return false;
  } else {
    spdlog::error("rocksdb: Failed to save edge with key {} and ts {}", serializedEdgeKey, *commit_timestamp_);
    return false;
  }
  return true;
}

bool DiskStorage::DiskAccessor::DeleteVertexFromDisk(const std::string &vertex) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto status = disk_transaction_->Delete(disk_storage->kvstore_->vertex_chandle, vertex);
  if (status.ok()) {
    spdlog::debug("rocksdb: Deleted vertex with key {}", vertex);
  } else if (status.IsBusy()) {
    spdlog::error("rocksdb: Vertex with key {} was changed and committed in another transaction", vertex);
    return false;
  } else {
    spdlog::error("rocksdb: Failed to delete vertex with key {}", vertex);
    return false;
  }
  return true;
}

bool DiskStorage::DiskAccessor::DeleteEdgeFromDisk(const std::string &edge) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto status = disk_transaction_->Delete(disk_storage->kvstore_->edge_chandle, edge);
  if (status.ok()) {
    spdlog::debug("rocksdb: Deleted edge with key {}", edge);
  } else if (status.IsBusy()) {
    spdlog::error("rocksdb: Edge with key {} was changed and committed in another transaction", edge);
    return false;
  } else {
    spdlog::error("rocksdb: Failed to delete edge with key {}", edge);
    return false;
  }
  return true;
}

/// TODO: add the comment about the storage lock you should take for correctly operating on this method
[[nodiscard]] utils::BasicResult<StorageDataManipulationError, void>
DiskStorage::DiskAccessor::CheckConstraintsAndFlushMainMemoryCache() {
  /// Flush vertex cache.
  auto vertex_acc = vertices_.access();
  uint64_t num_ser_edges = 0;

  /// TODO: andi process currently in the function to avoid iterating twice over changed vertices but needs refactoring
  std::vector<std::vector<PropertyValue>> unique_storage;
  auto *disk_unique_constraints =
      static_cast<DiskUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(storage_->indices_.label_index_.get());
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  /// TODO: andi I don't like that std::optional is used for checking errors but that's how it was before, refactor!
  for (Vertex &vertex : vertex_acc) {
    /// TODO: refactor this check, it is unreadable
    if (auto existence_constraint_validation_result = storage_->constraints_.existence_constraints_->Validate(vertex);
        existence_constraint_validation_result.has_value()) {
      return StorageDataManipulationError{existence_constraint_validation_result.value()};
    }
    if (auto unique_constraint_validation_result = disk_unique_constraints->Validate(vertex, unique_storage);
        unique_constraint_validation_result.has_value()) {
      return StorageDataManipulationError{unique_constraint_validation_result.value()};
    }

    if (vertex.deleted) {
      continue;
    }

    if (!WriteVertexToDisk(vertex)) {
      return StorageDataManipulationError{SerializationError{}};
    }

    std::stringstream ss;
    ss << "{";
    for (const auto &[property_id, property_value] : vertex.properties.Properties()) {
      ss << property_id.AsUint() << ": " << property_value;
    }
    ss << "}";

    spdlog::debug("Written vertex to main storage with key: {} and properties: {} in transaction: {}",
                  utils::SerializeVertex(vertex), ss.str(), transaction_.start_timestamp);

    /// TODO: andi don't ignore the return value
    disk_unique_constraints->SyncVertexToUniqueConstraintsStorage(vertex, *commit_timestamp_);
    disk_label_index->SyncVertexToLabelIndexStorage(vertex, *commit_timestamp_);
    disk_label_property_index->SyncVertexToLabelPropertyIndexStorage(vertex, *commit_timestamp_);

    spdlog::debug("rocksdb: Saved vertex with key {} and ts {}", utils::SerializeVertex(vertex), *commit_timestamp_);

    for (auto &edge_entry : vertex.out_edges) {
      EdgeRef edge = std::get<2>(edge_entry);
      auto [src_dest_key, dest_src_key] = utils::SerializeEdge(
          vertex.gid, std::get<1>(edge_entry)->gid, std::get<0>(edge_entry), edge, config_.properties_on_edges);

      if (!WriteEdgeToDisk(edge, src_dest_key)) {
        return StorageDataManipulationError{SerializationError{}};
      }

      if (!WriteEdgeToDisk(edge, dest_src_key)) {
        return StorageDataManipulationError{SerializationError{}};
      }

      num_ser_edges++;
    }
  }

  for (const auto &[vertex_gid, serialized_vertex_to_delete] : vertices_to_delete_) {
    if (!DeleteVertexFromDisk(serialized_vertex_to_delete) ||
        !disk_unique_constraints->ClearDeletedVertex(vertex_gid, *commit_timestamp_) ||
        !disk_label_index->ClearDeletedVertex(vertex_gid, *commit_timestamp_) ||
        !disk_label_property_index->ClearDeletedVertex(vertex_gid, *commit_timestamp_)) {
      return StorageDataManipulationError{SerializationError{}};
    }
  }

  for (const auto &edge_to_delete : edges_to_delete_) {
    if (!DeleteEdgeFromDisk(edge_to_delete)) {
      return StorageDataManipulationError{SerializationError{}};
    }
  }

  if (!disk_unique_constraints->DeleteVerticesWithRemovedConstraintLabel(transaction_.start_timestamp,
                                                                         *commit_timestamp_) ||
      !disk_label_index->DeleteVerticesWithRemovedIndexingLabel(transaction_.start_timestamp, *commit_timestamp_) ||
      !disk_label_property_index->DeleteVerticesWithRemovedIndexingLabel(transaction_.start_timestamp,
                                                                         *commit_timestamp_)) {
    return StorageDataManipulationError{SerializationError{}};
  }

  return {};
}

[[nodiscard]] std::optional<ConstraintViolation> DiskStorage::CheckExistingVerticesBeforeCreatingExistenceConstraint(
    LabelId label, PropertyId property) const {
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(it->key().ToString());
    PropertyStore properties = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToStringView());
    if (utils::Contains(labels, label) && !properties.HasProperty(property)) {
      return ConstraintViolation{ConstraintViolation::Type::EXISTENCE, label, std::set<PropertyId>{property}};
    }
  }
  return std::nullopt;
}

[[nodiscard]] utils::BasicResult<ConstraintViolation, std::vector<std::pair<std::string, std::string>>>
DiskStorage::CheckExistingVerticesBeforeCreatingUniqueConstraint(LabelId label,
                                                                 const std::set<PropertyId> &properties) const {
  std::set<std::vector<PropertyValue>> unique_storage;
  std::vector<std::pair<std::string, std::string>> vertices_for_constraints;

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key_str = it->key().ToString();
    std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(key_str);
    PropertyStore property_store = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToStringView());
    if (utils::Contains(labels, label) && property_store.HasAllProperties(properties)) {
      if (auto target_property_values = property_store.ExtractPropertyValues(properties);
          target_property_values.has_value() && !utils::Contains(unique_storage, *target_property_values)) {
        unique_storage.insert(*target_property_values);
        vertices_for_constraints.emplace_back(
            utils::SerializeVertexAsKeyForUniqueConstraint(label, properties, utils::ExtractGidFromKey(key_str)),
            utils::SerializeVertexAsValueForUniqueConstraint(label, labels, property_store));
      } else {
        return ConstraintViolation{ConstraintViolation::Type::UNIQUE, label, properties};
      }
    }
  }
  return vertices_for_constraints;
}

// this will be modified here for a disk-based storage
/// TODO: andi solve default argument warning
utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto could_replicate_all_sync_replicas = true;

  if (transaction_.deltas.empty() ||
      std::all_of(transaction_.deltas.begin(), transaction_.deltas.end(),
                  [](const Delta &delta) { return delta.action == Delta::Action::DELETE_DESERIALIZED_OBJECT; })) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    // If there are no deltas, then we don't have to serialize anything on the disk.
    disk_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  } else {
    // Result of validating the vertex against unqiue constraints. It has to be
    // declared outside of the critical section scope because its value is
    // tested for Abort call which has to be done out of the scope.
    std::optional<ConstraintViolation> unique_constraint_violation;

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;

    {
      std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
      commit_timestamp_.emplace(disk_storage->CommitTimestamp(desired_commit_timestamp));

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
          could_replicate_all_sync_replicas =
              disk_storage->AppendToWalDataManipulation(transaction_, *commit_timestamp_);
        }

        // Take committed_transactions lock while holding the engine lock to
        // make sure that committed transactions are sorted by the commit
        // timestamp in the list.
        disk_storage->committed_transactions_.WithLock([&](auto &committed_transactions) {
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

        disk_storage->commit_log_->MarkFinished(start_timestamp);
      }
    }

    if (auto res = CheckConstraintsAndFlushMainMemoryCache(); res.HasError()) {
      Abort();
      return res;
    }
  }

  if (commit_timestamp_) {
    // commit_timestamp_ is set only if the transaction has writes.
    logging::AssertRocksDBStatus(disk_transaction_->SetCommitTimestamp(*commit_timestamp_));
  }
  auto commitStatus = disk_transaction_->Commit();
  delete disk_transaction_;
  disk_transaction_ = nullptr;
  if (!commitStatus.ok()) {
    spdlog::error("rocksdb: Commit failed with status {}", commitStatus.ToString());
    return StorageDataManipulationError{SerializationError{}};
  }

  is_transaction_active_ = false;

  if (!could_replicate_all_sync_replicas) {
    return StorageDataManipulationError{ReplicationError{}};
  }

  return {};
}

std::vector<std::pair<std::string, std::string>> DiskStorage::SerializeVerticesForLabelIndex(LabelId label) {
  std::vector<std::pair<std::string, std::string>> vertices_to_be_indexed;

  rocksdb::ReadOptions ro;
  auto strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));

  const std::string serialized_label = utils::SerializeIdType(label);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key_str = it->key().ToString();
    if (const std::vector<std::string> labels_str = utils::ExtractLabelsFromMainDiskStorage(key_str);
        utils::Contains(labels_str, serialized_label)) {
      std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(key_str);
      PropertyStore property_store = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToStringView());
      vertices_to_be_indexed.emplace_back(
          utils::SerializeVertexAsKeyForLabelIndex(utils::SerializeIdType(label),
                                                   utils::ExtractGidFromMainDiskStorage(key_str)),
          utils::SerializeVertexAsValueForLabelIndex(label, labels, property_store));
    }
  }
  return vertices_to_be_indexed;
}

std::vector<std::pair<std::string, std::string>> DiskStorage::SerializeVerticesForLabelPropertyIndex(
    LabelId label, PropertyId property) {
  std::vector<std::pair<std::string, std::string>> vertices_to_be_indexed;

  rocksdb::ReadOptions ro;
  auto strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));

  const std::string serialized_label = utils::SerializeIdType(label);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key_str = it->key().ToString();
    PropertyStore property_store = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToString());
    if (const std::vector<std::string> labels_str = utils::ExtractLabelsFromMainDiskStorage(key_str);
        utils::Contains(labels_str, serialized_label) && property_store.HasProperty(property)) {
      std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(key_str);
      vertices_to_be_indexed.emplace_back(
          utils::SerializeVertexAsKeyForLabelPropertyIndex(utils::SerializeIdType(label),
                                                           utils::SerializeIdType(property),
                                                           utils::ExtractGidFromMainDiskStorage(key_str)),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, labels, property_store));
    }
  }
  return vertices_to_be_indexed;
}

void DiskStorage::DiskAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  auto scope_exit = utils::OnScopeExit([&]() {
    // On abort we need to delete disk transaction because after storage remove we couldn't remove
    // disk_transaction correctly in destructor.
    // This happens in tests when we create and remove storage in one test. For example, in
    // query_plan_accumulate_aggregate.cpp
    disk_transaction_->ClearSnapshot();
    delete disk_transaction_;
    disk_transaction_ = nullptr;
  });

  // We collect vertices and edges we've created here and then splice them into
  // `deleted_vertices_` and `deleted_edges_` lists, instead of adding them one
  // by one and acquiring lock every time.
  std::list<Gid> my_deleted_vertices;
  std::list<Gid> my_deleted_edges;

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
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
            case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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
            case Delta::Action::DELETE_DESERIALIZED_OBJECT:
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

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);

    disk_storage->deleted_vertices_.WithLock(
        [&](auto &deleted_vertices) { deleted_vertices.splice(deleted_vertices.begin(), my_deleted_vertices); });
    disk_storage->deleted_edges_.WithLock(
        [&](auto &deleted_edges) { deleted_edges.splice(deleted_edges.begin(), my_deleted_edges); });
  }

  disk_storage->commit_log_->MarkFinished(transaction_.start_timestamp);
  disk_transaction_->Rollback();
  is_transaction_active_ = false;
}

// maybe will need some usages here
void DiskStorage::DiskAccessor::FinalizeTransaction() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (commit_timestamp_) {
    disk_storage->commit_log_->MarkFinished(*commit_timestamp_);
    disk_storage->committed_transactions_.WithLock(
        [&](auto &committed_transactions) { committed_transactions.emplace_back(std::move(transaction_)); });
    commit_timestamp_.reset();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  if (!disk_label_index->CreateIndex(label, SerializeVerticesForLabelIndex(label))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  const auto success =
      AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_INDEX_CREATE, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

// this should be handled on an above level of abstraction
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
  if (!disk_label_property_index->CreateIndex(label, property,
                                              SerializeVerticesForLabelPropertyIndex(label, property))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

/// TODO: extract at the higher level
utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_index_->DropIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success =
      AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  if (!indices_.label_property_index_->DropIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success =
      AppendToWalDataDefinition(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageIndexDefinitionError{ReplicationError{}};
}

/// TODO: andi possible to put it into abstract storage
IndicesInfo DiskStorage::ListAllIndices() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {indices_.label_index_->ListIndices(), indices_.label_property_index_->ListIndices()};
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void> DiskStorage::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  if (constraints_.existence_constraints_->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }

  if (auto check = CheckExistingVerticesBeforeCreatingExistenceConstraint(label, property); check.has_value()) {
    return StorageExistenceConstraintDefinitionError{check.value()};
  }

  constraints_.existence_constraints_->InsertConstraint(label, property);

  /// TODO: andi extract this behavior into some private method
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  return StorageExistenceConstraintDefinitionError{ReplicationError{}};
}

/// TODO: andi Possible to move on abstract level
utils::BasicResult<StorageExistenceConstraintDroppingError, void> DiskStorage::DropExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  if (!constraints_.existence_constraints_->DropConstraint(label, property)) {
    return StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}};
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP, label,
                                           {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return {};
  }

  /// TODO: andi We don't support but return replication error. Refactor everything related to replication.
  return StorageExistenceConstraintDroppingError{ReplicationError{}};
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
DiskStorage::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                    const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());

  if (auto constraint_check = disk_unique_constraints->CheckIfConstraintCanBeCreated(label, properties);
      constraint_check != UniqueConstraints::CreationStatus::SUCCESS) {
    return constraint_check;
  }

  auto check = CheckExistingVerticesBeforeCreatingUniqueConstraint(label, properties);
  if (check.HasError()) {
    return StorageUniqueConstraintDefinitionError{check.GetError()};
  }

  if (!disk_unique_constraints->InsertConstraint(label, properties, check.GetValue())) {
    return StorageUniqueConstraintDefinitionError{ConstraintDefinitionError{}};
  }

  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE, label,
                                           properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return UniqueConstraints::CreationStatus::SUCCESS;
  }
  /// TODO: andi Remove replication error.
  return StorageUniqueConstraintDefinitionError{ReplicationError{}};
}

/// TODO: andi Possible to move on abstract level
utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
DiskStorage::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                  const std::optional<uint64_t> desired_commit_timestamp) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = constraints_.unique_constraints_->DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  auto success = AppendToWalDataDefinition(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP, label,
                                           properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;

  if (success) {
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }

  return StorageUniqueConstraintDroppingError{ReplicationError{}};
}

/// TODO: andi this should be handled on an above level of abstraction
ConstraintsInfo DiskStorage::ListAllConstraints() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {constraints_.existence_constraints_->ListConstraints(), constraints_.unique_constraints_->ListConstraints()};
}

Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) {
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
  return {transaction_id, start_timestamp, isolation_level, storage_mode};
}

template <bool force>
void DiskStorage::CollectGarbage() {
  if constexpr (force) {
    // We take the unique lock on the main storage lock so we can forcefully clean
    // everything we can
    if (!main_lock_.try_lock()) {
      CollectGarbage<false>();
      return;
    }
  } else {
    // Because the garbage collector iterates through the indices and constraints
    // to clean them up, it must take the main lock for reading to make sure that
    // the indices and constraints aren't concurrently being modified.
    main_lock_.lock_shared();
  }

  utils::OnScopeExit lock_releaser{[&] {
    if constexpr (force) {
      main_lock_.unlock();
    } else {
      main_lock_.unlock_shared();
    }
  }};

  // Garbage collection must be performed in two phases. In the first phase,
  // deltas that won't be applied by any transaction anymore are unlinked from
  // the version chains. They cannot be deleted immediately, because there
  // might be a transaction that still needs them to terminate the version
  // chain traversal. They are instead marked for deletion and will be deleted
  // in the second GC phase in this GC iteration or some of the following
  // ones.
  std::unique_lock<std::mutex> gc_guard(gc_lock_, std::try_to_lock);
  if (!gc_guard.owns_lock()) {
    return;
  }

  uint64_t oldest_active_start_timestamp = commit_log_->OldestActive();
  // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
  // list immediately, because we would have to repeatedly take
  // garbage_undo_buffers lock.
  std::list<std::pair<uint64_t, std::list<Delta>>> unlinked_undo_buffers;

  // We will only free vertices deleted up until now in this GC cycle, and we
  // will do it after cleaning-up the indices. That way we are sure that all
  // vertices that appear in an index also exist in main storage.
  std::list<Gid> current_deleted_edges;
  std::list<Gid> current_deleted_vertices;
  deleted_vertices_->swap(current_deleted_vertices);
  deleted_edges_->swap(current_deleted_edges);

  while (true) {
    // We don't want to hold the lock on commited transactions for too long,
    // because that prevents other transactions from committing.
    Transaction *transaction;
    {
      auto committed_transactions_ptr = committed_transactions_.Lock();
      if (committed_transactions_ptr->empty()) {
        break;
      }
      transaction = &committed_transactions_ptr->front();
    }

    auto commit_timestamp = transaction->commit_timestamp->load(std::memory_order_acquire);
    if (commit_timestamp >= oldest_active_start_timestamp) {
      break;
    }

    // When unlinking a delta which is the first delta in its version chain,
    // special care has to be taken to avoid the following race condition:
    //
    // [Vertex] --> [Delta A]
    //
    //    GC thread: Delta A is the first in its chain, it must be unlinked from
    //               vertex and marked for deletion
    //    TX thread: Update vertex and add Delta B with Delta A as next
    //
    // [Vertex] --> [Delta B] <--> [Delta A]
    //
    //    GC thread: Unlink delta from Vertex
    //
    // [Vertex] --> (nullptr)
    //
    // When processing a delta that is the first one in its chain, we
    // obtain the corresponding vertex or edge lock, and then verify that this
    // delta still is the first in its chain.
    // When processing a delta that is in the middle of the chain we only
    // process the final delta of the given transaction in that chain. We
    // determine the owner of the chain (either a vertex or an edge), obtain the
    // corresponding lock, and then verify that this delta is still in the same
    // position as it was before taking the lock.
    //
    // Even though the delta chain is lock-free (both `next` and `prev`) the
    // chain should not be modified without taking the lock from the object that
    // owns the chain (either a vertex or an edge). Modifying the chain without
    // taking the lock will cause subtle race conditions that will leave the
    // chain in a broken state.
    // The chain can be only read without taking any locks.

    for (Delta &delta : transaction->deltas) {
      while (true) {
        auto prev = delta.prev.Get();
        switch (prev.type) {
          case PreviousPtr::Type::VERTEX: {
            Vertex *vertex = prev.vertex;
            std::lock_guard<utils::SpinLock> vertex_guard(vertex->lock);
            if (vertex->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            vertex->delta = nullptr;
            if (vertex->deleted) {
              current_deleted_vertices.push_back(vertex->gid);
            }
            break;
          }
          case PreviousPtr::Type::EDGE: {
            Edge *edge = prev.edge;
            std::lock_guard<utils::SpinLock> edge_guard(edge->lock);
            if (edge->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            edge->delta = nullptr;
            if (edge->deleted) {
              current_deleted_edges.push_back(edge->gid);
            }
            break;
          }
          case PreviousPtr::Type::DELTA: {
            if (prev.delta->timestamp->load(std::memory_order_acquire) == commit_timestamp) {
              // The delta that is newer than this one is also a delta from this
              // transaction. We skip the current delta and will remove it as a
              // part of the suffix later.
              break;
            }
            std::unique_lock<utils::SpinLock> guard;
            {
              // We need to find the parent object in order to be able to use
              // its lock.
              auto parent = prev;
              while (parent.type == PreviousPtr::Type::DELTA) {
                parent = parent.delta->prev.Get();
              }
              switch (parent.type) {
                case PreviousPtr::Type::VERTEX:
                  guard = std::unique_lock<utils::SpinLock>(parent.vertex->lock);
                  break;
                case PreviousPtr::Type::EDGE:
                  guard = std::unique_lock<utils::SpinLock>(parent.edge->lock);
                  break;
                case PreviousPtr::Type::DELTA:
                case PreviousPtr::Type::NULLPTR:
                  LOG_FATAL("Invalid database state!");
              }
            }
            if (delta.prev.Get() != prev) {
              // Something changed, we could now be the first delta in the
              // chain.
              continue;
            }
            Delta *prev_delta = prev.delta;
            prev_delta->next.store(nullptr, std::memory_order_release);
            break;
          }
          case PreviousPtr::Type::NULLPTR: {
            LOG_FATAL("Invalid pointer!");
          }
        }
        break;
      }
    }

    committed_transactions_.WithLock([&](auto &committed_transactions) {
      unlinked_undo_buffers.emplace_back(0, std::move(transaction->deltas));
      committed_transactions.pop_front();
    });
  }
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
  if (!InitializeWalFile()) {
    return true;
  }
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

  // if (replication_role_.load() == ReplicationRole::MAIN) {
  //   replication_clients_.WithLock([&](auto &clients) {
  //     for (auto &client : clients) {
  //       client->StartTransactionReplication(wal_file_->SequenceNumber());
  //     }
  //   });
  // }

  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent, auto filter) {
    while (true) {
      auto *older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
        // replication_clients_.WithLock([&](auto &clients) {
        //   for (auto &client : clients) {
        //     client->IfStreamingTransaction(
        //         [&](auto &stream) { stream.AppendDelta(*delta, parent, final_commit_timestamp); });
        //   }
        // });
      }
      auto prev = delta->prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::DELTA) break;
      delta = prev.delta;
    }
  };

  // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
  // don't traverse them in that order. That is because for each delta we need
  // information about the vertex or edge they belong to and that information
  // isn't stored in the deltas themselves. In order to find out information
  // about the corresponding vertex or edge it is necessary to traverse the
  // delta chain for each delta until a vertex or edge is encountered. This
  // operation is very expensive as the chain grows.
  // Instead, we traverse the edges until we find a vertex or edge and traverse
  // their delta chains. This approach has a drawback because we lose the
  // correct order of the operations. Because of that, we need to traverse the
  // deltas several times and we have to manually ensure that the stored deltas
  // will be ordered correctly.

  // 1. Process all Vertex deltas and store all operations that create vertices
  // and modify vertex data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
          return true;

        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 2. Process all Vertex deltas and store all operations that create edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          return false;
      }
    });
  }
  // 3. Process all Edge deltas and store all operations that modify edge data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, *prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 4. Process all Vertex deltas and store all operations that delete edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 5. Process all Vertex deltas and store all operations that delete vertices.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;
        case Delta::Action::DELETE_DESERIALIZED_OBJECT:
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.
  wal_file_->AppendTransactionEnd(final_commit_timestamp);

  FinalizeWalFile();

  auto finalized_on_all_replicas = true;
  // replication_clients_.WithLock([&](auto &clients) {
  //   for (auto &client : clients) {
  //     client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(final_commit_timestamp); });
  //     const auto finalized = client->FinalizeTransactionReplication();

  //     if (client->Mode() == replication::ReplicationMode::SYNC) {
  //       finalized_on_all_replicas = finalized && finalized_on_all_replicas;
  //     }
  //   }
  // });

  return finalized_on_all_replicas;
}

bool DiskStorage::AppendToWalDataDefinition(durability::StorageGlobalOperation operation, LabelId label,
                                            const std::set<PropertyId> &properties, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) {
    return true;
  }

  auto finalized_on_all_replicas = true;
  wal_file_->AppendOperation(operation, label, properties, final_commit_timestamp);
  // {
  //   if (replication_role_.load() == ReplicationRole::MAIN) {
  //     replication_clients_.WithLock([&](auto &clients) {
  //       for (auto &client : clients) {
  //         client->StartTransactionReplication(wal_file_->SequenceNumber());
  //         client->IfStreamingTransaction(
  //             [&](auto &stream) { stream.AppendOperation(operation, label, properties, final_commit_timestamp); });

  //         const auto finalized = client->FinalizeTransactionReplication();
  //         if (client->Mode() == replication::ReplicationMode::SYNC) {
  //           finalized_on_all_replicas = finalized && finalized_on_all_replicas;
  //         }
  //       }
  //     });
  //   }
  // }
  FinalizeWalFile();
  return finalized_on_all_replicas;
}

utils::BasicResult<DiskStorage::CreateSnapshotError> DiskStorage::CreateSnapshot(std::optional<bool> is_periodic) {
  return {};
}

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

ReplicationRole DiskStorage::GetReplicationRole() const {
  /// TODO(andi): Since we won't support replication for the beginning don't crash with this, rather just log a
  /// message
  spdlog::debug("GetReplicationRole called, but we don't support replication yet");
  return {};
}

std::vector<DiskStorage::ReplicaInfo> DiskStorage::ReplicasInfo() { throw utils::NotYetImplemented("ReplicasInfo"); }

utils::BasicResult<DiskStorage::SetIsolationLevelError> DiskStorage::SetIsolationLevel(IsolationLevel isolation_level) {
  std::unique_lock main_guard{main_lock_};
  if (storage_mode_ == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    return Storage::SetIsolationLevelError::DisabledForAnalyticalMode;
  }

  isolation_level_ = isolation_level;
  return {};
}

void DiskStorage::RestoreReplicas() { throw utils::NotYetImplemented("RestoreReplicas"); }

bool DiskStorage::ShouldStoreAndRestoreReplicas() const {
  throw utils::NotYetImplemented("ShouldStoreAndRestoreReplicas");
}

}  // namespace memgraph::storage
