// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/disk/storage.hpp"

#include <atomic>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>

#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "kvstore/kvstore.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/disk/edge_import_mode_cache.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/edges_iterable.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/modified_edge.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/disk_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/result.hpp"
#include "utils/rocksdb_serialization.hpp"
#include "utils/skip_list.hpp"
#include "utils/stat.hpp"
#include "utils/string.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::storage {

namespace {

auto FindEdges(const View view, EdgeTypeId edge_type, const VertexAccessor *from_vertex, VertexAccessor *to_vertex)
    -> Result<EdgesVertexAccessorResult> {
  auto use_out_edges = [](Vertex const *from_vertex, Vertex const *to_vertex) {
    // Obtain the locks by `gid` order to avoid lock cycles.
    auto guard_from = std::unique_lock{from_vertex->lock, std::defer_lock};
    auto guard_to = std::unique_lock{to_vertex->lock, std::defer_lock};
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

    // With the potentially cheaper side FindEdges
    const auto out_n = from_vertex->out_edges.size();
    const auto in_n = to_vertex->in_edges.size();
    return out_n <= in_n;
  };

  return use_out_edges(from_vertex->vertex_, to_vertex->vertex_) ? from_vertex->OutEdges(view, {edge_type}, to_vertex)
                                                                 : to_vertex->InEdges(view, {edge_type}, from_vertex);
}

}  // namespace

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

constexpr const char *kDeserializeTimestamp = "0";
constexpr const char *kVertexHandle = "vertex";
constexpr const char *kEdgeHandle = "edge";
constexpr const char *kDefaultHandle = "default";
constexpr const char *kOutEdgesHandle = "out_edges";
constexpr const char *kInEdgesHandle = "in_edges";
constexpr const char *kLabelPropertyIndexStr = "label_property_index";
constexpr const char *kExistenceConstraintsStr = "existence_constraints";

/// TODO: (andi) Maybe a better way of checking would be if the first delta is DELETE_DESERIALIZED
/// then we now that the vertex has only been deserialized and nothing more has been done on it.
bool VertexNeedsToBeSerialized(const Vertex &vertex) {
  Delta *head = vertex.delta();
  while (head != nullptr) {
    if (head->action == Delta::Action::ADD_LABEL || head->action == Delta::Action::REMOVE_LABEL ||
        head->action == Delta::Action::DELETE_OBJECT || head->action == Delta::Action::RECREATE_OBJECT ||
        head->action == Delta::Action::SET_PROPERTY) {
      return true;
    }
    head = head->next;
  }
  return false;
}

bool VertexHasLabel(const Vertex &vertex, LabelId label, Transaction *transaction, View view) {
  bool deleted = vertex.deleted();
  bool has_label = std::find(vertex.labels.begin(), vertex.labels.end(), label) != vertex.labels.end();
  Delta *delta = vertex.delta();
  ApplyDeltasForRead(transaction, delta, view, [&deleted, &has_label, label](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::REMOVE_LABEL: {
        if (delta.label.value == label) {
          MG_ASSERT(has_label, "Invalid database state!");
          has_label = false;
        }
        break;
      }
      case Delta::Action::ADD_LABEL: {
        if (delta.label.value == label) {
          MG_ASSERT(!has_label, "Invalid database state!");
          has_label = true;
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        break;
      }
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
  bool deleted = vertex.deleted();
  PropertyValue value = vertex.properties.GetProperty(property);
  Delta *delta = vertex.delta();
  ApplyDeltasForRead(transaction, delta, view, [&deleted, &value, property](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::SET_PROPERTY: {
        if (delta.property.key == property) {
          value = *delta.property.value;
        }
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
        break;
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

bool VertexHasEqualPropertyValue(const Vertex &vertex, PropertyId property_id, PropertyValue property_value,
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

DiskStorage::DiskStorage(Config config)
    : Storage(config, StorageMode::ON_DISK_TRANSACTIONAL),
      kvstore_(std::make_unique<RocksDBStorage>()),
      durable_metadata_(config) {
  LoadPersistingMetadataInfo();
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  kvstore_->options_.compression = rocksdb::kNoCompression;
  kvstore_->options_.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;
  kvstore_->options_.wal_dir = config_.disk.wal_directory;
  kvstore_->options_.wal_compression = rocksdb::kNoCompression;
  std::vector<rocksdb::ColumnFamilyHandle *> column_handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  if (utils::DirExists(config.disk.main_storage_directory)) {
    column_families.emplace_back(kVertexHandle, kvstore_->options_);
    column_families.emplace_back(kEdgeHandle, kvstore_->options_);
    column_families.emplace_back(kDefaultHandle, kvstore_->options_);
    column_families.emplace_back(kOutEdgesHandle, kvstore_->options_);
    column_families.emplace_back(kInEdgesHandle, kvstore_->options_);

    logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                              config.disk.main_storage_directory, column_families,
                                                              &column_handles, &kvstore_->db_));
    kvstore_->vertex_chandle = column_handles[0];
    kvstore_->edge_chandle = column_handles[1];
    kvstore_->default_chandle = column_handles[2];
    kvstore_->out_edges_chandle = column_handles[3];
    kvstore_->in_edges_chandle = column_handles[4];
  } else {
    logging::AssertRocksDBStatus(rocksdb::TransactionDB::Open(kvstore_->options_, rocksdb::TransactionDBOptions(),
                                                              config.disk.main_storage_directory, &kvstore_->db_));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, kVertexHandle, &kvstore_->vertex_chandle));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, kEdgeHandle, &kvstore_->edge_chandle));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, kOutEdgesHandle, &kvstore_->out_edges_chandle));
    logging::AssertRocksDBStatus(
        kvstore_->db_->CreateColumnFamily(kvstore_->options_, kInEdgesHandle, &kvstore_->in_edges_chandle));
  }
}

DiskStorage::~DiskStorage() {
  durable_metadata_.UpdateMetaData(timestamp_, vertex_count_.load(std::memory_order_acquire),
                                   edge_count_.load(std::memory_order_acquire));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->vertex_chandle));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->edge_chandle));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->out_edges_chandle));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->in_edges_chandle));
  if (kvstore_->default_chandle) {
    // We must destroy default column family handle only if it was read from existing database.
    // https://github.com/facebook/rocksdb/issues/5006#issuecomment-1003154821
    logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->default_chandle));
  }
  delete kvstore_->options_.comparator;
  kvstore_->options_.comparator = nullptr;
}

DiskStorage::DiskAccessor::DiskAccessor(auto tag, DiskStorage *storage, IsolationLevel isolation_level,
                                        StorageMode storage_mode)
    : Accessor(tag, storage, isolation_level, storage_mode,
               memgraph::replication_coordination_glue::ReplicationRole::MAIN) {
  rocksdb::WriteOptions write_options;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  transaction_.disk_transaction_ = storage->kvstore_->db_->BeginTransaction(write_options, txOptions);
  transaction_.disk_transaction_->SetReadTimestampForValidation(transaction_.start_timestamp);
}

DiskStorage::DiskAccessor::DiskAccessor(DiskAccessor &&other) noexcept : Accessor(std::move(other)) {}

DiskStorage::DiskAccessor::~DiskAccessor() {
  if (is_transaction_active_) {
    Abort();
  }
  FinalizeTransaction();
}

void DiskStorage::LoadPersistingMetadataInfo() {
  if (auto last_timestamp = durable_metadata_.LoadTimestampIfExists(); last_timestamp.has_value()) {
    timestamp_ = last_timestamp.value();
  }
  if (auto vertex_count = durable_metadata_.LoadVertexCountIfExists(); vertex_count.has_value()) {
    vertex_count_ = vertex_count.value();
  }
  if (auto edge_count = durable_metadata_.LoadEdgeCountIfExists(); edge_count.has_value()) {
    edge_count_ = edge_count.value();
  }
  if (auto label_index = durable_metadata_.LoadLabelIndexInfoIfExists(); label_index.has_value()) {
    auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
    disk_label_index->LoadIndexInfo(label_index.value());
  }
  if (auto label_property_index = durable_metadata_.LoadLabelPropertyIndexInfoIfExists();
      label_property_index.has_value()) {
    auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
    disk_label_property_index->LoadIndexInfo(label_property_index.value());
  }
  if (auto existence_constraints = durable_metadata_.LoadExistenceConstraintInfoIfExists();
      existence_constraints.has_value()) {
    constraints_.existence_constraints_->LoadExistenceConstraints(existence_constraints.value());
  }
  if (auto unique_constraints = durable_metadata_.LoadUniqueConstraintInfoIfExists(); unique_constraints.has_value()) {
    auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
    disk_unique_constraints->LoadUniqueConstraints(unique_constraints.value());
  }
}

std::optional<storage::VertexAccessor> DiskStorage::LoadVertexToLabelIndexCache(
    Transaction *transaction, const std::string &key, const std::string &value, Delta *index_delta,
    utils::SkipList<storage::Vertex>::Accessor index_accessor) {
  storage::Gid gid = Gid::FromString(utils::ExtractGidFromLabelIndexStorage(key));
  if (ObjectExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelIndexStorage(key, value)};
  PropertyStore properties{utils::DeserializePropertiesFromLabelIndexStorage(value)};
  return CreateVertexFromDisk(transaction, index_accessor, gid, std::move(labels_id), std::move(properties),
                              index_delta);
}

/// TODO: can be decoupled by providing as arguments extractor functions and delta.
std::optional<storage::VertexAccessor> DiskStorage::LoadVertexToLabelPropertyIndexCache(
    Transaction *transaction, const std::string &key, const std::string &value, Delta *index_delta,
    utils::SkipList<storage::Vertex>::Accessor index_accessor) {
  storage::Gid gid = Gid::FromString(utils::ExtractGidFromLabelPropertyIndexStorage(key));
  if (ObjectExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelPropertyIndexStorage(key, value)};
  PropertyStore properties{utils::DeserializePropertiesFromLabelPropertyIndexStorage(value)};
  return CreateVertexFromDisk(transaction, index_accessor, gid, std::move(labels_id), std::move(properties),
                              index_delta);
}

void DiskStorage::LoadVerticesToMainMemoryCache(Transaction *transaction) {
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it =
      std::unique_ptr<rocksdb::Iterator>(transaction->disk_transaction_->GetIterator(ro, kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // We should pass it->timestamp().ToString() instead of "0"
    // This is hack until RocksDB will support timestamp() in WBWI iterator
    LoadVertexToMainMemoryCache(transaction, it->key().ToString(), it->value().ToString(), kDeserializeTimestamp);
  }
}

/// TODO: When loading from disk, you can in some situations load from index rocksdb not the main one
/// TODO: send from and to as arguments and remove so many methods
void DiskStorage::LoadVerticesFromMainStorageToEdgeImportCache(Transaction *transaction) {
  auto cache_accessor = edge_import_mode_cache_->AccessToVertices();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it =
      std::unique_ptr<rocksdb::Iterator>(transaction->disk_transaction_->GetIterator(ro, kvstore_->vertex_chandle));

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string value = it->value().ToString();
    storage::Gid gid = Gid::FromString(utils::ExtractGidFromMainDiskStorage(key));
    if (ObjectExistsInCache(cache_accessor, gid)) continue;

    std::vector<LabelId> labels_id{utils::DeserializeLabelsFromMainDiskStorage(key)};
    PropertyStore properties{utils::DeserializePropertiesFromMainDiskStorage(value)};
    CreateVertexFromDisk(transaction, cache_accessor, gid, std::move(labels_id), std::move(properties),
                         CreateDeleteDeserializedObjectDelta(transaction, std::move(key), kDeserializeTimestamp));
  }
}

void DiskStorage::HandleMainLoadingForEdgeImportCache(Transaction *transaction) {
  if (!edge_import_mode_cache_->AllVerticesScanned()) {
    LoadVerticesFromMainStorageToEdgeImportCache(transaction);
    edge_import_mode_cache_->SetScannedAllVertices();
  }
}

void DiskStorage::LoadVerticesFromLabelIndexStorageToEdgeImportCache(Transaction *transaction, LabelId label) {
  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  auto disk_index_transaction = disk_label_index->CreateRocksDBTransaction();
  auto cache_accessor = edge_import_mode_cache_->AccessToVertices();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));
  std::string label_prefix{label.ToString()};

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string value = it->value().ToString();
    if (key.starts_with(label_prefix)) {
      storage::Gid gid = Gid::FromString(utils::ExtractGidFromLabelIndexStorage(key));
      if (ObjectExistsInCache(cache_accessor, gid)) continue;

      std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelIndexStorage(key, value)};
      PropertyStore properties{utils::DeserializePropertiesFromLabelIndexStorage(value)};
      CreateVertexFromDisk(transaction, cache_accessor, gid, std::move(labels_id), std::move(properties),
                           CreateDeleteDeserializedObjectDelta(transaction, std::move(key), kDeserializeTimestamp));
    }
  }
}

void DiskStorage::HandleLoadingLabelForEdgeImportCache(Transaction *transaction, LabelId label) {
  if (!edge_import_mode_cache_->VerticesWithLabelScanned(label)) {
    LoadVerticesFromLabelIndexStorageToEdgeImportCache(transaction, label);

    if (!edge_import_mode_cache_->CreateIndex(label)) {
      throw utils::BasicException("Failed creation of in-memory label index.");
    }
  }
}

void DiskStorage::HandleLoadingLabelPropertyForEdgeImportCache(Transaction *transaction, LabelId label,
                                                               PropertyId property) {
  if (!edge_import_mode_cache_->VerticesWithLabelPropertyScanned(label, property)) {
    LoadVerticesFromLabelPropertyIndexStorageToEdgeImportCache(transaction, label, property);

    if (!edge_import_mode_cache_->CreateIndex(label, property)) {
      throw utils::BasicException("Failed creation of in-memory label-property index.");
    }
  }
}

/// TODO: Just extract disk_label_index and disk_label_property_index
void DiskStorage::LoadVerticesFromLabelPropertyIndexStorageToEdgeImportCache(Transaction *transaction, LabelId label,
                                                                             PropertyId property) {
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  auto cache_accessor = edge_import_mode_cache_->AccessToVertices();

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const std::string label_property_prefix = label.ToString() + "|" + property.ToString();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string value = it->value().ToString();
    if (key.starts_with(label_property_prefix)) {
      storage::Gid gid = Gid::FromString(utils::ExtractGidFromLabelPropertyIndexStorage(key));
      if (ObjectExistsInCache(cache_accessor, gid)) continue;

      std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelPropertyIndexStorage(key, value)};
      PropertyStore properties{utils::DeserializePropertiesFromLabelPropertyIndexStorage(value)};
      CreateVertexFromDisk(transaction, cache_accessor, gid, std::move(labels_id), std::move(properties),
                           CreateDeleteDeserializedObjectDelta(transaction, std::move(key), kDeserializeTimestamp));
    }
  }
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
    disk_storage->HandleMainLoadingForEdgeImportCache(&transaction_);

    return VerticesIterable(
        AllVerticesIterable(disk_storage->edge_import_mode_cache_->AccessToVertices(), storage_, &transaction_, view));
  }
  if (transaction_.scanned_all_vertices_) {
    return VerticesIterable(AllVerticesIterable(transaction_.vertices_->access(), storage_, &transaction_, view));
  }

  disk_storage->LoadVerticesToMainMemoryCache(&transaction_);
  transaction_.scanned_all_vertices_ = true;
  return VerticesIterable(AllVerticesIterable(transaction_.vertices_->access(), storage_, &transaction_, view));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);

  if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
    disk_storage->HandleLoadingLabelForEdgeImportCache(&transaction_, label);

    return VerticesIterable(disk_storage->edge_import_mode_cache_->Vertices(label, view, storage_, &transaction_));
  }

  transaction_.index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = transaction_.index_storage_.back();
  transaction_.index_deltas_storage_.emplace_back();
  auto &index_deltas = transaction_.index_deltas_storage_.back();

  auto gids = disk_storage->MergeVerticesFromMainCacheWithLabelIndexCache(&transaction_, label, view, index_deltas,
                                                                          indexed_vertices.get());
  disk_storage->LoadVerticesFromDiskLabelIndex(&transaction_, label, gids, index_deltas, indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), storage_, &transaction_, view));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
    disk_storage->HandleLoadingLabelPropertyForEdgeImportCache(&transaction_, label, property);

    return VerticesIterable(disk_storage->edge_import_mode_cache_->Vertices(label, property, std::nullopt, std::nullopt,
                                                                            view, storage_, &transaction_));
  }

  transaction_.index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = transaction_.index_storage_.back();
  transaction_.index_deltas_storage_.emplace_back();
  auto &index_deltas = transaction_.index_deltas_storage_.back();

  const auto label_property_filter = [this](const Vertex &vertex, LabelId label, PropertyId property,
                                            View view) -> bool {
    return VertexHasLabel(vertex, label, &transaction_, view) &&
           HasVertexProperty(vertex, property, &transaction_, view);
  };

  const auto gids = disk_storage->MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
      &transaction_, label, property, view, index_deltas, indexed_vertices.get(), label_property_filter);

  const auto disk_label_property_filter = [](const std::string &key, const std::string &label_property_prefix,
                                             const std::unordered_set<Gid> &gids, Gid curr_gid) -> bool {
    return key.starts_with(label_property_prefix) && !utils::Contains(gids, curr_gid);
  };

  disk_storage->LoadVerticesFromDiskLabelPropertyIndex(&transaction_, label, property, gids, index_deltas,
                                                       indexed_vertices.get(), disk_label_property_filter);

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), storage_, &transaction_, view));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value,
                                                     View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
    disk_storage->HandleLoadingLabelPropertyForEdgeImportCache(&transaction_, label, property);

    return VerticesIterable(disk_storage->edge_import_mode_cache_->Vertices(
        label, property, utils::MakeBoundInclusive(value), utils::MakeBoundInclusive(value), view, storage_,
        &transaction_));
  }

  transaction_.index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = transaction_.index_storage_.back();
  transaction_.index_deltas_storage_.emplace_back();
  auto &index_deltas = transaction_.index_deltas_storage_.back();

  auto label_property_filter = [this, &value](const Vertex &vertex, LabelId label, PropertyId property,
                                              View view) -> bool {
    return VertexHasLabel(vertex, label, &transaction_, view) &&
           VertexHasEqualPropertyValue(vertex, property, value, &transaction_, view);
  };

  const auto gids = disk_storage->MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
      &transaction_, label, property, view, index_deltas, indexed_vertices.get(), label_property_filter);

  disk_storage->LoadVerticesFromDiskLabelPropertyIndexWithPointValueLookup(&transaction_, label, property, gids, value,
                                                                           index_deltas, indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), storage_, &transaction_, view));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property,
                                                     const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                     const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                     View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
    disk_storage->HandleLoadingLabelPropertyForEdgeImportCache(&transaction_, label, property);

    return VerticesIterable(disk_storage->edge_import_mode_cache_->Vertices(label, property, lower_bound, upper_bound,
                                                                            view, storage_, &transaction_));
  }

  transaction_.index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = transaction_.index_storage_.back();
  transaction_.index_deltas_storage_.emplace_back();
  auto &index_deltas = transaction_.index_deltas_storage_.back();

  const auto gids = disk_storage->MergeVerticesFromMainCacheWithLabelPropertyIndexCacheForIntervalSearch(
      &transaction_, label, property, view, lower_bound, upper_bound, index_deltas, indexed_vertices.get());

  disk_storage->LoadVerticesFromDiskLabelPropertyIndexForIntervalSearch(
      &transaction_, label, property, gids, lower_bound, upper_bound, index_deltas, indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), storage_, &transaction_, view));
}

/// TODO: (andi) This should probably go into some other class not the storage. All utils methods
std::unordered_set<Gid> DiskStorage::MergeVerticesFromMainCacheWithLabelIndexCache(
    Transaction *transaction, LabelId label, View view, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices) {
  auto main_cache_acc = transaction->vertices_->access();
  std::unordered_set<Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    if (VertexHasLabel(vertex, label, transaction, view)) {
      spdlog::trace("Loaded vertex with gid: {} from main index storage to label index", vertex.gid.ToString());
      uint64_t ts = utils::GetEarliestTimestamp(vertex.delta());
      /// TODO: here are doing serialization and then later deserialization again -> expensive
      LoadVertexToLabelIndexCache(transaction, utils::SerializeVertexAsKeyForLabelIndex(label, vertex.gid),
                                  utils::SerializeVertexAsValueForLabelIndex(label, vertex.labels, vertex.properties),
                                  CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::nullopt, ts),
                                  indexed_vertices->access());
    }
  }
  return gids;
}

void DiskStorage::LoadVerticesFromDiskLabelIndex(Transaction *transaction, LabelId label,
                                                 const std::unordered_set<storage::Gid> &gids,
                                                 std::list<Delta> &index_deltas,
                                                 utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  auto disk_index_transaction = disk_label_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction->start_timestamp);

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto serialized_label = label.ToString();
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromString(utils::ExtractGidFromLabelIndexStorage(key));
    spdlog::trace("Loaded vertex with key: {} from label index storage", key);
    if (key.starts_with(serialized_label) && !utils::Contains(gids, curr_gid)) {
      // We should pass it->timestamp().ToString() instead of "0"
      // This is hack until RocksDB will support timestamp() in WBWI iterator
      LoadVertexToLabelIndexCache(
          transaction, index_it->key().ToString(), index_it->value().ToString(),
          CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::move(key), kDeserializeTimestamp),
          indexed_vertices->access());
    }
  }
}

std::unordered_set<Gid> DiskStorage::MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
    Transaction *transaction, LabelId label, PropertyId property, View view, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices, const auto &label_property_filter) {
  auto main_cache_acc = transaction->vertices_->access();
  std::unordered_set<storage::Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    if (label_property_filter(vertex, label, property, view)) {
      uint64_t ts = utils::GetEarliestTimestamp(vertex.delta());
      LoadVertexToLabelPropertyIndexCache(
          transaction, utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties),
          CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::nullopt, ts), indexed_vertices->access());
    }
  }

  return gids;
}

void DiskStorage::LoadVerticesFromDiskLabelPropertyIndex(Transaction *transaction, LabelId label, PropertyId property,
                                                         const std::unordered_set<storage::Gid> &gids,
                                                         std::list<Delta> &index_deltas,
                                                         utils::SkipList<Vertex> *indexed_vertices,
                                                         const auto &label_property_filter) {
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction->start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto label_property_prefix = label.ToString() + "|" + property.ToString();
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromString(utils::ExtractGidFromLabelPropertyIndexStorage(key));
    if (label_property_filter(key, label_property_prefix, gids, curr_gid)) {
      // We should pass it->timestamp().ToString() instead of "0"
      // This is hack until RocksDB will support timestamp() in WBWI iterator
      LoadVertexToLabelPropertyIndexCache(
          transaction, index_it->key().ToString(), index_it->value().ToString(),
          CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::move(key), kDeserializeTimestamp),
          indexed_vertices->access());
    }
  }
}

void DiskStorage::LoadVerticesFromDiskLabelPropertyIndexWithPointValueLookup(
    Transaction *transaction, LabelId label, PropertyId property, const std::unordered_set<storage::Gid> &gids,
    const PropertyValue &value, std::list<Delta> &index_deltas, utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction->start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto label_property_prefix = label.ToString() + "|" + property.ToString();
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    std::string it_value = index_it->value().ToString();
    Gid curr_gid = Gid::FromString(utils::ExtractGidFromLabelPropertyIndexStorage(key));
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value);
    if (key.starts_with(label_property_prefix) && !utils::Contains(gids, curr_gid) &&
        properties.IsPropertyEqual(property, value)) {
      // We should pass it->timestamp().ToString() instead of "0"
      // This is hack until RocksDB will support timestamp() in WBWI iterator
      LoadVertexToLabelPropertyIndexCache(
          transaction, index_it->key().ToString(), index_it->value().ToString(),
          CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::move(key), kDeserializeTimestamp),
          indexed_vertices->access());
    }
  }
}

std::unordered_set<Gid> DiskStorage::MergeVerticesFromMainCacheWithLabelPropertyIndexCacheForIntervalSearch(
    Transaction *transaction, LabelId label, PropertyId property, View view,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices) {
  auto main_cache_acc = transaction->vertices_->access();
  std::unordered_set<storage::Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    auto prop_value = GetVertexProperty(vertex, property, transaction, view);
    if (VertexHasLabel(vertex, label, transaction, view) &&
        IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      uint64_t ts = utils::GetEarliestTimestamp(vertex.delta());
      LoadVertexToLabelPropertyIndexCache(
          transaction, utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties),
          CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::nullopt, ts), indexed_vertices->access());
    }
  }
  return gids;
}

void DiskStorage::LoadVerticesFromDiskLabelPropertyIndexForIntervalSearch(
    Transaction *transaction, LabelId label, PropertyId property, const std::unordered_set<storage::Gid> &gids,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction->start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const std::string label_property_prefix = label.ToString() + "|" + property.ToString();
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key_str = index_it->key().ToString();
    std::string it_value_str = index_it->value().ToString();
    Gid curr_gid = Gid::FromString(utils::ExtractGidFromLabelPropertyIndexStorage(key_str));
    /// TODO: andi this will be optimized
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value_str);
    PropertyValue prop_value = properties.GetProperty(property);
    if (!key_str.starts_with(label_property_prefix) || utils::Contains(gids, curr_gid) ||
        !IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      continue;
    }
    // We should pass it->timestamp().ToString() instead of "0"
    // This is hack until RocksDB will support timestamp() in WBWI iterator
    LoadVertexToLabelPropertyIndexCache(
        transaction, index_it->key().ToString(), index_it->value().ToString(),
        CreateDeleteDeserializedIndexObjectDelta(index_deltas, std::move(key_str), kDeserializeTimestamp),
        indexed_vertices->access());
  }
}

EdgesIterable DiskStorage::DiskAccessor::Edges(EdgeTypeId /*edge_type*/, View /*view*/) {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

uint64_t DiskStorage::DiskAccessor::ApproximateVertexCount() const {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  return disk_storage->vertex_count_.load(std::memory_order_acquire);
}

uint64_t DiskStorage::DiskAccessor::ApproximateEdgeCount(EdgeTypeId /*edge_type*/) const {
  spdlog::info("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return 0U;
}

uint64_t DiskStorage::GetDiskSpaceUsage() const {
  uint64_t main_disk_storage_size = utils::GetDirDiskUsage(config_.disk.main_storage_directory);
  uint64_t index_disk_storage_size = utils::GetDirDiskUsage(config_.disk.label_index_directory) +
                                     utils::GetDirDiskUsage(config_.disk.label_property_index_directory);
  uint64_t constraints_disk_storage_size = utils::GetDirDiskUsage(config_.disk.unique_constraints_directory);
  uint64_t metadata_disk_storage_size = utils::GetDirDiskUsage(config_.disk.id_name_mapper_directory) +
                                        utils::GetDirDiskUsage(config_.disk.name_id_mapper_directory);
  uint64_t durability_disk_storage_size =
      utils::GetDirDiskUsage(config_.disk.durability_directory) + utils::GetDirDiskUsage(config_.disk.wal_directory);
  return main_disk_storage_size + index_disk_storage_size + constraints_disk_storage_size + metadata_disk_storage_size +
         durability_disk_storage_size;
}

StorageInfo DiskStorage::GetBaseInfo() {
  StorageInfo info{};
  info.vertex_count = vertex_count_;
  info.edge_count = edge_count_.load(std::memory_order_acquire);
  if (info.vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    info.average_degree = 2.0 * static_cast<double>(info.edge_count) / info.vertex_count;
  }
  info.memory_res = utils::GetMemoryRES();
  info.disk_usage = GetDiskSpaceUsage();
  return info;
}

StorageInfo DiskStorage::GetInfo(memgraph::replication_coordination_glue::ReplicationRole replication_role) {
  StorageInfo info = GetBaseInfo();
  {
    auto access = Access(replication_role);
    const auto &lbl = access->ListAllIndices();
    info.label_indices = lbl.label.size();
    info.label_property_indices = lbl.label_property.size();
    info.text_indices = lbl.text_indices.size();
    const auto &con = access->ListAllConstraints();
    info.existence_constraints = con.existence.size();
    info.unique_constraints = con.unique.size();
  }
  info.storage_mode = storage_mode_;
  info.isolation_level = isolation_level_;
  info.durability_snapshot_enabled =
      config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit;
  info.durability_wal_enabled =
      config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  return info;
}

void DiskStorage::SetEdgeImportMode(EdgeImportMode edge_import_status) {
  std::unique_lock main_guard{main_lock_};
  if (edge_import_status == edge_import_status_) {
    return;
  }
  if (edge_import_status == EdgeImportMode::ACTIVE) {
    edge_import_mode_cache_ = std::make_unique<EdgeImportModeCache>(config_);
  } else {
    edge_import_mode_cache_.reset(nullptr);
  }

  edge_import_status_ = edge_import_status;
  spdlog::trace("Edge import mode changed to: {}", EdgeImportModeToString(edge_import_status));
}

EdgeImportMode DiskStorage::GetEdgeImportMode() const {
  std::shared_lock storage_guard_(main_lock_);
  return edge_import_status_;
}

VertexAccessor DiskStorage::DiskAccessor::CreateVertex() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto gid = disk_storage->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = transaction_.vertices_->access();

  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");

  if (delta) {
    delta->prev.Set(&*it);
  }

  disk_storage->vertex_count_.fetch_add(1, std::memory_order_acq_rel);
  return {&*it, storage_, &transaction_};
}

std::optional<VertexAccessor> DiskStorage::DiskAccessor::FindVertex(storage::Gid gid, View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  return disk_storage->FindVertex(gid, &transaction_, view);
}

std::optional<EdgeAccessor> DiskStorage::DiskAccessor::FindEdge(storage::Gid gid, View view) {
  throw utils::NotYetImplemented("Id based lookup for on-disk storage mode is not yet implemented on edges.");
}

Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
DiskStorage::DiskAccessor::DetachDelete(std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges,
                                        bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

  /// TODO: (andi) Refactor
  auto maybe_result = Storage::Accessor::DetachDelete(nodes, edges, detach);
  if (maybe_result.HasError()) {
    return maybe_result.GetError();
  }

  auto value = maybe_result.GetValue();
  if (!value) {
    return std::make_optional<ReturnType>();
  }
  auto &[deleted_vertices, deleted_edges] = *value;

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  for (const auto &vertex : deleted_vertices) {
    transaction_.vertices_to_delete_.emplace(vertex.vertex_->gid.ToString(), utils::SerializeVertex(*vertex.vertex_));
    transaction_.manyDeltasCache.Invalidate(vertex.vertex_);
    disk_storage->vertex_count_.fetch_sub(1, std::memory_order_acq_rel);
  }

  for (const auto &edge : deleted_edges) {
    const std::string ser_edge_gid = edge.Gid().ToString();
    const auto src_vertex_gid = edge.from_vertex_->gid.ToString();
    const auto dst_vertex_gid = edge.to_vertex_->gid.ToString();
    transaction_.edges_to_delete_.emplace(ser_edge_gid, std::make_pair(src_vertex_gid, dst_vertex_gid));

    transaction_.manyDeltasCache.Invalidate(edge.from_vertex_, edge.edge_type_, EdgeDirection::OUT);
    transaction_.manyDeltasCache.Invalidate(edge.to_vertex_, edge.edge_type_, EdgeDirection::IN);
    transaction_.RemoveModifiedEdge(edge.Gid());
  }

  return maybe_result;
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                           EdgeTypeId edge_type) {
  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  if (from_vertex->deleted() || to_vertex->deleted()) return Error::DELETED_OBJECT;

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto gid = storage::Gid::FromUint(disk_storage->edge_id_.fetch_add(1, std::memory_order_acq_rel));
  EdgeRef edge(gid);
  bool edge_import_mode_active = disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE;

  if (storage_->config_.salient.items.properties_on_edges) {
    auto acc = edge_import_mode_active ? disk_storage->edge_import_mode_cache_->AccessToEdges()
                                       : transaction_.edges_->access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }

  ModifiedEdgeInfo modified_edge(Delta::Action::DELETE_OBJECT, from_vertex->gid, to_vertex->gid, edge_type, edge);
  /// TODO: (andi) Change when decoupled edge creation from edge deletion.
  transaction_.AddModifiedEdge(gid, modified_edge);

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);

  transaction_.manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
  transaction_.manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);

  if (storage_->config_.salient.items.enable_schema_metadata) {
    storage_->stored_edge_types_.try_insert(edge_type);
  }
  storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, storage_, &transaction_);
}

std::optional<EdgeAccessor> DiskStorage::DiskAccessor::FindEdge(Gid gid, View view, EdgeTypeId edge_type,
                                                                VertexAccessor *from_vertex,
                                                                VertexAccessor *to_vertex) {
  auto res = FindEdges(view, edge_type, from_vertex, to_vertex);
  if (res.HasError()) return std::nullopt;  // TODO: use a Result type

  auto const it = std::ranges::find_if(
      res->edges, [gid](EdgeAccessor const &edge_accessor) { return edge_accessor.edge_.ptr->gid == gid; });

  if (it == res->edges.end()) return std::nullopt;  // TODO: use a Result type

  return *it;
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::EdgeSetFrom(EdgeAccessor * /*edge*/, VertexAccessor * /*new_from*/) {
  MG_ASSERT(false, "EdgeSetFrom is currently only implemented for InMemory storage");
  return Error::NONEXISTENT_OBJECT;
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::EdgeSetTo(EdgeAccessor * /*edge*/, VertexAccessor * /*new_to*/) {
  MG_ASSERT(false, "EdgeSetTo is currently only implemented for InMemory storage");
  return Error::NONEXISTENT_OBJECT;
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::EdgeChangeType(EdgeAccessor * /*edge*/, EdgeTypeId /*new_edge_type*/) {
  MG_ASSERT(false, "EdgeChangeType is currently only implemented for InMemory storage");
  return Error::NONEXISTENT_OBJECT;
}

bool DiskStorage::WriteVertexToVertexColumnFamily(Transaction *transaction, const Vertex &vertex) {
  MG_ASSERT(transaction->commit_timestamp, "Writing vertex to disk but commit timestamp not set.");
  auto commit_ts = transaction->commit_timestamp->load(std::memory_order_relaxed);
  const auto ser_vertex = utils::SerializeVertex(vertex);
  auto status = transaction->disk_transaction_->Put(kvstore_->vertex_chandle, ser_vertex,
                                                    utils::SerializeProperties(vertex.properties));
  if (status.ok()) {
    spdlog::trace("rocksdb: Saved vertex with key {} and ts {} to vertex column family", ser_vertex, commit_ts);
    return true;
  }
  spdlog::error("rocksdb: Failed to save vertex with key {} and ts {} to vertex column family", ser_vertex, commit_ts);
  return false;
}

bool DiskStorage::WriteEdgeToEdgeColumnFamily(Transaction *transaction, const std::string &serialized_edge_key,
                                              const std::string &serialized_edge_value) {
  MG_ASSERT(transaction->commit_timestamp, "Writing edge to disk but commit timestamp not set.");
  auto commit_ts = transaction->commit_timestamp->load(std::memory_order_relaxed);
  rocksdb::Status status =
      transaction->disk_transaction_->Put(kvstore_->edge_chandle, serialized_edge_key, serialized_edge_value);

  if (status.ok()) {
    spdlog::trace("rocksdb: Saved edge {} with ts {} to edge column family", serialized_edge_key, commit_ts);
    return true;
  }
  spdlog::error("rocksdb: Failed to save edge {} with ts {} to edge column family", serialized_edge_key, commit_ts);
  return false;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool DiskStorage::WriteEdgeToConnectivityIndex(Transaction *transaction, const std::string &vertex_gid,
                                               const std::string &edge_gid, rocksdb::ColumnFamilyHandle *handle,
                                               std::string mode) {
  MG_ASSERT(transaction->commit_timestamp, "Writing edge to disk but commit timestamp not set.");
  std::string value;
  const auto put_status = std::invoke([transaction, handle, &value, &vertex_gid, &edge_gid]() {
    rocksdb::ReadOptions ro;
    std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
    rocksdb::Slice ts(strTs);
    ro.timestamp = &ts;

    if (transaction->disk_transaction_->Get(ro, handle, vertex_gid, &value).IsNotFound()) {
      return transaction->disk_transaction_->Put(handle, vertex_gid, edge_gid);
    }
    return transaction->disk_transaction_->Put(handle, vertex_gid, value + "," + edge_gid);
  });

  if (put_status.ok()) {
    spdlog::trace("rocksdb: Saved edge {} to {} edges connectivity index for vertex {}", edge_gid, mode, vertex_gid);
    return true;
  }

  spdlog::error("rocksdb: Failed to save edge {} to {} edges connectivity index for vertex {}", edge_gid, mode,
                vertex_gid);
  return false;
}

bool DiskStorage::DeleteVertexFromDisk(Transaction *transaction, const std::string &vertex_gid,
                                       const std::string &vertex) {
  /// TODO: (andi) This should be atomic delete.
  auto vertex_del_status = transaction->disk_transaction_->Delete(kvstore_->vertex_chandle, vertex);
  auto vertex_out_conn_status = transaction->disk_transaction_->Delete(kvstore_->out_edges_chandle, vertex_gid);
  auto vertex_in_conn_status = transaction->disk_transaction_->Delete(kvstore_->in_edges_chandle, vertex_gid);

  if (vertex_del_status.ok() && vertex_out_conn_status.ok() && vertex_in_conn_status.ok()) {
    spdlog::trace("rocksdb: Deleted vertex with key {}", vertex);
    return true;
  }
  spdlog::error("rocksdb: Failed to delete vertex with key {}", vertex);
  return false;
}

bool DiskStorage::DeleteEdgeFromEdgeColumnFamily(Transaction *transaction, const std::string &edge_gid) {
  if (!transaction->disk_transaction_->Delete(kvstore_->edge_chandle, edge_gid).ok()) {
    spdlog::error("rocksdb: Failed to delete edge {}", edge_gid);
    return false;
  }
  spdlog::trace("rocksdb: Deleted edge from edge column family", edge_gid);
  return true;
}

/// TODO: (andi) This is currently not optimal as it will for each edge deserialize all neighborhood edges
/// and then remove the edge from the neighborhood edges. This can be optimized by saving vertices together with
// deleted edges and then modifying the deletion procedure. This is currently bad if we have some supernode.
bool DiskStorage::DeleteEdgeFromDisk(Transaction *transaction, const std::string &edge_gid,
                                     const std::string &src_vertex_gid, const std::string &dst_vertex_gid) {
  /// TODO: (andi) Should be atomic deletion.
  if (!DeleteEdgeFromEdgeColumnFamily(transaction, edge_gid)) {
    return false;
  }

  if (!transaction->vertices_to_delete_.contains(src_vertex_gid)) {
    if (!DeleteEdgeFromConnectivityIndex(transaction, src_vertex_gid, edge_gid, kvstore_->out_edges_chandle, "OUT")) {
      spdlog::error("rocksdb: Failed to delete edge with key {}", edge_gid);
      return false;
    }
    spdlog::trace("rocksdb: Deleted edge with key {} from out edges of vertex", edge_gid, src_vertex_gid);
  }
  if (!transaction->vertices_to_delete_.contains(dst_vertex_gid)) {
    if (!DeleteEdgeFromConnectivityIndex(transaction, dst_vertex_gid, edge_gid, kvstore_->in_edges_chandle, "IN")) {
      spdlog::error("rocksdb: Failed to delete edge with key {}", edge_gid);
      return false;
    }
    spdlog::trace("rocksdb: Deleted edge with key {} from in edges of vertex", edge_gid, dst_vertex_gid);
  }

  return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool DiskStorage::DeleteEdgeFromConnectivityIndex(Transaction *transaction, const std::string &vertex_gid,
                                                  const std::string &edge_gid, rocksdb::ColumnFamilyHandle *handle,
                                                  std::string mode) {
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;

  std::string edges;
  auto edges_status = transaction->disk_transaction_->Get(ro, handle, vertex_gid, &edges);
  if (!edges_status.ok()) {
    /// NOTE: Edge could be created and deleted in the same txn, so no need to fail explicitly.
    spdlog::error("rocksdb: Failed to find {} edges collection of vertex {}.", mode, vertex_gid);
    return true;
  }

  std::vector<std::string> edges_vec = utils::Split(edges, ",");
  MG_ASSERT(std::erase(edges_vec, edge_gid) > 0U, "Edge must be in the edges collection of vertex");
  if (edges_vec.empty()) {
    if (!transaction->disk_transaction_->Delete(handle, vertex_gid).ok()) {
      spdlog::error("rocksdb: Failed to delete edge {} from edges connectivity index for vertex {}", edge_gid,
                    vertex_gid);
      return false;
    }
    return true;
  }

  if (!transaction->disk_transaction_->Put(handle, vertex_gid, utils::Join(edges_vec, ",")).ok()) {
    spdlog::error("rocksdb: Failed to delete edge {} from edges connectivity index for vertex {}", edge_gid,
                  vertex_gid);
    return false;
  }
  return true;
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::CheckVertexConstraintsBeforeCommit(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  if (auto existence_constraint_validation_result = constraints_.existence_constraints_->Validate(vertex);
      existence_constraint_validation_result.has_value()) {
    return StorageManipulationError{existence_constraint_validation_result.value()};
  }

  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
  if (auto unique_constraint_validation_result = disk_unique_constraints->Validate(vertex, unique_storage);
      unique_constraint_validation_result.has_value()) {
    return StorageManipulationError{unique_constraint_validation_result.value()};
  }
  return {};
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::FlushVertices(
    Transaction *transaction, const auto &vertex_acc, std::vector<std::vector<PropertyValue>> &unique_storage) {
  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());

  auto commit_ts = transaction->commit_timestamp->load(std::memory_order_relaxed);
  for (const Vertex &vertex : vertex_acc) {
    if (!VertexNeedsToBeSerialized(vertex)) {
      continue;
    }
    if (auto check_result = CheckVertexConstraintsBeforeCommit(vertex, unique_storage); check_result.HasError()) {
      return check_result.GetError();
    }

    if (vertex.deleted()) {
      continue;
    }

    /// NOTE: this deletion has to come before writing, otherwise RocksDB thinks that all entries are deleted
    if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta()); maybe_old_disk_key.has_value()) {
      if (!DeleteVertexFromDisk(transaction, vertex.gid.ToString(), maybe_old_disk_key.value())) {
        return StorageManipulationError{SerializationError{}};
      }
    }

    if (!WriteVertexToVertexColumnFamily(transaction, vertex)) {
      return StorageManipulationError{SerializationError{}};
    }

    if (!disk_unique_constraints->SyncVertexToUniqueConstraintsStorage(vertex, commit_ts) ||
        !disk_label_index->SyncVertexToLabelIndexStorage(vertex, commit_ts) ||
        !disk_label_property_index->SyncVertexToLabelPropertyIndexStorage(vertex, commit_ts)) {
      return StorageManipulationError{SerializationError{}};
    }
  }

  return {};
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::ClearDanglingVertices(
    Transaction *transaction) {
  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());

  auto commit_ts = transaction->commit_timestamp->load(std::memory_order_relaxed);
  if (!disk_unique_constraints->DeleteVerticesWithRemovedConstraintLabel(transaction->start_timestamp, commit_ts) ||
      !disk_label_index->DeleteVerticesWithRemovedIndexingLabel(transaction->start_timestamp, commit_ts) ||
      !disk_label_property_index->DeleteVerticesWithRemovedIndexingLabel(transaction->start_timestamp, commit_ts)) {
    return StorageManipulationError{SerializationError{}};
  }
  return {};
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::FlushIndexCache(
    Transaction *transaction) {
  std::vector<std::vector<PropertyValue>> unique_storage;

  for (const auto &vec : transaction->index_storage_) {
    if (auto vertices_res = FlushVertices(transaction, vec->access(), unique_storage); vertices_res.HasError()) {
      return vertices_res.GetError();
    }
  }

  return {};
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::FlushDeletedVertices(
    Transaction *transaction) {
  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());

  auto commit_ts = transaction->commit_timestamp->load(std::memory_order_relaxed);
  for (const auto &[vertex_gid, serialized_vertex_to_delete] : transaction->vertices_to_delete_) {
    if (!DeleteVertexFromDisk(transaction, vertex_gid, serialized_vertex_to_delete) ||
        !disk_unique_constraints->ClearDeletedVertex(vertex_gid, commit_ts) ||
        !disk_label_index->ClearDeletedVertex(vertex_gid, commit_ts) ||
        !disk_label_property_index->ClearDeletedVertex(vertex_gid, commit_ts)) {
      return StorageManipulationError{SerializationError{}};
    }
  }

  return {};
}

[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::FlushDeletedEdges(
    Transaction *transaction) {
  for (const auto &[edge_to_delete, vertices] : transaction->edges_to_delete_) {
    const auto &[src_vertex_id, dst_vertex_id] = vertices;
    if (!DeleteEdgeFromDisk(transaction, edge_to_delete, src_vertex_id, dst_vertex_id)) {
      return StorageManipulationError{SerializationError{}};
    }
  }
  return {};
}

/// TODO: (andi) It would be much better that all operations related to edges are done based on modified src and
/// dest_vertex. Otherwise we will be doing a lot of unnecessary deserializations of neighborhood.
/// std::map<src_vertex_gid, ...>
/// std::map<dst_vertex_gid, ...>
/// Here we also do flushing of too many things, we don't need to serialize edges in read-only txn, check that...
[[nodiscard]] utils::BasicResult<StorageManipulationError, void> DiskStorage::FlushModifiedEdges(
    Transaction *transaction, const auto &edges_acc) {
  for (const auto &modified_edge : transaction->modified_edges_) {
    const std::string edge_gid = modified_edge.first.ToString();
    const Delta::Action root_action = modified_edge.second.delta_action;
    const auto src_vertex_gid = modified_edge.second.src_vertex_gid.ToString();
    const auto dst_vertex_gid = modified_edge.second.dest_vertex_gid.ToString();

    if (!config_.salient.items.properties_on_edges) {
      /// If the object was created then flush it, otherwise since properties on edges are false
      /// edge wasn't modified for sure.
      if (root_action == Delta::Action::DELETE_OBJECT &&
          !WriteEdgeToEdgeColumnFamily(
              transaction, edge_gid,
              utils::SerializeEdgeAsValue(src_vertex_gid, dst_vertex_gid, modified_edge.second.edge_type_id))) {
        return StorageManipulationError{SerializationError{}};
      }
    } else {
      // If the delta is DELETE_OBJECT, the edge is just created so there is nothing to delete.
      // If the edge was deserialized, only properties can be modified -> key stays the same as when deserialized
      // so we can delete it.
      // This is done to avoid storing multiple versions of the same data.
      if (root_action == Delta::Action::DELETE_DESERIALIZED_OBJECT &&
          !DeleteEdgeFromEdgeColumnFamily(transaction, edge_gid)) {
        return StorageManipulationError{SerializationError{}};
      }

      const auto &edge = edges_acc.find(modified_edge.first);
      MG_ASSERT(edge != edges_acc.end(),
                "Database in invalid state, commit not possible! Please restart your DB and start the import again.");

      /// TODO: (andi) I think this is not wrong but it would be better to use AtomicWrites across column families.
      if (!WriteEdgeToEdgeColumnFamily(
              transaction, edge_gid,
              utils::SerializeEdgeAsValue(src_vertex_gid, dst_vertex_gid, modified_edge.second.edge_type_id, &*edge))) {
        return StorageManipulationError{SerializationError{}};
      }
    }
    if (root_action == Delta::Action::DELETE_OBJECT &&
        (!WriteEdgeToConnectivityIndex(transaction, src_vertex_gid, edge_gid, kvstore_->out_edges_chandle, "OUT") ||
         !WriteEdgeToConnectivityIndex(transaction, dst_vertex_gid, edge_gid, kvstore_->in_edges_chandle, "IN"))) {
      return StorageManipulationError{SerializationError{}};
    }
  }
  return {};
}

/// NOTE: This will create Delta object which will cause deletion of old key entry on the disk
std::optional<storage::VertexAccessor> DiskStorage::LoadVertexToMainMemoryCache(Transaction *transaction,
                                                                                const std::string &key,
                                                                                const std::string &value,
                                                                                std::string &&ts) {
  auto main_storage_accessor = transaction->vertices_->access();

  storage::Gid gid = Gid::FromString(utils::ExtractGidFromKey(key));
  if (ObjectExistsInCache(main_storage_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromMainDiskStorage(key)};
  PropertyStore properties{utils::DeserializePropertiesFromMainDiskStorage(value)};
  return CreateVertexFromDisk(transaction, main_storage_accessor, gid, std::move(labels_id), std::move(properties),
                              CreateDeleteDeserializedObjectDelta(transaction, key, std::move(ts)));
}

VertexAccessor DiskStorage::CreateVertexFromDisk(Transaction *transaction, utils::SkipList<Vertex>::Accessor &accessor,
                                                 storage::Gid gid, std::vector<LabelId> label_ids,
                                                 PropertyStore properties, Delta *delta) {
  auto [it, inserted] = accessor.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != accessor.end(), "Invalid Vertex accessor!");
  it->labels = std::move(label_ids);
  it->properties = std::move(properties);
  delta->prev.Set(&*it);
  return {&*it, this, transaction};
}

std::optional<VertexAccessor> DiskStorage::FindVertex(storage::Gid gid, Transaction *transaction, View view) {
  auto acc = edge_import_status_ == EdgeImportMode::ACTIVE ? edge_import_mode_cache_->AccessToVertices()
                                                           : transaction->vertices_->access();
  auto vertex_it = acc.find(gid);
  if (vertex_it != acc.end()) {
    return VertexAccessor::Create(&*vertex_it, this, transaction, view);
  }
  for (const auto &vec : transaction->index_storage_) {
    acc = vec->access();
    auto index_it = acc.find(gid);
    if (index_it != acc.end()) {
      return VertexAccessor::Create(&*index_it, this, transaction, view);
    }
  }

  rocksdb::ReadOptions read_opts;
  auto strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  read_opts.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(
      transaction->disk_transaction_->GetIterator(read_opts, kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (Gid::FromString(utils::ExtractGidFromKey(key)) == gid) {
      // We should pass it->timestamp().ToString() instead of "0"
      // This is hack until RocksDB will support timestamp() in WBWI iterator
      return LoadVertexToMainMemoryCache(transaction, key, it->value().ToString(), kDeserializeTimestamp);
    }
  }
  return std::nullopt;
}

std::optional<EdgeAccessor> DiskStorage::CreateEdgeFromDisk(const VertexAccessor *from, const VertexAccessor *to,
                                                            Transaction *transaction, EdgeTypeId edge_type,
                                                            storage::Gid gid, const std::string_view properties,
                                                            const std::string &old_disk_key, std::string &&read_ts) {
  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  bool edge_import_mode_active = edge_import_status_ == EdgeImportMode::ACTIVE;

  if (edge_import_mode_active) {
    auto import_mode_edge_cache_acc = edge_import_mode_cache_->AccessToEdges();
    if (auto it = import_mode_edge_cache_acc.find(gid); it != import_mode_edge_cache_acc.end()) {
      return EdgeAccessor(EdgeRef(&*it), edge_type, from_vertex, to_vertex, this, transaction);
    }
  }

  EdgeRef edge(gid);
  if (config_.salient.items.properties_on_edges) {
    auto acc = edge_import_mode_active ? edge_import_mode_cache_->AccessToEdges() : transaction->edges_->access();
    auto *delta = CreateDeleteDeserializedObjectDelta(transaction, old_disk_key, std::move(read_ts));
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
    edge.ptr->properties.SetBuffer(properties);
  }

  ModifiedEdgeInfo modified_edge(Delta::Action::DELETE_DESERIALIZED_OBJECT, from_vertex->gid, to_vertex->gid, edge_type,
                                 edge);
  if (transaction->AddModifiedEdge(gid, modified_edge)) {
    spdlog::trace("Edge {} added to out edges of vertex with gid {}", gid.ToString(), from_vertex->gid.AsUint());
    spdlog::trace("Edge {} added to in edges of vertex with gid {}", gid.ToString(), to_vertex->gid.AsUint());
    from_vertex->out_edges.emplace_back(edge_type, to_vertex, edge);
    to_vertex->in_edges.emplace_back(edge_type, from_vertex, edge);
    transaction->manyDeltasCache.Invalidate(from_vertex, edge_type, EdgeDirection::OUT);
    transaction->manyDeltasCache.Invalidate(to_vertex, edge_type, EdgeDirection::IN);
  }

  return EdgeAccessor(edge, edge_type, from_vertex, to_vertex, this, transaction);
}

std::vector<EdgeAccessor> DiskStorage::OutEdges(const VertexAccessor *src_vertex,
                                                const std::vector<EdgeTypeId> &edge_types,
                                                const VertexAccessor *destination, Transaction *transaction,
                                                View view) {
  /// Check whether the vertex is deleted in the current tx only if View::NEW is requested
  if (view == View::NEW && src_vertex->vertex_->deleted()) return {};

  const std::string src_vertex_gid = src_vertex->Gid().ToString();
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;

  std::string out_edges_str;
  auto conn_index_res =
      transaction->disk_transaction_->Get(ro, kvstore_->out_edges_chandle, src_vertex_gid, &out_edges_str);

  if (!conn_index_res.ok()) {
    spdlog::trace("rocksdb: Couldn't find out edges of vertex {}.", src_vertex_gid);
    return {};
  }

  std::vector<EdgeAccessor> result;
  auto out_edges = utils::Split(out_edges_str, ",");
  for (const std::string &edge_gid_str : out_edges) {
    std::string edge_val_str;
    auto edge_res = transaction->disk_transaction_->Get(ro, kvstore_->edge_chandle, edge_gid_str, &edge_val_str);

    MG_ASSERT(edge_res.ok(), "rocksdb: Failed to find edge with gid {} in edge column family", edge_gid_str);

    auto edge_type_id = utils::ExtractEdgeTypeIdFromEdgeValue(edge_val_str);
    if (!edge_types.empty() && !utils::Contains(edge_types, edge_type_id)) continue;

    auto edge_gid = Gid::FromString(edge_gid_str);
    auto properties_str =
        config_.salient.items.properties_on_edges ? utils::GetPropertiesFromEdgeValue(edge_val_str) : "";

    const auto edge = std::invoke([this, destination, &edge_val_str, transaction, view, src_vertex, edge_type_id,
                                   edge_gid, &properties_str, &edge_gid_str]() {
      auto dst_vertex_gid = utils::ExtractDstVertexGidFromEdgeValue(edge_val_str);
      if (!destination) {
        auto dst_vertex = FindVertex(dst_vertex_gid, transaction, view);
        /// TODO: (andi) I think dst_vertex->deleted() should be unnecessary
        /// Check whether the vertex is deleted in the current tx only if View::NEW is requested
        if (!dst_vertex.has_value() || (view == View::NEW && dst_vertex->vertex_->deleted()))
          return std::optional<EdgeAccessor>{};

        return CreateEdgeFromDisk(src_vertex, &*dst_vertex, transaction, edge_type_id, edge_gid, properties_str,
                                  edge_gid_str, kDeserializeTimestamp);
      }
      /// This is needed for filtering
      /// Second check not needed I think
      if (dst_vertex_gid != destination->Gid() || destination->vertex_->deleted()) {
        return std::optional<EdgeAccessor>{};
      }

      return CreateEdgeFromDisk(src_vertex, destination, transaction, edge_type_id, edge_gid, properties_str,
                                edge_gid_str, kDeserializeTimestamp);
    });
    if (edge.has_value()) result.emplace_back(*edge);
  }

  return result;
}

std::vector<EdgeAccessor> DiskStorage::InEdges(const VertexAccessor *dst_vertex,
                                               const std::vector<EdgeTypeId> &edge_types, const VertexAccessor *source,
                                               Transaction *transaction, View view) {
  /// Check whether the vertex is deleted in the current tx only if View::NEW is requested
  if (view == View::NEW && dst_vertex->vertex_->deleted()) return {};

  const std::string dst_vertex_gid = dst_vertex->Gid().ToString();
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction->start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;

  std::string in_edges_str;
  auto conn_index_res =
      transaction->disk_transaction_->Get(ro, kvstore_->in_edges_chandle, dst_vertex_gid, &in_edges_str);

  if (!conn_index_res.ok()) {
    spdlog::trace("rocksdb: Couldn't find in edges of vertex {}.", dst_vertex_gid);
    return {};
  }

  auto in_edges = utils::Split(in_edges_str, ",");
  std::vector<EdgeAccessor> result;
  for (const std::string &edge_gid_str : in_edges) {
    std::string edge_val_str;
    auto edge_res = transaction->disk_transaction_->Get(ro, kvstore_->edge_chandle, edge_gid_str, &edge_val_str);

    MG_ASSERT(edge_res.ok(), "rocksdb: Failed to find edge with gid {} in edge column family", edge_gid_str);

    auto edge_type_id = utils::ExtractEdgeTypeIdFromEdgeValue(edge_val_str);
    if (!edge_types.empty() && !utils::Contains(edge_types, edge_type_id)) continue;

    auto edge_gid = Gid::FromString(edge_gid_str);
    auto properties_str = utils::GetPropertiesFromEdgeValue(edge_val_str);

    const auto edge = std::invoke([this, source, &edge_val_str, transaction, view, dst_vertex, edge_type_id, edge_gid,
                                   &properties_str, &edge_gid_str]() {
      auto src_vertex_gid = utils::ExtractSrcVertexGidFromEdgeValue(edge_val_str);
      if (!source) {
        auto src_vertex = FindVertex(src_vertex_gid, transaction, view);
        /// Check whether the vertex is deleted in the current tx only if View::NEW is requested
        /// TODO: (andi) Second check I think isn't necessary
        if (!src_vertex.has_value() || (view == View::NEW && src_vertex->vertex_->deleted()))
          return std::optional<EdgeAccessor>{};

        return CreateEdgeFromDisk(&*src_vertex, dst_vertex, transaction, edge_type_id, edge_gid, properties_str,
                                  edge_gid_str, kDeserializeTimestamp);
      }
      /// TODO: (andi) 2nd check not needed I think
      if (src_vertex_gid != source->Gid() || source->vertex_->deleted()) {
        return std::optional<EdgeAccessor>{};
      }
      return CreateEdgeFromDisk(source, dst_vertex, transaction, edge_type_id, edge_gid, properties_str, edge_gid_str,
                                kDeserializeTimestamp);
    });

    if (edge.has_value()) result.emplace_back(*edge);
  }

  return result;
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

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageManipulationError, void> DiskStorage::DiskAccessor::Commit(
    CommitReplArgs reparg, DatabaseAccessProtector /*db_acc*/) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  bool edge_import_mode_active = disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE;

  if (!transaction_.md_deltas.empty()) {
    // This is usually done by the MVCC, but it does not handle the metadata deltas
    transaction_.EnsureCommitTimestampExists();
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    commit_timestamp_.emplace(disk_storage->CommitTimestamp(reparg.desired_commit_timestamp));
    transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);

    for (const auto &md_delta : transaction_.md_deltas) {
      switch (md_delta.action) {
        case MetadataDelta::Action::LABEL_INDEX_CREATE: {
          if (!disk_storage->durable_metadata_.PersistLabelIndexCreation(md_delta.label)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::LABEL_PROPERTY_INDEX_CREATE: {
          const auto &info = md_delta.label_property;
          if (!disk_storage->durable_metadata_.PersistLabelPropertyIndexAndExistenceConstraintCreation(
                  info.label, info.property, kLabelPropertyIndexStr)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::EDGE_INDEX_CREATE: {
          throw utils::NotYetImplemented("Edge-type indexing is not yet implemented on on-disk storage mode.");
        }
        case MetadataDelta::Action::LABEL_INDEX_DROP: {
          if (!disk_storage->durable_metadata_.PersistLabelIndexDeletion(md_delta.label)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::LABEL_PROPERTY_INDEX_DROP: {
          const auto &info = md_delta.label_property;
          if (!disk_storage->durable_metadata_.PersistLabelPropertyIndexAndExistenceConstraintDeletion(
                  info.label, info.property, kLabelPropertyIndexStr)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::EDGE_INDEX_DROP: {
          throw utils::NotYetImplemented("Edge-type indexing is not yet implemented on on-disk storage mode.");
        }
        case MetadataDelta::Action::LABEL_INDEX_STATS_SET: {
          throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
        } break;
        case MetadataDelta::Action::LABEL_INDEX_STATS_CLEAR: {
          throw utils::NotYetImplemented("ClearIndexStats(stats) is not implemented for DiskStorage.");
        } break;
        case MetadataDelta::Action::LABEL_PROPERTY_INDEX_STATS_SET: {
          throw utils::NotYetImplemented("SetIndexStats(stats) is not implemented for DiskStorage.");
        } break;
        case MetadataDelta::Action::LABEL_PROPERTY_INDEX_STATS_CLEAR: {
          throw utils::NotYetImplemented("ClearIndexStats(stats) is not implemented for DiskStorage.");
        } break;
        case MetadataDelta::Action::TEXT_INDEX_CREATE: {
          const auto &info = md_delta.text_index;
          if (!disk_storage->durable_metadata_.PersistTextIndexCreation(info.index_name, info.label)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::TEXT_INDEX_DROP: {
          const auto &info = md_delta.text_index;
          if (!disk_storage->durable_metadata_.PersistTextIndexDeletion(info.index_name, info.label)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::EXISTENCE_CONSTRAINT_CREATE: {
          const auto &info = md_delta.label_property;
          if (!disk_storage->durable_metadata_.PersistLabelPropertyIndexAndExistenceConstraintCreation(
                  info.label, info.property, kExistenceConstraintsStr)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::EXISTENCE_CONSTRAINT_DROP: {
          const auto &info = md_delta.label_property;
          if (!disk_storage->durable_metadata_.PersistLabelPropertyIndexAndExistenceConstraintDeletion(
                  info.label, info.property, kExistenceConstraintsStr)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::UNIQUE_CONSTRAINT_CREATE: {
          const auto &info = md_delta.label_properties;
          if (!disk_storage->durable_metadata_.PersistUniqueConstraintCreation(info.label, info.properties)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
        case MetadataDelta::Action::UNIQUE_CONSTRAINT_DROP: {
          const auto &info = md_delta.label_properties;
          if (!disk_storage->durable_metadata_.PersistUniqueConstraintDeletion(info.label, info.properties)) {
            return StorageManipulationError{PersistenceError{}};
          }
        } break;
      }
    }
  } else if (transaction_.deltas.empty() ||
             (!edge_import_mode_active &&
              std::all_of(transaction_.deltas.begin(), transaction_.deltas.end(), [](const Delta &delta) {
                return delta.action == Delta::Action::DELETE_DESERIALIZED_OBJECT;
              }))) {
  } else {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    commit_timestamp_.emplace(disk_storage->CommitTimestamp(reparg.desired_commit_timestamp));
    transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);

    if (edge_import_mode_active) {
      auto edges_acc = disk_storage->edge_import_mode_cache_->AccessToEdges();
      if (auto res = disk_storage->FlushModifiedEdges(&transaction_, edges_acc); res.HasError()) {
        Abort();
        return res;
      }
      if (auto del_edges_res = disk_storage->FlushDeletedEdges(&transaction_); del_edges_res.HasError()) {
        Abort();
        return del_edges_res.GetError();
      }
    } else {
      std::vector<std::vector<PropertyValue>> unique_storage;
      if (auto vertices_flush_res =
              disk_storage->FlushVertices(&transaction_, transaction_.vertices_->access(), unique_storage);
          vertices_flush_res.HasError()) {
        Abort();
        return vertices_flush_res.GetError();
      }

      if (auto del_vertices_res = disk_storage->FlushDeletedVertices(&transaction_); del_vertices_res.HasError()) {
        Abort();
        return del_vertices_res.GetError();
      }

      auto tx_edges_acc = transaction_.edges_->access();
      if (auto modified_edges_res = disk_storage->FlushModifiedEdges(&transaction_, tx_edges_acc);
          modified_edges_res.HasError()) {
        Abort();
        return modified_edges_res.GetError();
      }

      if (auto del_edges_res = disk_storage->FlushDeletedEdges(&transaction_); del_edges_res.HasError()) {
        Abort();
        return del_edges_res.GetError();
      }

      if (auto clear_dangling_res = disk_storage->ClearDanglingVertices(&transaction_); clear_dangling_res.HasError()) {
        Abort();
        return clear_dangling_res.GetError();
      }

      if (auto index_flush_res = disk_storage->FlushIndexCache(&transaction_); index_flush_res.HasError()) {
        Abort();
        return index_flush_res.GetError();
      }
    }
  }

  if (commit_timestamp_) {
    // commit_timestamp_ is set only if the transaction has writes.
    logging::AssertRocksDBStatus(transaction_.disk_transaction_->SetCommitTimestamp(*commit_timestamp_));
  }
  auto commitStatus = transaction_.disk_transaction_->Commit();
  if (!commitStatus.ok()) {
    Abort();
    spdlog::error("rocksdb: Commit failed with status {}", commitStatus.ToString());
    return StorageManipulationError{SerializationError{}};
  }

  delete transaction_.disk_transaction_;
  transaction_.disk_transaction_ = nullptr;

  spdlog::trace("rocksdb: Commit successful");
  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    disk_storage->indices_.text_index_.Commit();
  }
  disk_storage->durable_metadata_.UpdateMetaData(disk_storage->timestamp_, disk_storage->vertex_count_,
                                                 disk_storage->edge_count_);
  is_transaction_active_ = false;

  return {};
}

std::vector<std::pair<std::string, std::string>> DiskStorage::SerializeVerticesForLabelIndex(LabelId label) {
  std::vector<std::pair<std::string, std::string>> vertices_to_be_indexed;

  rocksdb::ReadOptions ro;
  auto strTs = utils::StringTimestamp(std::numeric_limits<uint64_t>::max());
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it = std::unique_ptr<rocksdb::Iterator>(kvstore_->db_->NewIterator(ro, kvstore_->vertex_chandle));

  const std::string serialized_label = label.ToString();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key_str = it->key().ToString();
    if (const std::vector<std::string> labels_str = utils::ExtractLabelsFromMainDiskStorage(key_str);
        utils::Contains(labels_str, serialized_label)) {
      std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(key_str);
      PropertyStore property_store = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToStringView());
      vertices_to_be_indexed.emplace_back(
          utils::SerializeVertexAsKeyForLabelIndex(label.ToString(), utils::ExtractGidFromMainDiskStorage(key_str)),
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

  const std::string serialized_label = label.ToString();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const std::string key_str = it->key().ToString();
    PropertyStore property_store = utils::DeserializePropertiesFromMainDiskStorage(it->value().ToString());
    if (const std::vector<std::string> labels_str = utils::ExtractLabelsFromMainDiskStorage(key_str);
        utils::Contains(labels_str, serialized_label) && property_store.HasProperty(property)) {
      std::vector<LabelId> labels = utils::DeserializeLabelsFromMainDiskStorage(key_str);
      vertices_to_be_indexed.emplace_back(
          utils::SerializeVertexAsKeyForLabelPropertyIndex(label.ToString(), property.ToString(),
                                                           utils::ExtractGidFromMainDiskStorage(key_str)),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, labels, property_store));
    }
  }
  return vertices_to_be_indexed;
}

void DiskStorage::DiskAccessor::UpdateObjectsCountOnAbort() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  uint64_t transaction_id = transaction_.transaction_id;

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
        Delta *current = vertex->delta();
        while (current != nullptr && current->timestamp->load(std::memory_order_acquire) == transaction_id) {
          switch (current->action) {
            case Delta::Action::DELETE_DESERIALIZED_OBJECT:
            case Delta::Action::DELETE_OBJECT: {
              disk_storage->vertex_count_.fetch_sub(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              disk_storage->vertex_count_.fetch_add(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE:
            case Delta::Action::REMOVE_LABEL:
            case Delta::Action::ADD_LABEL:
            case Delta::Action::SET_PROPERTY:
            case Delta::Action::ADD_IN_EDGE: {
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              storage_->edge_count_.fetch_add(1, std::memory_order_acq_rel);
              break;
            }
            case Delta::Action::REMOVE_OUT_EDGE: {
              storage_->edge_count_.fetch_sub(1, std::memory_order_acq_rel);
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
        }
        vertex->delta(current);
        if (current != nullptr) {
          current->prev.Set(vertex);
        }

        break;
      }
      case PreviousPtr::Type::EDGE:
      case PreviousPtr::Type::DELTA:
      case PreviousPtr::Type::NULLPTR:
        break;
    }
  }
}

void DiskStorage::DiskAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  // NOTE: On abort we need to delete disk transaction because after storage remove we couldn't remove
  // disk_transaction correctly in destructor.
  // This happens in tests when we create and remove storage in one test. For example, in
  // query_plan_accumulate_aggregate.cpp
  transaction_.disk_transaction_->Rollback();
  transaction_.disk_transaction_->ClearSnapshot();
  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    storage_->indices_.text_index_.Rollback();
  }
  delete transaction_.disk_transaction_;
  transaction_.disk_transaction_ = nullptr;
  is_transaction_active_ = false;
  UpdateObjectsCountOnAbort();
}

void DiskStorage::DiskAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    auto *disk_storage = static_cast<DiskStorage *>(storage_);

    if (disk_storage->edge_import_status_ == EdgeImportMode::ACTIVE) {
      auto &committed_transactions = disk_storage->edge_import_mode_cache_->GetCommittedTransactions();
      committed_transactions.WithLock(
          [&](auto &committed_txs) { committed_txs.emplace_back(std::move(transaction_)); });
    }
    commit_timestamp_.reset();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::CreateIndex(
    LabelId label, bool unique_access_needed) {
  if (unique_access_needed) {
    MG_ASSERT(unique_guard_.owns_lock(), "Create index requires unique access to the storage!");
  }
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_label_index = static_cast<DiskLabelIndex *>(on_disk->indices_.label_index_.get());
  if (!disk_label_index->CreateIndex(label, on_disk->SerializeVerticesForLabelIndex(label))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_create, label);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::CreateIndex(LabelId label,
                                                                                             PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Create index requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(on_disk->indices_.label_property_index_.get());
  if (!disk_label_property_index->CreateIndex(label, property,
                                              on_disk->SerializeVerticesForLabelPropertyIndex(label, property))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_create, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::CreateIndex(
    EdgeTypeId /*edge_type*/, bool /*unique_access_needed*/) {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::DropIndex(LabelId label) {
  MG_ASSERT(unique_guard_.owns_lock(), "Create index requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_label_index = static_cast<DiskLabelIndex *>(on_disk->indices_.label_index_.get());
  if (!disk_label_index->DropIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_index_drop, label);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::DropIndex(LabelId label,
                                                                                           PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Create index requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(on_disk->indices_.label_property_index_.get());
  if (!disk_label_property_index->DropIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::label_property_index_drop, label, property);
  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DiskAccessor::DropIndex(EdgeTypeId /*edge_type*/) {
  throw utils::NotYetImplemented(
      "Edge-type index related operations are not yet supported using on-disk storage mode.");
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void>
DiskStorage::DiskAccessor::CreateExistenceConstraint(LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Create existence constraint requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *existence_constraints = on_disk->constraints_.existence_constraints_.get();
  if (existence_constraints->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }
  if (auto check = on_disk->CheckExistingVerticesBeforeCreatingExistenceConstraint(label, property);
      check.has_value()) {
    return StorageExistenceConstraintDefinitionError{check.value()};
  }
  existence_constraints->InsertConstraint(label, property);
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_create, label, property);
  return {};
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void> DiskStorage::DiskAccessor::DropExistenceConstraint(
    LabelId label, PropertyId property) {
  MG_ASSERT(unique_guard_.owns_lock(), "Drop existence constraint requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *existence_constraints = on_disk->constraints_.existence_constraints_.get();
  if (!existence_constraints->DropConstraint(label, property)) {
    return StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::existence_constraint_drop, label, property);
  return {};
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
DiskStorage::DiskAccessor::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties) {
  MG_ASSERT(unique_guard_.owns_lock(), "Create unique constraint requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(on_disk->constraints_.unique_constraints_.get());
  if (auto constraint_check = disk_unique_constraints->CheckIfConstraintCanBeCreated(label, properties);
      constraint_check != UniqueConstraints::CreationStatus::SUCCESS) {
    return constraint_check;
  }
  auto check = on_disk->CheckExistingVerticesBeforeCreatingUniqueConstraint(label, properties);
  if (check.HasError()) {
    return StorageUniqueConstraintDefinitionError{check.GetError()};
  }
  if (!disk_unique_constraints->InsertConstraint(label, properties, check.GetValue())) {
    return StorageUniqueConstraintDefinitionError{ConstraintDefinitionError{}};
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_create, label, properties);
  return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus DiskStorage::DiskAccessor::DropUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties) {
  MG_ASSERT(unique_guard_.owns_lock(), "Drop unique constraint requires a unique access to the storage!");
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(on_disk->constraints_.unique_constraints_.get());
  if (auto ret = disk_unique_constraints->DropConstraint(label, properties);
      ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  transaction_.md_deltas.emplace_back(MetadataDelta::unique_constraint_create, label, properties);
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

void DiskStorage::DiskAccessor::DropGraph() {}

Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode,
                                           memgraph::replication_coordination_glue::ReplicationRole /*is_main*/) {
  /// We acquire the transaction engine lock here because we access (and
  /// modify) the transaction engine variables (`transaction_id` and
  /// `timestamp`) below.
  uint64_t transaction_id = 0;
  uint64_t start_timestamp = 0;
  bool edge_import_mode_active{false};
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    transaction_id = transaction_id_++;
    start_timestamp = timestamp_++;
    edge_import_mode_active = edge_import_status_ == EdgeImportMode::ACTIVE;
  }

  return {transaction_id, start_timestamp,         isolation_level,
          storage_mode,   edge_import_mode_active, !constraints_.empty()};
}

uint64_t DiskStorage::CommitTimestamp(const std::optional<uint64_t> desired_commit_timestamp) {
  if (!desired_commit_timestamp) {
    return timestamp_++;
  }
  timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
  return *desired_commit_timestamp;
}

std::unique_ptr<Storage::Accessor> DiskStorage::Access(
    memgraph::replication_coordination_glue::ReplicationRole /*replication_role*/,
    std::optional<IsolationLevel> override_isolation_level) {
  auto isolation_level = override_isolation_level.value_or(isolation_level_);
  if (isolation_level != IsolationLevel::SNAPSHOT_ISOLATION) {
    throw utils::NotYetImplemented("Disk storage supports only SNAPSHOT isolation level.");
  }
  return std::unique_ptr<DiskAccessor>(
      new DiskAccessor{Storage::Accessor::shared_access, this, isolation_level, storage_mode_});
}
std::unique_ptr<Storage::Accessor> DiskStorage::UniqueAccess(
    memgraph::replication_coordination_glue::ReplicationRole /*replication_role*/,
    std::optional<IsolationLevel> override_isolation_level) {
  auto isolation_level = override_isolation_level.value_or(isolation_level_);
  if (isolation_level != IsolationLevel::SNAPSHOT_ISOLATION) {
    throw utils::NotYetImplemented("Disk storage supports only SNAPSHOT isolation level.");
  }
  return std::unique_ptr<DiskAccessor>(
      new DiskAccessor{Storage::Accessor::unique_access, this, isolation_level, storage_mode_});
}

bool DiskStorage::DiskAccessor::EdgeTypeIndexExists(EdgeTypeId /*edge_type*/) const {
  spdlog::info("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return false;
}

IndicesInfo DiskStorage::DiskAccessor::ListAllIndices() const {
  auto *on_disk = static_cast<DiskStorage *>(storage_);
  auto *disk_label_index = static_cast<DiskLabelIndex *>(on_disk->indices_.label_index_.get());
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(on_disk->indices_.label_property_index_.get());
  auto &text_index = storage_->indices_.text_index_;
  return {disk_label_index->ListIndices(),
          disk_label_property_index->ListIndices(),
          {/* edge type indices */},
          text_index.ListIndices()};
}
ConstraintsInfo DiskStorage::DiskAccessor::ListAllConstraints() const {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  return {disk_storage->constraints_.existence_constraints_->ListConstraints(),
          disk_storage->constraints_.unique_constraints_->ListConstraints()};
}
}  // namespace memgraph::storage
