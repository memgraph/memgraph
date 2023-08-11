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
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>

#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include "kvstore/kvstore.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/disk/edge_import_mode_cache.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/disk/unique_constraints.hpp"
#include "storage/v2/edge_import_mode.hpp"
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
#include "utils/disk_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/result.hpp"
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
constexpr const char *lastTransactionStartTimeStamp = "last_transaction_start_timestamp";
constexpr const char *label_index_str = "label_index";
constexpr const char *label_property_index_str = "label_property_index";
constexpr const char *existence_constraints_str = "existence_constraints";
constexpr const char *unique_constraints_str = "unique_constraints";

bool VertexNeedsToBeSerialized(const Vertex &vertex) {
  Delta *head = vertex.delta;
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

void DiskStorage::LoadTimestampIfExists() {
  if (!utils::DirExists(config_.disk.durability_directory)) {
    return;
  }
  if (auto last_timestamp_ = durability_kvstore_->Get(lastTransactionStartTimeStamp); last_timestamp_.has_value()) {
    timestamp_ = std::stoull(last_timestamp_.value());
  }
}

void DiskStorage::LoadIndexInfoIfExists() const {
  if (utils::DirExists(config_.disk.durability_directory)) {
    LoadLabelIndexInfoIfExists();
    LoadLabelPropertyIndexInfoIfExists();
  }
}

void DiskStorage::LoadLabelIndexInfoIfExists() const {
  if (auto label_index = durability_kvstore_->Get(label_index_str); label_index.has_value()) {
    auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
    const std::vector<std::string> labels{utils::Split(label_index.value(), "|")};
    disk_label_index->LoadIndexInfo(labels);
  }
}

void DiskStorage::LoadLabelPropertyIndexInfoIfExists() const {
  if (auto label_property_index = durability_kvstore_->Get(label_property_index_str);
      label_property_index.has_value()) {
    auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
    const std::vector<std::string> keys{utils::Split(label_property_index.value(), "|")};
    disk_label_property_index->LoadIndexInfo(keys);
  }
}

void DiskStorage::LoadConstraintsInfoIfExists() const {
  if (utils::DirExists(config_.disk.durability_directory)) {
    LoadExistenceConstraintInfoIfExists();
    LoadUniqueConstraintInfoIfExists();
  }
}

void DiskStorage::LoadExistenceConstraintInfoIfExists() const {
  if (auto existence_constraints = durability_kvstore_->Get(existence_constraints_str);
      existence_constraints.has_value()) {
    std::vector<std::string> keys = utils::Split(existence_constraints.value(), "|");
    constraints_.existence_constraints_->LoadExistenceConstraints(keys);
  }
}

void DiskStorage::LoadUniqueConstraintInfoIfExists() const {
  if (auto unique_constraints = durability_kvstore_->Get(unique_constraints_str); unique_constraints.has_value()) {
    std::vector<std::string> keys = utils::Split(unique_constraints.value(), "|");
    auto *disk_unique_constraints = static_cast<DiskUniqueConstraints *>(constraints_.unique_constraints_.get());
    disk_unique_constraints->LoadUniqueConstraints(keys);
  }
}

DiskStorage::DiskStorage(Config config)
    : Storage(config, StorageMode::ON_DISK_TRANSACTIONAL),
      kvstore_(std::make_unique<RocksDBStorage>()),
      durability_kvstore_(std::make_unique<kvstore::KVStore>(config.disk.durability_directory)) {
  LoadTimestampIfExists();
  LoadIndexInfoIfExists();
  LoadConstraintsInfoIfExists();
  kvstore_->options_.create_if_missing = true;
  kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  kvstore_->options_.compression = rocksdb::kNoCompression;
  kvstore_->options_.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;
  kvstore_->options_.wal_dir = config_.disk.wal_directory;
  kvstore_->options_.wal_compression = rocksdb::kNoCompression;
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
  durability_kvstore_->Put(lastTransactionStartTimeStamp, std::to_string(timestamp_));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->vertex_chandle));
  logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->edge_chandle));
  if (kvstore_->default_chandle) {
    // We must destroy default column family handle only if it was read from existing database.
    // https://github.com/facebook/rocksdb/issues/5006#issuecomment-1003154821
    logging::AssertRocksDBStatus(kvstore_->db_->DestroyColumnFamilyHandle(kvstore_->default_chandle));
  }
  delete kvstore_->options_.comparator;
  kvstore_->options_.comparator = nullptr;
}

DiskStorage::DiskAccessor::DiskAccessor(DiskStorage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : Accessor(storage, isolation_level, storage_mode), config_(storage->config_.items) {
  rocksdb::WriteOptions write_options;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  disk_transaction_ = storage->kvstore_->db_->BeginTransaction(write_options, txOptions);
  disk_transaction_->SetReadTimestampForValidation(transaction_.start_timestamp);
}

DiskStorage::DiskAccessor::DiskAccessor(DiskAccessor &&other) noexcept
    : Accessor(std::move(other)), config_(other.config_) {
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

DiskStorage::DiskAccessor::~DiskAccessor() {
  if (is_transaction_active_) {
    Abort();
  }

  FinalizeTransaction();
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToEdgeImportCache(std::string &&key,
                                                                                              std::string &&value) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto cache_accessor = disk_storage->edge_import_mode_cache_->cache_.access();

  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromKey(key)));
  if (VertexExistsInCache(cache_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromMainDiskStorage(key)};
  PropertyStore properties{utils::DeserializePropertiesFromMainDiskStorage(value)};
  return CreateVertex(cache_accessor, gid, std::move(labels_id), std::move(properties),
                      CreateDeleteDeserializedObjectDelta(&transaction_, key));
}

/// NOTE: This will create Delta object which will cause deletion of old key entry on the disk
std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToMainMemoryCache(std::string &&key,
                                                                                              std::string &&value) {
  auto main_storage_accessor = vertices_.access();

  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromKey(key)));
  if (VertexExistsInCache(main_storage_accessor, gid)) {
    return std::nullopt;
  }
  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromMainDiskStorage(key)};
  PropertyStore properties{utils::DeserializePropertiesFromMainDiskStorage(value)};
  return CreateVertex(main_storage_accessor, gid, std::move(labels_id), std::move(properties),
                      CreateDeleteDeserializedObjectDelta(&transaction_, key));
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToLabelIndexCache(
    LabelId indexing_label, std::string &&key, std::string &&value, Delta *index_delta,
    utils::SkipList<storage::Vertex>::Accessor index_accessor) {
  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelIndexStorage(key)));
  if (VertexExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }

  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelIndexStorage(value)};
  labels_id.push_back(indexing_label);
  PropertyStore properties{utils::DeserializePropertiesFromLabelIndexStorage(value)};
  return CreateVertex(index_accessor, gid, std::move(labels_id), std::move(properties), index_delta);
}

std::optional<storage::VertexAccessor> DiskStorage::DiskAccessor::LoadVertexToLabelPropertyIndexCache(
    LabelId indexing_label, std::string &&key, std::string &&value, Delta *index_delta,
    utils::SkipList<storage::Vertex>::Accessor index_accessor) {
  storage::Gid gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key)));
  if (VertexExistsInCache(index_accessor, gid)) {
    return std::nullopt;
  }

  std::vector<LabelId> labels_id{utils::DeserializeLabelsFromLabelPropertyIndexStorage(value)};
  labels_id.push_back(indexing_label);
  PropertyStore properties{utils::DeserializePropertiesFromLabelPropertyIndexStorage(value)};
  return CreateVertex(index_accessor, gid, std::move(labels_id), std::move(properties), index_delta);
}

std::optional<EdgeAccessor> DiskStorage::DiskAccessor::DeserializeEdge(const rocksdb::Slice &key,
                                                                       const rocksdb::Slice &value) {
  const auto edge_parts = utils::Split(key.ToStringView(), "|");
  const Gid edge_gid = Gid::FromUint(std::stoull(edge_parts[4]));

  auto edge_acc = edges_.access();
  auto res = edge_acc.find(edge_gid);
  if (res != edge_acc.end()) {
    return std::nullopt;
  }

  const auto [from_gid, to_gid] = std::invoke(
      [](const auto &edge_parts) {
        if (edge_parts[2] == "0") {  // out edge
          return std::make_pair(edge_parts[0], edge_parts[1]);
        }
        // in edge
        return std::make_pair(edge_parts[1], edge_parts[0]);
      },
      edge_parts);

  const auto from_acc = FindVertex(Gid::FromUint(std::stoull(from_gid)), View::OLD);
  const auto to_acc = FindVertex(Gid::FromUint(std::stoull(to_gid)), View::OLD);
  if (!from_acc || !to_acc) {
    throw utils::BasicException("Non-existing vertices found during edge deserialization");
  }
  const auto edge_type_id = storage::EdgeTypeId::FromUint(std::stoull(edge_parts[3]));
  auto maybe_edge = CreateEdge(&*from_acc, &*to_acc, edge_type_id, edge_gid, value.ToStringView(), key.ToString());
  MG_ASSERT(maybe_edge.HasValue());

  return *maybe_edge;
}

void DiskStorage::DiskAccessor::LoadVerticesToMainMemoryCache() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it =
      std::unique_ptr<rocksdb::Iterator>(disk_transaction_->GetIterator(ro, disk_storage->kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    LoadVertexToMainMemoryCache(it->key().ToString(), it->value().ToString());
  }
}

/// TODO: how to remove this
void DiskStorage::DiskAccessor::LoadVerticesToEdgeImportCache() {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto it =
      std::unique_ptr<rocksdb::Iterator>(disk_transaction_->GetIterator(ro, disk_storage->kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    LoadVertexToEdgeImportCache(it->key().ToString(), it->value().ToString());
  }
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(View view) {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  if (disk_storage->edge_import_status_ == EdgeImportMode::ON) {
    if (!disk_storage->edge_import_mode_cache_->scanned_all_vertices_) {
      LoadVerticesToEdgeImportCache();
    }
    return VerticesIterable(AllVerticesIterable(disk_storage->edge_import_mode_cache_->cache_.access(), &transaction_,
                                                view, &storage_->indices_, &storage_->constraints_,
                                                storage_->config_.items));
  }
  LoadVerticesToMainMemoryCache();
  return VerticesIterable(AllVerticesIterable(vertices_.access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, View view) {
  index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = index_storage_.back();
  index_deltas_storage_.emplace_back();
  auto &index_deltas = index_deltas_storage_.back();

  auto gids = MergeVerticesFromMainCacheWithLabelIndexCache(label, view, index_deltas, indexed_vertices.get());
  LoadVerticesFromDiskLabelIndex(label, gids, index_deltas, indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

std::unordered_set<Gid> DiskStorage::DiskAccessor::MergeVerticesFromMainCacheWithLabelIndexCache(
    LabelId label, View view, std::list<Delta> &index_deltas, utils::SkipList<Vertex> *indexed_vertices) {
  auto main_cache_acc = vertices_.access();
  std::unordered_set<Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    if (VertexHasLabel(vertex, label, &transaction_, view)) {
      spdlog::trace("Loaded vertex with gid: {} from main index storage to label index",
                    utils::SerializeIdType(vertex.gid));
      /// TODO: here are doing serialization and then later deserialization again -> expensive
      LoadVertexToLabelIndexCache(label, utils::SerializeVertexAsKeyForLabelIndex(label, vertex.gid),
                                  utils::SerializeVertexAsValueForLabelIndex(label, vertex.labels, vertex.properties),
                                  CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, std::nullopt),
                                  indexed_vertices->access());
    }
  }
  return gids;
}

void DiskStorage::DiskAccessor::LoadVerticesFromDiskLabelIndex(LabelId label,
                                                               const std::unordered_set<storage::Gid> &gids,
                                                               std::list<Delta> &index_deltas,
                                                               utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_index = static_cast<DiskLabelIndex *>(storage_->indices_.label_index_.get());
  auto disk_index_transaction = disk_label_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);

  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto serialized_label = utils::SerializeIdType(label);
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelIndexStorage(key)));
    spdlog::trace("Loaded vertex with key: {} from label index storage", key);
    if (key.starts_with(serialized_label) && !utils::Contains(gids, curr_gid)) {
      LoadVertexToLabelIndexCache(label, index_it->key().ToString(), index_it->value().ToString(),
                                  CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, key),
                                  indexed_vertices->access());
    }
  }
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, View view) {
  index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = index_storage_.back();
  index_deltas_storage_.emplace_back();
  auto &index_deltas = index_deltas_storage_.back();

  const auto label_property_filter = [this](const Vertex &vertex, LabelId label, PropertyId property,
                                            View view) -> bool {
    return VertexHasLabel(vertex, label, &transaction_, view) &&
           HasVertexProperty(vertex, property, &transaction_, view);
  };

  const auto gids = MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
      label, property, view, index_deltas, indexed_vertices.get(), label_property_filter);

  const auto disk_label_property_filter = [](const std::string &key, const std::string &label_property_prefix,
                                             const std::unordered_set<Gid> &gids, Gid curr_gid) -> bool {
    return key.starts_with(label_property_prefix) && !utils::Contains(gids, curr_gid);
  };

  LoadVerticesFromDiskLabelPropertyIndex(label, property, gids, index_deltas, indexed_vertices.get(),
                                         disk_label_property_filter);

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

std::unordered_set<Gid> DiskStorage::DiskAccessor::MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
    LabelId label, PropertyId property, View view, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices, const auto &label_property_filter) {
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    /// TODO: delta support for clearing old disk keys
    if (label_property_filter(vertex, label, property, view)) {
      LoadVertexToLabelPropertyIndexCache(
          label, utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties),
          CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, std::nullopt),
          indexed_vertices->access());
    }
  }

  return gids;
}

void DiskStorage::DiskAccessor::LoadVerticesFromDiskLabelPropertyIndex(LabelId label, PropertyId property,
                                                                       const std::unordered_set<storage::Gid> &gids,
                                                                       std::list<Delta> &index_deltas,
                                                                       utils::SkipList<Vertex> *indexed_vertices,
                                                                       const auto &label_property_filter) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto label_property_prefix = utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property);
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key)));
    /// TODO: optimize
    if (label_property_filter(key, label_property_prefix, gids, curr_gid)) {
      LoadVertexToLabelPropertyIndexCache(label, index_it->key().ToString(), index_it->value().ToString(),
                                          CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, key),
                                          indexed_vertices->access());
    }
  }
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value,
                                                     View view) {
  index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = index_storage_.back();
  index_deltas_storage_.emplace_back();
  auto &index_deltas = index_deltas_storage_.back();

  auto label_property_filter = [this, &value](const Vertex &vertex, LabelId label, PropertyId property,
                                              View view) -> bool {
    return VertexHasLabel(vertex, label, &transaction_, view) &&
           VertexHasEqualPropertyValue(vertex, property, value, &transaction_, view);
  };

  const auto gids = MergeVerticesFromMainCacheWithLabelPropertyIndexCache(
      label, property, view, index_deltas, indexed_vertices.get(), label_property_filter);

  LoadVerticesFromDiskLabelPropertyIndexWithPointValueLookup(label, property, gids, value, index_deltas,
                                                             indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

void DiskStorage::DiskAccessor::LoadVerticesFromDiskLabelPropertyIndexWithPointValueLookup(
    LabelId label, PropertyId property, const std::unordered_set<storage::Gid> &gids, const PropertyValue &value,
    std::list<Delta> &index_deltas, utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());
  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const auto label_property_prefix = utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property);
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key = index_it->key().ToString();
    std::string it_value = index_it->value().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key)));
    /// TODO: optimize
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value);
    if (key.starts_with(label_property_prefix) && !utils::Contains(gids, curr_gid) &&
        properties.IsPropertyEqual(property, value)) {
      LoadVertexToLabelPropertyIndexCache(label, index_it->key().ToString(), index_it->value().ToString(),
                                          CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, key),
                                          indexed_vertices->access());
    }
  }
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property,
                                                     const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                     const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                     View view) {
  index_storage_.emplace_back(std::make_unique<utils::SkipList<storage::Vertex>>());
  auto &indexed_vertices = index_storage_.back();
  index_deltas_storage_.emplace_back();
  auto &index_deltas = index_deltas_storage_.back();

  const auto gids = MergeVerticesFromMainCacheWithLabelPropertyIndexCacheForIntervalSearch(
      label, property, view, lower_bound, upper_bound, index_deltas, indexed_vertices.get());

  LoadVerticesFromDiskLabelPropertyIndexForIntervalSearch(label, property, gids, lower_bound, upper_bound, index_deltas,
                                                          indexed_vertices.get());

  return VerticesIterable(AllVerticesIterable(indexed_vertices->access(), &transaction_, view, &storage_->indices_,
                                              &storage_->constraints_, storage_->config_.items));
}

std::unordered_set<Gid>
DiskStorage::DiskAccessor::MergeVerticesFromMainCacheWithLabelPropertyIndexCacheForIntervalSearch(
    LabelId label, PropertyId property, View view, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices) {
  auto main_cache_acc = vertices_.access();
  std::unordered_set<storage::Gid> gids;
  gids.reserve(main_cache_acc.size());

  for (const auto &vertex : main_cache_acc) {
    gids.insert(vertex.gid);
    auto prop_value = GetVertexProperty(vertex, property, &transaction_, view);
    if (VertexHasLabel(vertex, label, &transaction_, view) &&
        IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      LoadVertexToLabelPropertyIndexCache(
          label, utils::SerializeVertexAsKeyForLabelPropertyIndex(label, property, vertex.gid),
          utils::SerializeVertexAsValueForLabelPropertyIndex(label, vertex.labels, vertex.properties),
          CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, std::nullopt),
          indexed_vertices->access());
    }
  }
  return gids;
}

void DiskStorage::DiskAccessor::LoadVerticesFromDiskLabelPropertyIndexForIntervalSearch(
    LabelId label, PropertyId property, const std::unordered_set<storage::Gid> &gids,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, std::list<Delta> &index_deltas,
    utils::SkipList<Vertex> *indexed_vertices) {
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  auto disk_index_transaction = disk_label_property_index->CreateRocksDBTransaction();
  disk_index_transaction->SetReadTimestampForValidation(transaction_.start_timestamp);
  rocksdb::ReadOptions ro;
  std::string strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  ro.timestamp = &ts;
  auto index_it = std::unique_ptr<rocksdb::Iterator>(disk_index_transaction->GetIterator(ro));

  const std::string label_property_prefix = utils::SerializeIdType(label) + "|" + utils::SerializeIdType(property);
  for (index_it->SeekToFirst(); index_it->Valid(); index_it->Next()) {
    std::string key_str = index_it->key().ToString();
    std::string it_value_str = index_it->value().ToString();
    Gid curr_gid = Gid::FromUint(std::stoull(utils::ExtractGidFromLabelPropertyIndexStorage(key_str)));
    /// TODO: andi this will be optimized
    /// TODO: couple this condition
    PropertyStore properties = utils::DeserializePropertiesFromLabelPropertyIndexStorage(it_value_str);
    auto prop_value = properties.GetProperty(property);
    if (!key_str.starts_with(label_property_prefix) || utils::Contains(gids, curr_gid) ||
        !IsPropertyValueWithinInterval(prop_value, lower_bound, upper_bound)) {
      continue;
    }
    LoadVertexToLabelPropertyIndexCache(label, index_it->key().ToString(), index_it->value().ToString(),
                                        CreateDeleteDeserializedIndexObjectDelta(&transaction_, index_deltas, key_str),
                                        indexed_vertices->access());
  }
}

uint64_t DiskStorage::DiskAccessor::ApproximateVertexCount() const {
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  return disk_storage->kvstore_->ApproximateVertexCount();
}

bool DiskStorage::PersistLabelIndexCreation(LabelId label) const {
  if (auto label_index_store = durability_kvstore_->Get(label_index_str); label_index_store.has_value()) {
    std::string &value = label_index_store.value();
    value += "|" + utils::SerializeIdType(label);
    return durability_kvstore_->Put(label_index_str, value);
  }
  return durability_kvstore_->Put(label_index_str, utils::SerializeIdType(label));
}

bool DiskStorage::PersistLabelIndexDeletion(LabelId label) const {
  if (auto label_index_store = durability_kvstore_->Get(label_index_str); label_index_store.has_value()) {
    const std::string &value = label_index_store.value();
    std::vector<std::string> labels = utils::Split(value, "|");
    std::erase(labels, utils::SerializeIdType(label));
    if (labels.empty()) {
      return durability_kvstore_->Delete(label_index_str);
    }
    return durability_kvstore_->Put(label_index_str, utils::Join(labels, "|"));
  }
  return true;
}

bool DiskStorage::PersistLabelPropertyIndexAndExistenceConstraintCreation(LabelId label, PropertyId property,
                                                                          const char *key) const {
  if (auto label_property_index_store = durability_kvstore_->Get(key); label_property_index_store.has_value()) {
    std::string &value = label_property_index_store.value();
    value += "|" + utils::SerializeIdType(label) + "," + utils::SerializeIdType(property);
    return durability_kvstore_->Put(key, value);
  }
  return durability_kvstore_->Put(key, utils::SerializeIdType(label) + "," + utils::SerializeIdType(property));
}

bool DiskStorage::PersistLabelPropertyIndexAndExistenceConstraintDeletion(LabelId label, PropertyId property,
                                                                          const char *key) const {
  if (auto label_property_index_store = durability_kvstore_->Get(key); label_property_index_store.has_value()) {
    const std::string &value = label_property_index_store.value();
    std::vector<std::string> label_properties = utils::Split(value, "|");
    std::erase(label_properties, utils::SerializeIdType(label) + "," + utils::SerializeIdType(property));
    if (label_properties.empty()) {
      return durability_kvstore_->Delete(key);
    }
    return durability_kvstore_->Put(key, utils::Join(label_properties, "|"));
  }
  return true;
}

bool DiskStorage::PersistUniqueConstraintCreation(LabelId label, const std::set<PropertyId> &properties) const {
  std::string entry = utils::SerializeIdType(label);
  for (auto property : properties) {
    entry += "," + utils::SerializeIdType(property);
  }

  if (auto unique_store = durability_kvstore_->Get(unique_constraints_str); unique_store.has_value()) {
    std::string &value = unique_store.value();
    value += "|" + entry;
    return durability_kvstore_->Put(unique_constraints_str, value);
  }
  return durability_kvstore_->Put(unique_constraints_str, entry);
}

bool DiskStorage::PersistUniqueConstraintDeletion(LabelId label, const std::set<PropertyId> &properties) const {
  std::string entry = utils::SerializeIdType(label);
  for (auto property : properties) {
    entry += "," + utils::SerializeIdType(property);
  }

  if (auto unique_store = durability_kvstore_->Get(unique_constraints_str); unique_store.has_value()) {
    const std::string &value = unique_store.value();
    std::vector<std::string> unique_constraints = utils::Split(value, "|");
    std::erase(unique_constraints, entry);
    if (unique_constraints.empty()) {
      return durability_kvstore_->Delete(unique_constraints_str);
    }
    return durability_kvstore_->Put(unique_constraints_str, utils::Join(unique_constraints, "|"));
  }
  return true;
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

StorageInfo DiskStorage::GetInfo() const {
  auto vertex_count = kvstore_->ApproximateVertexCount();
  auto edge_count = kvstore_->ApproximateEdgeCount();
  double average_degree = 0.0;
  if (vertex_count) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions, cppcoreguidelines-narrowing-conversions)
    average_degree = 2.0 * static_cast<double>(edge_count) / vertex_count;
  }

  return {vertex_count, edge_count, average_degree, utils::GetMemoryUsage(), GetDiskSpaceUsage()};
}

void DiskStorage::SetEdgeImportMode(EdgeImportMode edge_import_status) {
  std::unique_lock main_guard{main_lock_};
  if (edge_import_status == edge_import_status_) {
    return;
  }
  if (edge_import_status == EdgeImportMode::ON) {
    const auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
    const auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
    edge_import_mode_cache_ = std::make_unique<EdgeImportModeCache>(disk_label_index, disk_label_property_index);
  } else {
    edge_import_mode_cache_ = nullptr;
  }

  edge_import_status_ = edge_import_status;
  spdlog::trace("Edge import mode changed to: {}", EdgeImportModeToString(edge_import_status));
}

EdgeImportMode DiskStorage::GetEdgeImportMode() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return edge_import_status_;
}

VertexAccessor DiskStorage::DiskAccessor::CreateVertex() {
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
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

VertexAccessor DiskStorage::DiskAccessor::CreateVertex(utils::SkipList<Vertex>::Accessor &accessor, storage::Gid gid,
                                                       std::vector<LabelId> &&label_ids, PropertyStore &&properties,
                                                       Delta *delta) {
  OOMExceptionEnabler oom_exception;
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  /// TODO: I don't think we need here
  disk_storage->vertex_id_.store(std::max(disk_storage->vertex_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                                 std::memory_order_release);
  auto [it, inserted] = accessor.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != accessor.end(), "Invalid Vertex accessor!");
  it->labels = std::move(label_ids);
  it->properties = std::move(properties);
  delta->prev.Set(&*it);
  return {&*it, &transaction_, &storage_->indices_, &storage_->constraints_, config_};
}

std::optional<VertexAccessor> DiskStorage::DiskAccessor::FindVertex(storage::Gid gid, View view) {
  auto acc = vertices_.access();
  auto vertex_it = acc.find(gid);
  if (vertex_it != acc.end()) {
    return VertexAccessor::Create(&*vertex_it, &transaction_, &storage_->indices_, &storage_->constraints_, config_,
                                  view);
  }
  for (const auto &vec : index_storage_) {
    acc = vec->access();
    auto index_it = acc.find(gid);
    if (index_it != acc.end()) {
      return VertexAccessor::Create(&*index_it, &transaction_, &storage_->indices_, &storage_->constraints_, config_,
                                    view);
    }
  }

  rocksdb::ReadOptions read_opts;
  auto strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  read_opts.timestamp = &ts;
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto it = std::unique_ptr<rocksdb::Iterator>(
      disk_transaction_->GetIterator(read_opts, disk_storage->kvstore_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (Gid::FromUint(std::stoull(utils::ExtractGidFromKey(key))) == gid) {
      return LoadVertexToMainMemoryCache(std::move(key), it->value().ToString());
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

bool DiskStorage::DiskAccessor::PrefetchEdgeFilter(const std::string_view disk_edge_key_str,
                                                   const VertexAccessor &vertex_acc, EdgeDirection edge_direction) {
  bool isOutEdge = (edge_direction == EdgeDirection::OUT);
  DiskEdgeKey disk_edge_key(disk_edge_key_str);
  auto edges_res = (isOutEdge ? vertex_acc.OutEdges(storage::View::NEW) : vertex_acc.InEdges(storage::View::NEW));
  const std::string disk_vertex_gid = (isOutEdge ? disk_edge_key.GetVertexOutGid() : disk_edge_key.GetVertexInGid());
  auto edge_gid = disk_edge_key.GetEdgeGid();

  if (disk_vertex_gid != utils::SerializeIdType(vertex_acc.Gid())) {
    return false;
  }

  // We need to search in edges_to_delete_ because removed edges are not presented in edges_res
  if (auto edgeIt = edges_to_delete_.find(disk_edge_key.GetSerializedKey()); edgeIt != edges_to_delete_.end()) {
    return false;
  }

  MG_ASSERT(edges_res.HasValue());
  auto edges = edges_res.GetValue();
  bool isEdgeAlreadyInMemory = std::any_of(edges.begin(), edges.end(), [edge_gid](const auto &edge_acc) {
    return utils::SerializeIdType(edge_acc.Gid()) == edge_gid;
  });

  return !isEdgeAlreadyInMemory;
}

void DiskStorage::DiskAccessor::PrefetchEdges(const VertexAccessor &vertex_acc, EdgeDirection edge_direction) {
  rocksdb::ReadOptions read_opts;
  auto strTs = utils::StringTimestamp(transaction_.start_timestamp);
  rocksdb::Slice ts(strTs);
  read_opts.timestamp = &ts;
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto it = std::unique_ptr<rocksdb::Iterator>(
      disk_transaction_->GetIterator(read_opts, disk_storage->kvstore_->edge_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const rocksdb::Slice &key = it->key();
    auto keyStr = key.ToStringView();
    if (PrefetchEdgeFilter(keyStr, vertex_acc, edge_direction)) {
      DeserializeEdge(key, it->value());
    }
  }
}

void DiskStorage::DiskAccessor::PrefetchInEdges(const VertexAccessor &vertex_acc) {
  PrefetchEdges(vertex_acc, EdgeDirection::IN);
}

void DiskStorage::DiskAccessor::PrefetchOutEdges(const VertexAccessor &vertex_acc) {
  PrefetchEdges(vertex_acc, EdgeDirection::OUT);
}

Result<EdgeAccessor> DiskStorage::DiskAccessor::CreateEdge(const VertexAccessor *from, const VertexAccessor *to,
                                                           EdgeTypeId edge_type, storage::Gid gid,
                                                           const std::string_view properties,
                                                           const std::string &old_disk_key) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

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
  disk_storage->edge_id_.store(std::max(disk_storage->edge_id_.load(std::memory_order_acquire), gid.AsUint() + 1),
                               std::memory_order_release);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto *delta = CreateDeleteDeserializedObjectDelta(&transaction_, old_disk_key);
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

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

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
  const auto edge_ref = edge->edge_;
  const auto edge_type = edge->edge_type_;

  std::unique_lock<utils::SpinLock> guard;
  if (config_.properties_on_edges) {
    const auto *edge_ptr = edge_ref.ptr;
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
    const std::tuple<EdgeTypeId, Vertex *, EdgeRef> link(edge_type, vertex, edge_ref);
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

  const auto op1 = delete_edge_from_storage(to_vertex, &from_vertex->out_edges);
  const auto op2 = delete_edge_from_storage(from_vertex, &to_vertex->in_edges);

  const DiskEdgeKey disk_edge_key(from_vertex->gid, to_vertex->gid, edge_type, edge_ref, config_.properties_on_edges);
  edges_to_delete_.emplace(disk_edge_key.GetSerializedKey());

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
/// TODO: this method should also delete the old key
bool DiskStorage::DiskAccessor::WriteVertexToDisk(const Vertex &vertex) {
  MG_ASSERT(commit_timestamp_.has_value(), "Writing vertex to disk but commit timestamp not set.");
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  auto status = disk_transaction_->Put(disk_storage->kvstore_->vertex_chandle, utils::SerializeVertex(vertex),
                                       utils::SerializeProperties(vertex.properties));
  if (status.ok()) {
    spdlog::trace("rocksdb: Saved vertex with key {} and ts {}", utils::SerializeVertex(vertex), *commit_timestamp_);
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
  MG_ASSERT(commit_timestamp_.has_value(), "Writing vertex to disk but commit timestamp not set.");
  auto *disk_storage = static_cast<DiskStorage *>(storage_);
  rocksdb::Status status;
  if (config_.properties_on_edges) {
    status = disk_transaction_->Put(disk_storage->kvstore_->edge_chandle, serializedEdgeKey,
                                    utils::SerializeProperties(edge.ptr->properties));
  } else {
    status = disk_transaction_->Put(disk_storage->kvstore_->edge_chandle, serializedEdgeKey, "");
  }
  if (status.ok()) {
    spdlog::trace("rocksdb: Saved edge with key {} and ts {}", serializedEdgeKey, *commit_timestamp_);
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
    spdlog::trace("rocksdb: Deleted vertex with key {}", vertex);
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
    spdlog::trace("rocksdb: Deleted edge with key {}", edge);
  } else if (status.IsBusy()) {
    spdlog::error("rocksdb: Edge with key {} was changed and committed in another transaction", edge);
    return false;
  } else {
    spdlog::error("rocksdb: Failed to delete edge with key {}", edge);
    return false;
  }
  return true;
}

[[nodiscard]] utils::BasicResult<StorageDataManipulationError, void>
DiskStorage::DiskAccessor::CheckVertexConstraintsBeforeCommit(
    const Vertex &vertex, std::vector<std::vector<PropertyValue>> &unique_storage) const {
  if (auto existence_constraint_validation_result = storage_->constraints_.existence_constraints_->Validate(vertex);
      existence_constraint_validation_result.has_value()) {
    return StorageDataManipulationError{existence_constraint_validation_result.value()};
  }

  auto *disk_unique_constraints =
      static_cast<DiskUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
  if (auto unique_constraint_validation_result = disk_unique_constraints->Validate(vertex, unique_storage);
      unique_constraint_validation_result.has_value()) {
    return StorageDataManipulationError{unique_constraint_validation_result.value()};
  }
  return {};
}

[[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::FlushMainMemoryCache() {
  auto vertex_acc = vertices_.access();

  std::vector<std::vector<PropertyValue>> unique_storage;
  auto *disk_unique_constraints =
      static_cast<DiskUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(storage_->indices_.label_index_.get());
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  /// TODO: andi I don't like that std::optional is used for checking errors but that's how it was before, refactor!
  for (Vertex &vertex : vertex_acc) {
    if (VertexNeedsToBeSerialized(vertex)) {
      if (auto check_result = CheckVertexConstraintsBeforeCommit(vertex, unique_storage); check_result.HasError()) {
        return check_result.GetError();
      }

      /// TODO: what if something is changed and then deleted and how does this work connected to indices and
      /// constraints
      if (vertex.deleted) {
        continue;
      }

      /// TODO: expose temporal coupling
      /// NOTE: this deletion has to come before writing, otherwise RocksDB thinks that all entries are deleted
      /// TODO: This has to deal with index storage if read from index cache
      if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(vertex.delta); maybe_old_disk_key.has_value()) {
        if (!DeleteVertexFromDisk(maybe_old_disk_key.value())) {
          return StorageDataManipulationError{SerializationError{}};
        }
      }

      if (!WriteVertexToDisk(vertex)) {
        return StorageDataManipulationError{SerializationError{}};
      }

      if (!disk_unique_constraints->SyncVertexToUniqueConstraintsStorage(vertex, *commit_timestamp_) ||
          !disk_label_index->SyncVertexToLabelIndexStorage(vertex, *commit_timestamp_) ||
          !disk_label_property_index->SyncVertexToLabelPropertyIndexStorage(vertex, *commit_timestamp_)) {
        return StorageDataManipulationError{SerializationError{}};
      }
    }

    for (auto &edge_entry : vertex.out_edges) {
      EdgeRef edge = std::get<2>(edge_entry);
      const DiskEdgeKey src_dest_key(vertex.gid, std::get<1>(edge_entry)->gid, std::get<0>(edge_entry), edge,
                                     config_.properties_on_edges);

      /// TODO: expose temporal coupling
      /// NOTE: this deletion has to come before writing, otherwise RocksDB thinks that all entries are deleted
      if (config_.properties_on_edges) {
        if (auto maybe_old_disk_key = utils::GetOldDiskKeyOrNull(edge.ptr->delta); maybe_old_disk_key.has_value()) {
          if (!DeleteEdgeFromDisk(maybe_old_disk_key.value())) {
            return StorageDataManipulationError{SerializationError{}};
          }
        }
      }

      if (!WriteEdgeToDisk(edge, src_dest_key.GetSerializedKey())) {
        return StorageDataManipulationError{SerializationError{}};
      }

      /// TODO: what if edge has already been deleted
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

/// TODO: I think unique_storage is not needed here
[[nodiscard]] utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::FlushIndexCache() {
  std::vector<std::vector<PropertyValue>> unique_storage;
  auto *disk_unique_constraints =
      static_cast<DiskUniqueConstraints *>(storage_->constraints_.unique_constraints_.get());
  auto *disk_label_index = static_cast<DiskLabelIndex *>(storage_->indices_.label_index_.get());
  auto *disk_label_property_index =
      static_cast<DiskLabelPropertyIndex *>(storage_->indices_.label_property_index_.get());

  for (const auto &vec : index_storage_) {
    auto vertex_acc = vec->access();
    for (Vertex &vertex : vertex_acc) {
      if (auto check_result = CheckVertexConstraintsBeforeCommit(vertex, unique_storage); check_result.HasError()) {
        return check_result.GetError();
      }

      /// TODO: what if something is changed and then deleted
      if (vertex.deleted) {
        continue;
      }

      if (!WriteVertexToDisk(vertex)) {
        return StorageDataManipulationError{SerializationError{}};
      }

      /// TODO: andi don't ignore the return value
      if (!disk_unique_constraints->SyncVertexToUniqueConstraintsStorage(vertex, *commit_timestamp_) ||
          !disk_label_index->SyncVertexToLabelIndexStorage(vertex, *commit_timestamp_) ||
          !disk_label_property_index->SyncVertexToLabelPropertyIndexStorage(vertex, *commit_timestamp_)) {
        return StorageDataManipulationError{SerializationError{}};
      }

      for (auto &edge_entry : vertex.out_edges) {
        EdgeRef edge = std::get<2>(edge_entry);
        DiskEdgeKey src_dest_key(vertex.gid, std::get<1>(edge_entry)->gid, std::get<0>(edge_entry), edge,
                                 config_.properties_on_edges);

        if (!WriteEdgeToDisk(edge, src_dest_key.GetSerializedKey())) {
          return StorageDataManipulationError{SerializationError{}};
        }

        /// TODO: what if edge has already been deleted
      }
    }
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

// NOLINTNEXTLINE(google-default-arguments)
utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  auto *disk_storage = static_cast<DiskStorage *>(storage_);

  if (transaction_.deltas.empty() ||
      std::all_of(transaction_.deltas.begin(), transaction_.deltas.end(),
                  [](const Delta &delta) { return delta.action == Delta::Action::DELETE_DESERIALIZED_OBJECT; })) {
  } else {
    std::unique_lock<utils::SpinLock> engine_guard(storage_->engine_lock_);
    commit_timestamp_.emplace(disk_storage->CommitTimestamp(desired_commit_timestamp));

    if (auto res = FlushMainMemoryCache(); res.HasError()) {
      Abort();
      return res;
    }

    if (auto res = FlushIndexCache(); res.HasError()) {
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
  spdlog::trace("rocksdb: Commit successful");

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

/// TODO: what to do with all that?
void DiskStorage::DiskAccessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  // NOTE: On abort we need to delete disk transaction because after storage remove we couldn't remove
  // disk_transaction correctly in destructor.
  // This happens in tests when we create and remove storage in one test. For example, in
  // query_plan_accumulate_aggregate.cpp
  disk_transaction_->Rollback();
  disk_transaction_->ClearSnapshot();
  delete disk_transaction_;
  disk_transaction_ = nullptr;

  is_transaction_active_ = false;
}

void DiskStorage::DiskAccessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    commit_timestamp_.reset();
  }
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  auto *disk_label_index = static_cast<DiskLabelIndex *>(indices_.label_index_.get());
  if (!disk_label_index->CreateIndex(label, SerializeVerticesForLabelIndex(label))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  if (!PersistLabelIndexCreation(label)) {
    return StorageIndexDefinitionError{IndexPersistenceError{}};
  }

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelIndices);

  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  auto *disk_label_property_index = static_cast<DiskLabelPropertyIndex *>(indices_.label_property_index_.get());
  if (!disk_label_property_index->CreateIndex(label, property,
                                              SerializeVerticesForLabelPropertyIndex(label, property))) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  if (!PersistLabelPropertyIndexAndExistenceConstraintCreation(label, property, label_property_index_str)) {
    return StorageIndexDefinitionError{IndexPersistenceError{}};
  }

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);

  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  if (!indices_.label_index_->DropIndex(label)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  if (!PersistLabelIndexDeletion(label)) {
    return StorageIndexDefinitionError{IndexPersistenceError{}};
  }

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelIndices);

  return {};
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  if (!indices_.label_property_index_->DropIndex(label, property)) {
    return StorageIndexDefinitionError{IndexDefinitionError{}};
  }

  if (!PersistLabelPropertyIndexAndExistenceConstraintDeletion(label, property, label_property_index_str)) {
    return StorageIndexDefinitionError{IndexPersistenceError{}};
  }

  // We don't care if there is a replication error because on main node the change will go through
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);

  return {};
}

utils::BasicResult<StorageExistenceConstraintDefinitionError, void> DiskStorage::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);

  if (constraints_.existence_constraints_->ConstraintExists(label, property)) {
    return StorageExistenceConstraintDefinitionError{ConstraintDefinitionError{}};
  }

  if (auto check = CheckExistingVerticesBeforeCreatingExistenceConstraint(label, property); check.has_value()) {
    return StorageExistenceConstraintDefinitionError{check.value()};
  }

  constraints_.existence_constraints_->InsertConstraint(label, property);

  if (!PersistLabelPropertyIndexAndExistenceConstraintCreation(label, property, existence_constraints_str)) {
    return StorageExistenceConstraintDefinitionError{ConstraintsPersistenceError{}};
  }

  return {};
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void> DiskStorage::DropExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  if (!constraints_.existence_constraints_->DropConstraint(label, property)) {
    return StorageExistenceConstraintDroppingError{ConstraintDefinitionError{}};
  }

  if (!PersistLabelPropertyIndexAndExistenceConstraintDeletion(label, property, existence_constraints_str)) {
    return StorageExistenceConstraintDroppingError{ConstraintsPersistenceError{}};
  }

  return {};
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
DiskStorage::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                    const std::optional<uint64_t> /*desired_commit_timestamp*/) {
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

  if (!PersistUniqueConstraintCreation(label, properties)) {
    return StorageUniqueConstraintDefinitionError{ConstraintsPersistenceError{}};
  }

  return UniqueConstraints::CreationStatus::SUCCESS;
}

utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
DiskStorage::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                  const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  std::unique_lock<utils::RWLock> storage_guard(main_lock_);
  auto ret = constraints_.unique_constraints_->DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }

  if (!PersistUniqueConstraintDeletion(label, properties)) {
    return StorageUniqueConstraintDroppingError{ConstraintsPersistenceError{}};
  }

  return UniqueConstraints::DeletionStatus::SUCCESS;
}

Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level, StorageMode storage_mode) {
  /// We acquire the transaction engine lock here because we access (and
  /// modify) the transaction engine variables (`transaction_id` and
  /// `timestamp`) below.
  uint64_t transaction_id = 0;
  uint64_t start_timestamp = 0;
  {
    std::lock_guard<utils::SpinLock> guard(engine_lock_);
    transaction_id = transaction_id_++;
    /// TODO: when we introduce replication to the disk storage, take care of start_timestamp
    start_timestamp = timestamp_++;
  }
  return {transaction_id, start_timestamp, isolation_level, storage_mode};
}

uint64_t DiskStorage::CommitTimestamp(const std::optional<uint64_t> desired_commit_timestamp) {
  if (!desired_commit_timestamp) {
    return timestamp_++;
  }
  timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
  return *desired_commit_timestamp;
}

}  // namespace memgraph::storage
