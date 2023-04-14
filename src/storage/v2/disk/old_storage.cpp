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

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <variant>
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include "io/network/endpoint.hpp"
#include "storage/v2/disk/edge_accessor.hpp"
#include "storage/v2/disk/old_storage.hpp"
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

/*
This function will be the same for both storage modes. We can move it on a different place later.
*/
std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::CreateVertex() {
  OOMExceptionEnabler oom_exception;
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  return std::make_unique<DiskVertexAccessor>(&*it, &transaction_, &storage_->indices_, &storage_->constraints_,
                                              config_);
}

/*
Same as function above, but with a given GID.
*/
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
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  return std::make_unique<DiskVertexAccessor>(&*it, &transaction_, &storage_->indices_, &storage_->constraints_,
                                              config_);
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::DeserializeVertex(const std::string_view key,
                                                                             const std::string_view value) {
  /// Create vertex
  spdlog::info("Key to deserialize: {}", key);
  const auto vertex_parts = utils::Split(key, "|");
  auto impl = CreateVertex(storage::Gid::FromUint(std::stoull(vertex_parts[1])));
  // Deserialize labels
  if (!vertex_parts[0].empty()) {
    const auto labels = utils::Split(vertex_parts[0], ",");
    for (const auto &label : labels) {
      const storage::LabelId label_id = storage::LabelId::FromUint(std::stoull(label));
      // TODO: introduce error handling
      impl->AddLabel(label_id);
    }
  }
  impl->SetPropertyStore(value);
  return impl;
}

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::FindVertex(storage::Gid gid, View /*view*/) {
  auto it =
      std::unique_ptr<rocksdb::Iterator>(storage_->db_->NewIterator(rocksdb::ReadOptions(), storage_->vertex_chandle));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    const auto &key = it->key().ToString();
    if (const auto vertex_parts = utils::Split(key, "|"); vertex_parts[1] == SerializeIdType(gid)) {
      return DeserializeVertex(key, it->value().ToStringView());
    }
  }
  return {};
}

Result<std::unique_ptr<VertexAccessor>> DiskStorage::DiskAccessor::DeleteVertex(VertexAccessor *vertex) {
  if (!CheckRocksDBStatus(
          storage_->db_->Delete(rocksdb::WriteOptions(), storage_->vertex_chandle, SerializeVertex(vertex)))) {
    return Error::SERIALIZATION_ERROR;
  }
  return Result<std::unique_ptr<VertexAccessor>>{std::unique_ptr<VertexAccessor>(vertex)};
}

Result<std::optional<std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>>>
DiskStorage::DiskAccessor::DetachDeleteVertex(VertexAccessor *vertex) {
  using ReturnType = std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>;
  auto del_vertex = DeleteVertex(vertex);
  if (del_vertex.HasError()) {
    return del_vertex.GetError();
  }

  auto out_edges = vertex->OutEdges(storage::View::OLD);
  auto in_edges = vertex->InEdges(storage::View::OLD);
  if (out_edges.HasError() || in_edges.HasError()) {
    return out_edges.GetError();
  }
  // if (auto del_edges = DeleteEdges(*out_edges), del_in_edges = DeleteEdges(*in_edges);
  //     del_edges.has_value() && del_in_edges.has_value()) {
  //   del_edges->insert(del_in_edges->end(), std::make_move_iterator(del_in_edges->begin()),
  //                     std::make_move_iterator(del_in_edges->end()));
  //   return std::make_pair(*del_vertex, *del_edges);
  // }
  return Error::SERIALIZATION_ERROR;
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                            EdgeTypeId edge_type) {
  throw utils::NotYetImplemented("CreateEdge");
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                            EdgeTypeId edge_type, storage::Gid gid) {
  throw utils::NotYetImplemented("CreateEdge");
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::DeleteEdge(EdgeAccessor *edge) {
  throw utils::NotYetImplemented("DeleteEdge");
}

const std::string &DiskStorage::DiskAccessor::LabelToName(LabelId label) const { return storage_->LabelToName(label); }

const std::string &DiskStorage::DiskAccessor::PropertyToName(PropertyId property) const {
  return storage_->PropertyToName(property);
}

const std::string &DiskStorage::DiskAccessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return storage_->EdgeTypeToName(edge_type);
}

LabelId DiskStorage::DiskAccessor::NameToLabel(const std::string_view name) { return storage_->NameToLabel(name); }

PropertyId DiskStorage::DiskAccessor::NameToProperty(const std::string_view name) {
  return storage_->NameToProperty(name);
}

EdgeTypeId DiskStorage::DiskAccessor::NameToEdgeType(const std::string_view name) {
  return storage_->NameToEdgeType(name);
}

void DiskStorage::DiskAccessor::AdvanceCommand() { ++transaction_.command_id; }

utils::BasicResult<StorageDataManipulationError, void> DiskStorage::DiskAccessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("Commit");
}

void DiskStorage::DiskAccessor::Abort() { throw utils::NotYetImplemented("Abort"); }

void DiskStorage::DiskAccessor::FinalizeTransaction() { throw utils::NotYetImplemented("FinalizeTransaction"); }

std::optional<uint64_t> DiskStorage::DiskAccessor::GetTransactionId() const {
  throw utils::NotYetImplemented("GetTransactionId");
}

const std::string &DiskStorage::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

const std::string &DiskStorage::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

const std::string &DiskStorage::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

LabelId DiskStorage::NameToLabel(const std::string_view name) {
  return LabelId::FromUint(name_id_mapper_.NameToId(name));
}

PropertyId DiskStorage::NameToProperty(const std::string_view name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

EdgeTypeId DiskStorage::NameToEdgeType(const std::string_view name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateIndex");
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::CreateIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateIndex");
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropIndex");
}

utils::BasicResult<StorageIndexDefinitionError, void> DiskStorage::DropIndex(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropIndex");
}

IndicesInfo DiskStorage::ListAllIndices() const { throw utils::NotYetImplemented("ListAllIndices"); }

utils::BasicResult<StorageExistenceConstraintDefinitionError, void> DiskStorage::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateExistenceConstraint");
}

utils::BasicResult<StorageExistenceConstraintDroppingError, void> DiskStorage::DropExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropExistenceConstraint");
}

utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>
DiskStorage::CreateUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                    const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("CreateUniqueConstraint");
}

utils::BasicResult<StorageUniqueConstraintDroppingError, UniqueConstraints::DeletionStatus>
DiskStorage::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                  const std::optional<uint64_t> desired_commit_timestamp) {
  throw utils::NotYetImplemented("DropUniqueConstraint");
}

ConstraintsInfo DiskStorage::ListAllConstraints() const { throw utils::NotYetImplemented("ListAllConstraints"); }

StorageInfo DiskStorage::GetInfo() const { throw utils::NotYetImplemented("GetInfo"); }

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, View view) {
  throw utils::NotYetImplemented("Vertices");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, View view) {
  throw utils::NotYetImplemented("Vertices");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value,
                                                     View view) {
  throw utils::NotYetImplemented("Vertices");
}

VerticesIterable DiskStorage::DiskAccessor::Vertices(LabelId label, PropertyId property,
                                                     const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                     const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                     View view) {
  throw utils::NotYetImplemented("Vertices");
}

Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level) {
  throw utils::NotYetImplemented("CreateTransaction");
}

}  // namespace memgraph::storage
