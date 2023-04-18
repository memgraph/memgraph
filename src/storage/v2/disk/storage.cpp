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

#include "storage/v2/storage.hpp"
#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>
#include "storage/v2/id_types.hpp"
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

inline std::string DiskStorage::DiskAccessor::SerializeIdType(const auto &id) const {
  return std::to_string(id.AsUint());
}

inline auto DiskStorage::DiskAccessor::DeserializeIdType(const std::string &str) {
  return Gid::FromUint(std::stoull(str));
}

inline std::string DiskStorage::DiskAccessor::SerializeTimestamp(const uint64_t ts) { return std::to_string(ts); }

std::string DiskStorage::DiskAccessor::SerializeLabels(const auto &&labels) const {
  if (labels.HasError() || (*labels).empty()) {
    return "";
  }
  std::string result = std::to_string((*labels)[0].AsUint());
  std::string ser_labels = std::accumulate(
      std::next((*labels).begin()), (*labels).end(), result,
      [](const std::string &join, const auto &label_id) { return join + "," + std::to_string(label_id.AsUint()); });
  return ser_labels;
}

std::string DiskStorage::DiskAccessor::SerializeVertex(const VertexAccessor *vertex_acc) const {
  MG_ASSERT(commit_timestamp_.has_value(), "Transaction must be committed to serialize vertex.");
  std::string result = SerializeLabels(vertex_acc->Labels(storage::View::OLD)) + "|";
  result += SerializeIdType(vertex_acc->Gid()) + "|";
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

std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::DeserializeVertex(const std::string_view key,
                                                                             const std::string_view value) {
  /// Create vertex
  const std::vector<std::string> vertex_parts = utils::Split(key, "|");
  auto impl = CreateVertex(storage::Gid::FromUint(std::stoull(vertex_parts[1])));
  // Deserialize labels
  if (!vertex_parts[0].empty()) {
    const auto labels = utils::Split(vertex_parts[0], ",");
    std::for_each(labels.begin(), labels.end(), [impl = std::move(impl)](auto &label) {
      // TODO(andi): Introduce error handling of adding labels
      impl->AddLabel(storage::LabelId::FromUint(std::stoull(label)));
    });
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

VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label, property, value)");
}

VerticesIterable Vertices(LabelId label, PropertyId property,
                          const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                          const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  throw utils::NotYetImplemented("DiskStorage::DiskAccessor::Vertices(label, property, lower_bound, upper_bound)");
}

/*
This function will be the same for both storage modes. We can move it on a different place later.
*/
std::unique_ptr<VertexAccessor> DiskStorage::DiskAccessor::CreateVertex() {
  OOMExceptionEnabler oom_exception;
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = vertices_.access();
  auto delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  return std::make_unique<DiskVertexAccessor>(&*it, &transaction_, &storage_->indices_, &storage_->constraints_,
                                              config_, storage::Gid::FromUint(gid));
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
  auto [it, inserted] = acc.insert(Vertex{gid, delta});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&*it);
  return std::make_unique<DiskVertexAccessor>(&*it, &transaction_, &storage_->indices_, &storage_->constraints_,
                                              config_, gid);
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
  using ReturnType = std::vector<std::unique_ptr<EdgeAccessor>>;
  ReturnType edge_accs;
  for (auto &&it : edge_accessors) {
    if (const auto deleted_edge_res = DeleteEdge(it); !deleted_edge_res.HasError()) {
      return deleted_edge_res.GetError();
    }
    // edge_accs.push_back(std::make_unique<DiskEdgeAccessor>(std::move(it), nullptr, nullptr, nullptr));
  }
  return edge_accs;
}

Result<std::optional<std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>>>
DiskStorage::DiskAccessor::DetachDeleteVertex(VertexAccessor *vertex) {
  using ReturnType = std::pair<std::unique_ptr<VertexAccessor>, std::vector<std::unique_ptr<EdgeAccessor>>>;
  auto *disk_vertex_acc = dynamic_cast<DiskVertexAccessor *>(vertex);
  MG_ASSERT(disk_vertex_acc,
            "VertexAccessor must be from the same storage as the storage accessor when deleting a vertex!");
  MG_ASSERT(disk_vertex_acc->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = disk_vertex_acc->vertex_;

  auto del_vertex = DeleteVertex(vertex);
  if (del_vertex.HasError()) {
    return del_vertex.GetError();
  }

  auto out_edges = vertex->OutEdges(storage::View::OLD);
  auto in_edges = vertex->InEdges(storage::View::OLD);
  if (out_edges.HasError() || in_edges.HasError()) {
    return out_edges.GetError();
  }

  if (auto del_edges = DeleteEdges(*out_edges), del_in_edges = DeleteEdges(*in_edges);
      del_edges.HasError() && del_in_edges.HasError()) {
    // TODO: optimize this using splice
    del_edges->insert(del_in_edges->end(), std::make_move_iterator(del_in_edges->begin()),
                      std::make_move_iterator(del_in_edges->end()));
    return std::make_optional<ReturnType>(
        std::make_unique<DiskVertexAccessor>(vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_,
                                             config_, vertex_ptr->gid, true),
        del_edges.GetValue());
  }
  return Error::SERIALIZATION_ERROR;
}

Result<std::unique_ptr<EdgeAccessor>> DiskStorage::DiskAccessor::CreateEdge(VertexAccessor *from, VertexAccessor *to,
                                                                            EdgeTypeId edge_type, storage::Gid gid) {
  auto *inMemoryVAFrom = dynamic_cast<DiskVertexAccessor *>(from);
  auto *inMemoryVATo = dynamic_cast<DiskVertexAccessor *>(to);
  MG_ASSERT(inMemoryVAFrom,
            "Source VertexAccessor must be from the same storage as the storage accessor when creating an edge!");
  MG_ASSERT(inMemoryVATo,
            "Target VertexAccessor must be from the same storage as the storage accessor when creating an edge!");

  OOMExceptionEnabler oom_exception;
  MG_ASSERT(inMemoryVAFrom->transaction_ == inMemoryVATo->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(inMemoryVAFrom->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto from_vertex = inMemoryVAFrom->vertex_;
  auto to_vertex = inMemoryVATo->vertex_;

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

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto delta = CreateDeleteObjectDelta(&transaction_);
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
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = edges_.access();
    auto delta = CreateDeleteObjectDelta(&transaction_);
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
        // modifications before they are written to disk.
        // Replica can log only the write transaction received from Main
        // so the Wal files are consistent
        if (storage_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
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
          if (storage_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
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

// probably will need some usages here
void DiskStorage::DiskAccessor::Abort() { throw utils::NotYetImplemented("Abort"); }

// maybe will need some usages here
void DiskStorage::DiskAccessor::FinalizeTransaction() { throw utils::NotYetImplemented("FinalizeTransaction"); }

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

Transaction DiskStorage::CreateTransaction(IsolationLevel isolation_level) {
  throw utils::NotYetImplemented("CreateTransaction");
}

}  // namespace memgraph::storage
