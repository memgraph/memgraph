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
#include "spdlog/spdlog.h"
#include "storage/v2/transaction.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/stat.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

Storage::Storage(Config config, StorageMode storage_mode)
    : config_(config),
      snapshot_directory_(config.durability.storage_directory / durability::kSnapshotDirectory),
      wal_directory_(config.durability.storage_directory / durability::kWalDirectory),
      lock_file_path_(config.durability.storage_directory / durability::kLockFile),
      isolation_level_(config.transaction.isolation_level),
      storage_mode_(storage_mode),
      indices_(&constraints_, config, storage_mode),
      constraints_(config, storage_mode),
      uuid_(utils::GenerateUUID()),
      epoch_id_(utils::GenerateUUID()),
      global_locker_(file_retainer_.AddLocker()) {}

Storage::Accessor::Accessor(Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : storage_(storage),
      storage_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction(isolation_level, storage_mode)),
      is_transaction_active_(true),
      creation_storage_mode_(storage_mode) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      storage_guard_(std::move(other.storage_guard_)),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_),
      creation_storage_mode_(other.creation_storage_mode_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

bool Storage::LockPath() {
  auto locker_accessor = global_locker_.Access();
  return locker_accessor.AddPath(config_.durability.storage_directory);
}

bool Storage::UnlockPath() {
  {
    auto locker_accessor = global_locker_.Access();
    if (!locker_accessor.RemovePath(config_.durability.storage_directory)) {
      return false;
    }
  }

  // We use locker accessor in seperate scope so we don't produce deadlock
  // after we call clean queue.
  file_retainer_.CleanQueue();
  return true;
}

// this should be handled on an above level of abstraction
const std::string &Storage::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

// this should be handled on an above level of abstraction
const std::string &Storage::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

// this should be handled on an above level of abstraction
const std::string &Storage::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

// this should be handled on an above level of abstraction
LabelId Storage::NameToLabel(const std::string_view name) { return LabelId::FromUint(name_id_mapper_.NameToId(name)); }

// this should be handled on an above level of abstraction
PropertyId Storage::NameToProperty(const std::string_view name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

// this should be handled on an above level of abstraction
EdgeTypeId Storage::NameToEdgeType(const std::string_view name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

IndicesInfo Storage::ListAllIndices() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {indices_.label_index_->ListIndices(), indices_.label_property_index_->ListIndices()};
}

ConstraintsInfo Storage::ListAllConstraints() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {constraints_.existence_constraints_->ListConstraints(), constraints_.unique_constraints_->ListConstraints()};
}

/// Main lock is taken by the caller.
void Storage::SetStorageMode(StorageMode storage_mode) {
  std::unique_lock main_guard{main_lock_};
  MG_ASSERT(storage_mode_ != StorageMode::ON_DISK_TRANSACTIONAL && storage_mode != StorageMode::ON_DISK_TRANSACTIONAL);
  storage_mode_ = storage_mode;
}

StorageMode Storage::GetStorageMode() const { return storage_mode_; }

const std::string &Storage::Accessor::LabelToName(LabelId label) const { return storage_->LabelToName(label); }

const std::string &Storage::Accessor::PropertyToName(PropertyId property) const {
  return storage_->PropertyToName(property);
}

const std::string &Storage::Accessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return storage_->EdgeTypeToName(edge_type);
}

LabelId Storage::Accessor::NameToLabel(const std::string_view name) { return storage_->NameToLabel(name); }

PropertyId Storage::Accessor::NameToProperty(const std::string_view name) { return storage_->NameToProperty(name); }

EdgeTypeId Storage::Accessor::NameToEdgeType(const std::string_view name) { return storage_->NameToEdgeType(name); }

StorageMode Storage::Accessor::GetCreationStorageMode() const { return creation_storage_mode_; }

std::optional<uint64_t> Storage::Accessor::GetTransactionId() const {
  if (is_transaction_active_) {
    return transaction_.transaction_id.load(std::memory_order_acquire);
  }
  return {};
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

}  // namespace memgraph::storage
