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

#include "spdlog/spdlog.h"

#include "storage/v2/disk/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/stat.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

Storage::Storage(Config config, StorageMode storage_mode)
    : name_id_mapper_(std::invoke([config, storage_mode]() -> std::unique_ptr<NameIdMapper> {
        if (storage_mode == StorageMode::ON_DISK_TRANSACTIONAL) {
          return std::make_unique<DiskNameIdMapper>(config.disk.name_id_mapper_directory,
                                                    config.disk.id_name_mapper_directory);
        }
        return std::make_unique<NameIdMapper>();
      })),
      config_(config),
      isolation_level_(config.transaction.isolation_level),
      storage_mode_(storage_mode),
      indices_(&constraints_, config, storage_mode),
      constraints_(config, storage_mode) {}

Storage::Accessor::Accessor(Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
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

utils::BasicResult<Storage::SetIsolationLevelError> Storage::SetIsolationLevel(IsolationLevel isolation_level) {
  std::unique_lock main_guard{main_lock_};
  if (storage_mode_ == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    return Storage::SetIsolationLevelError::DisabledForAnalyticalMode;
  }

  isolation_level_ = isolation_level;
  return {};
}

StorageMode Storage::Accessor::GetCreationStorageMode() const { return creation_storage_mode_; }

std::optional<uint64_t> Storage::Accessor::GetTransactionId() const {
  if (is_transaction_active_) {
    return transaction_.transaction_id.load(std::memory_order_acquire);
  }
  return {};
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

}  // namespace memgraph::storage
