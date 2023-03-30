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

#include <atomic>
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::storage {

/// This function iterates through the undo buffers from an object (starting
/// from the supplied delta) and determines what deltas should be applied to get
/// the currently visible version of the object. When the function finds a delta
/// that should be applied it calls the callback function with the delta that
/// should be applied passed as a parameter to the callback. It is up to the
/// caller to apply the deltas.
template <typename TCallback>
inline void ApplyDeltasForRead(Transaction *transaction, const Delta *delta, View view, const TCallback &callback) {
  // if the transaction is not committed, then its deltas have transaction_id for the timestamp, otherwise they have
  // its commit timestamp set.
  // This allows the transaction to see its changes even though it's committed.
  const auto commit_timestamp = transaction->commit_timestamp
                                    ? transaction->commit_timestamp->load(std::memory_order_acquire)
                                    : transaction->transaction_id.load(std::memory_order_acquire);
  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    auto cid = delta->command_id;

    // For SNAPSHOT ISOLATION -> we can only see the changes which were committed before the start of the current
    // transaction
    //
    // For READ COMMITTED -> we can only see the changes which are committed. Commit timestamps of
    // uncommitted changes are set to the transaction id of the transaction that made the change. Transaction id is
    // always higher than start or commit timestamps so we know if the timestamp is lower than the initial transaction
    // id value, that the change is committed.
    //
    // For READ UNCOMMITTED -> we accept any change.
    if ((transaction->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION && ts < transaction->start_timestamp) ||
        (transaction->isolation_level == IsolationLevel::READ_COMMITTED && ts < kTransactionInitialId) ||
        (transaction->isolation_level == IsolationLevel::READ_UNCOMMITTED)) {
      break;
    }

    // We shouldn't undo our newest changes because the user requested a NEW
    // view of the database.
    if (view == View::NEW && ts == commit_timestamp && cid <= transaction->command_id) {
      break;
    }

    // We shouldn't undo our older changes because the user requested a OLD view
    // of the database.
    if (view == View::OLD && ts == commit_timestamp && cid < transaction->command_id) {
      break;
    }

    // This delta must be applied, call the callback.
    callback(*delta);

    // Move to the next delta.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

/// This function prepares the object for a write. It checks whether there are
/// any serialization errors in the process (eg. the object can't be written to
/// from this transaction because it is being written to from another
/// transaction) and returns a `bool` value indicating whether the caller can
/// proceed with a write operation.
template <typename TObj>
inline bool PrepareForWrite(Transaction *transaction, TObj *object) {
  if (object->delta == nullptr) return true;

  auto ts = object->delta->timestamp->load(std::memory_order_acquire);
  if (ts == transaction->transaction_id.load(std::memory_order_acquire) || ts < transaction->start_timestamp) {
    return true;
  }

  transaction->must_abort = true;
  return false;
}

/// This function creates a `DELETE_OBJECT` delta in the transaction and returns
/// a pointer to the created delta. It doesn't perform any linking of the delta
/// and is primarily used to create the first delta for an object (that must be
/// a `DELETE_OBJECT` delta).
/// @throw std::bad_alloc
inline Delta *CreateDeleteObjectDelta(Transaction *transaction) {
  transaction->EnsureCommitTimestampExists();
  return &transaction->deltas.emplace_back(Delta::DeleteObjectTag(), transaction->commit_timestamp.get(),
                                           transaction->command_id);
}

/// This function creates a delta in the transaction for the object and links
/// the delta into the object's delta list.
/// @throw std::bad_alloc
template <typename TObj, class... Args>
inline void CreateAndLinkDelta(Transaction *transaction, TObj *object, Args &&...args) {
  transaction->EnsureCommitTimestampExists();
  auto delta = &transaction->deltas.emplace_back(std::forward<Args>(args)..., transaction->commit_timestamp.get(),
                                                 transaction->command_id);

  // The operations are written in such order so that both `next` and `prev`
  // chains are valid at all times. The chains must be valid at all times
  // because garbage collection (which traverses the chains) is done
  // concurrently (as well as other execution threads).

  // 1. We need to set the next delta of the new delta to the existing delta.
  delta->next.store(object->delta, std::memory_order_release);
  // 2. We need to set the previous delta of the new delta to the object.
  delta->prev.Set(object);
  // 3. We need to set the previous delta of the existing delta to the new
  // delta. After this point the garbage collector will be able to see the new
  // delta but won't modify it until we are done with all of our modifications.
  if (object->delta) {
    object->delta->prev.Set(delta);
  }
  // 4. Finally, we need to set the object's delta to the new delta. The garbage
  // collector and other transactions will acquire the object lock to read the
  // delta from the object. Because the lock is held during the whole time this
  // modification is being done, everybody else will wait until we are fully
  // done with our modification before they read the object's delta value.
  object->delta = delta;
}

}  // namespace memgraph::storage
