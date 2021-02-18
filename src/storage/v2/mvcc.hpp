#pragma once

#include "storage/v2/delta.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

/// This function iterates through the undo buffers from an object (starting
/// from the supplied delta) and determines what deltas should be applied to get
/// the currently visible version of the object. When the function finds a delta
/// that should be applied it calls the callback function with the delta that
/// should be applied passed as a parameter to the callback. It is up to the
/// caller to apply the deltas.
template <typename TCallback>
inline void ApplyDeltasForRead(Transaction *transaction, const Delta *delta, View view, const TCallback &callback) {
  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    auto cid = delta->command_id;

    // This is a committed change that we see so we shouldn't undo it.
    if (ts < transaction->start_timestamp) {
      break;
    }

    // We shouldn't undo our newest changes because the user requested a NEW
    // view of the database.
    if (view == View::NEW && ts == transaction->transaction_id && cid <= transaction->command_id) {
      break;
    }

    // We shouldn't undo our older changes because the user requested a OLD view
    // of the database.
    if (view == View::OLD && ts == transaction->transaction_id && cid < transaction->command_id) {
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
  if (ts == transaction->transaction_id || ts < transaction->start_timestamp) {
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

}  // namespace storage
