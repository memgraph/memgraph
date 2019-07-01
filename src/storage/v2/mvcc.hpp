#pragma once

#include "storage/v2/delta.hpp"
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
inline void ApplyDeltasForRead(Transaction *transaction, Delta *delta,
                               View view, const TCallback &callback) {
  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    auto cid = delta->command_id;

    // This is a committed change that we see so we shouldn't undo it.
    if (ts < transaction->start_timestamp) {
      break;
    }

    // We shouldn't undo our newest changes because the user requested a NEW
    // view of the database.
    if (view == View::NEW && ts == transaction->transaction_id &&
        cid <= transaction->command_id) {
      break;
    }

    // We shouldn't undo our older changes because the user requested a OLD view
    // of the database.
    if (view == View::OLD && ts == transaction->transaction_id &&
        cid < transaction->command_id) {
      break;
    }

    // This delta must be applied, call the callback.
    callback(*delta);

    // Move to the next delta.
    delta = delta->next.load(std::memory_order_acquire);
  }
}

/// This function prepares the Vertex object for a write. It checks whether
/// there are any serialization errors in the process (eg. the object can't be
/// written to from this transaction because it is being written to from another
/// transaction) and returns a `bool` value indicating whether the caller can
/// proceed with a write operation.
inline bool PrepareForWrite(Transaction *transaction, Vertex *vertex) {
  if (vertex->delta == nullptr) return true;

  auto ts = vertex->delta->timestamp->load(std::memory_order_acquire);
  if (ts == transaction->transaction_id || ts < transaction->start_timestamp) {
    return true;
  }

  transaction->must_abort = true;
  return false;
}

/// This function creates a delta in the transaction and returns a pointer to
/// the created delta. It doesn't perform any linking of the delta and is
/// primarily used to create the first delta for an object.
inline Delta *CreateDelta(Transaction *transaction, Delta::Action action,
                          uint64_t value) {
  return &transaction->deltas.emplace_back(
      action, value, &transaction->commit_timestamp, transaction->command_id);
}

/// This function creates a delta in the transaction for the Vertex object and
/// links the delta into the Vertex's delta list. It also adds the Vertex to the
/// transaction's modified vertices list.
inline void CreateAndLinkDelta(Transaction *transaction, Vertex *vertex,
                               Delta::Action action, uint64_t value) {
  auto delta = &transaction->deltas.emplace_back(
      action, value, &transaction->commit_timestamp, transaction->command_id);

  if (vertex->delta) {
    vertex->delta->prev = delta;
  }
  delta->next.store(vertex->delta, std::memory_order_release);
  vertex->delta = delta;

  if (transaction->modified_vertices.empty() ||
      transaction->modified_vertices.back() != vertex) {
    transaction->modified_vertices.push_back(vertex);
  }
}

}  // namespace storage
