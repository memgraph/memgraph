#pragma once

#include <malloc.h>

#include <limits>
#include <list>

#include "glog/logging.h"
#include "storage/distributed/mvcc/record.hpp"
#include "transactions/transaction.hpp"

/**
 * @brief - Implements deferred deletion.
 * @Tparam T - type of object to delete (Vertex/Edge/VersionList...)
 * This is NOT a thread-safe class.
 */
template <typename T>
class DeferredDeleter {
 private:
  struct DeletedObject {
    T *object;
    tx::TransactionId deleted_at;
  };

 public:
  /**
   * @brief - check if everything is freed
   */
  ~DeferredDeleter() {
    CHECK(objects_.size() == 0U)
        << "Objects are not freed when calling the destructor.";
  }

  /**
   * @brief - Add object to this deleter. This method assumes that it will
   * always be called with a non-decreasing sequence of `deleted_at`.
   * @param object - object to add
   * @param deleted_at - when was the object deleted
   */
  void AddObject(T *object, tx::TransactionId deleted_at) {
    CHECK(previous_tx_id_ <= deleted_at) << "deleted_at must be non-decreasing";
    previous_tx_id_ = deleted_at;
    objects_.push_back(DeletedObject{object, deleted_at});
  }

  /**
   * @brief - Free memory of objects deleted before the id.
   * @param id - delete before this id
   */
  void FreeExpiredObjects(tx::TransactionId id) {
    auto it = objects_.begin();
    while (it != objects_.end() && it->deleted_at < id) {
      delete it->object;
      ++it;
    }
    objects_.erase(objects_.begin(), it);
  }

  /**
   * @brief - Return number of stored objects.
   */
  size_t Count() { return objects_.size(); }

 private:
  // Ascendingly sorted list of deleted objects by `deleted_at`.
  std::list<DeletedObject> objects_;
  // Last transaction ID that had deleted objects
  tx::TransactionId previous_tx_id_{0};
};
