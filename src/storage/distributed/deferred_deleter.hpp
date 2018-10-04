#pragma once

#include <malloc.h>

#include <limits>
#include <list>

#include "glog/logging.h"
#include "mvcc/distributed/record.hpp"
#include "transactions/transaction.hpp"

/**
 * @brief - Implements deferred deletion.
 * @Tparam T - type of object to delete (Vertex/Edge/VersionList...)
 * This is NOT a thread-safe class.
 */
template <typename T>
class DeferredDeleter {
 public:
  /**
   * @brief - keep track of what object was deleted at which time.
   */
  struct DeletedObject {
    const T *object;
    const tx::TransactionId deleted_at;
    DeletedObject(const T *object, tx::TransactionId deleted_at)
        : object(object), deleted_at(deleted_at) {}
  };

  /**
   * @brief - check if everything is freed
   */
  ~DeferredDeleter() {
    CHECK(objects_.size() == 0U)
        << "Objects are not freed when calling the destructor.";
  }

  /**
   * @brief - Add objects to this deleter. This method assumes that it will
   * always be called with a non-decreasing sequence of `deleted_at`.
   * @param objects - vector of objects to add
   * @param last_transaction - nothing newer or equal to it can see these
   * objects
   */
  void AddObjects(const std::vector<DeletedObject> &objects) {
    auto previous_tx_id = objects_.empty()
                              ? std::numeric_limits<tx::TransactionId>::min()
                              : objects_.back().deleted_at;
    for (auto object : objects) {
      CHECK(previous_tx_id <= object.deleted_at)
          << "deleted_at must be non-decreasing";
      previous_tx_id = object.deleted_at;
      objects_.push_back(object);
    }
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
};
