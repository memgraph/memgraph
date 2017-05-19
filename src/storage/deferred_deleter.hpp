#pragma once

#include <malloc.h>

#include <list>

#include "mvcc/id.hpp"
#include "mvcc/record.hpp"
#include "utils/assert.hpp"

/**
 * @brief - Implements deferred deletion.
 * @Tparam T - type of object to delete (Vertex/Edge/VersionList...)
 * This is NOT a thread-safe class.
 */
template <typename T>
class DeferredDeleter {
 public:
  /**
   * @brief - check if everything is freed
   */
  ~DeferredDeleter() {
    permanent_assert(objects_.size() == 0,
                     "Objects are not freed when calling the destructor.");
  }

  /**
   * @brief - Add objects to this deleter. This method assumes that it will
   * always be called with a non-decreasing sequence of `last_transaction`.
   * @param objects - vector of objects to add
   * @param last_transaction - nothing newer or equal to it can see these
   * objects
   */
  void AddObjects(const std::vector<T *> &objects, const Id &last_transaction) {
    debug_assert(
        objects_.size() == 0 || objects_.back().deleted_at <= last_transaction,
        "Transaction ids are not non-decreasing.");
    for (auto object : objects)
      objects_.emplace_back(DeletedObject(object, last_transaction));
  }

  /**
   * @brief - Free memory of objects deleted before the id.
   * @param id - delete before this id
   */
  void FreeExpiredObjects(const Id &id) {
    auto it = objects_.begin();
    while (it != objects_.end() && it->deleted_at < id) {
      delete it->object;
      ++it;
    }
    objects_.erase(objects_.begin(), it);
    // After deleting objects - to force release of now deleted (free) memory
    // back to OS we need to call malloc_trim. This will force memgraph to
    // return memory from top of the heap to OS. If we don't use this, it seems
    // to the outside observer we are leaking memory while in fact we are not,
    // we were just keeping it close by in case we need it in near future.
    malloc_trim(0);
  }

  /**
   * @brief - Return number of stored objects.
   */
  size_t Count() { return objects_.size(); }

 private:
  /**
   * @brief - keep track of what object was deleted at which time.
   */
  struct DeletedObject {
    const T *object;
    const Id deleted_at;
    DeletedObject(T *object, const Id &deleted_at)
        : object(object), deleted_at(deleted_at) {}
  };

  // Ascendingly sorted list of deleted objects by `deleted_at`.
  std::list<DeletedObject> objects_;
};
