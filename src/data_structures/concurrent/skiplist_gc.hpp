#pragma once

#include <malloc.h>

#include <list>
#include <mutex>
#include <utility>

#include "gflags/gflags.h"

#include "data_structures/concurrent/concurrent_list.hpp"
#include "logging/loggable.hpp"

#include "threading/sync/spinlock.hpp"
#include "utils/executioner.hpp"

DECLARE_int32(skiplist_gc_interval);

/**
 * @brief Garbage collects nodes.
 * We are doing garbage collection by keeping track of alive accessors which
 * were requested from the parent skiplist. When some prefix [id, id+n] of
 * accessors becomes dead we try to empty the collection of (accessors_id,
 * entry*) with the id of that last dead accessor. Each entry is added to
 * collection after it has been re-linked and can't be seen by any accessors
 * created after that time and that marks the safe time for deleting entry.
 * @Tparam TNode - type of underlying pointer to objects which will be
 * collected.
 */
template <class TNode>
class SkipListGC : public Loggable {
 public:
  explicit SkipListGC() : Loggable("SkipListGc") {
    executor_job_id_ = GetExecutioner().RegisterJob(
        std::bind(&SkipListGC::GarbageCollect, this));
  }

  ~SkipListGC() {
    // We have to unregister the job because otherwise Executioner might access
    // some member variables of this class after it has been destructed.
    GetExecutioner().UnRegisterJob(executor_job_id_);
    for (auto it = deleted_list_.begin(); it != deleted_list_.end(); ++it) {
      TNode::destroy(it->second);
      it.remove();
    }
  }

  /**
   * @brief - Returns instance of executioner shared between all SkipLists.
   */
  auto &GetExecutioner() {
    static Executioner executioner(
        (std::chrono::seconds(FLAGS_skiplist_gc_interval)));

    return executioner;
  }

  SkipListGC(const SkipListGC &other) = delete;
  SkipListGC(SkipListGC &&other) = delete;
  SkipListGC operator=(const SkipListGC &other) = delete;
  SkipListGC operator=(SkipListGC &&other) = delete;

  /**
   * @brief - Keep track of each accessor with it's status, so we know which
   * ones are alive and which ones are dead.
   */
  struct AccessorStatus {
    AccessorStatus(const int64_t id, bool alive) : id_(id), alive_(alive) {}

    AccessorStatus(AccessorStatus &&other) = default;

    AccessorStatus(const AccessorStatus &other) = delete;
    AccessorStatus operator=(const AccessorStatus &other) = delete;
    AccessorStatus operator=(AccessorStatus &&other) = delete;

    const int64_t id_{-1};
    bool alive_{false};
  };

  /**
   * @brief - Creates a new accessors and returns reference to it's status. This
   * method is thread-safe.
   */
  AccessorStatus &CreateNewAccessor() {
    std::unique_lock<std::mutex> lock(mutex_);
    accessors_.emplace_back(++last_accessor_id_, true);
    return accessors_.back();
  }

  /**
   * @brief - Destroys objects which were previously collected and can be safely
   * removed. This method is not thread-safe.
   */
  void GarbageCollect() {
    std::unique_lock<std::mutex> lock(mutex_);
    auto last_dead_accessor = accessors_.end();
    for (auto it = accessors_.begin(); it != accessors_.end(); ++it) {
      if (it->alive_) break;
      last_dead_accessor = it;
    }
    // We didn't find any dead accessor and that means we are not sure that we
    // can delete anything.
    if (last_dead_accessor == accessors_.end()) return;
    // We don't need lock anymore because we are not modifying this structure
    // anymore, or accessing it any further down.
    const int64_t safe_id = last_dead_accessor->id_;
    accessors_.erase(accessors_.begin(), ++last_dead_accessor);
    lock.unlock();

    // We can only modify this in a not-thread safe way because we are the only
    // thread ever accessing it here, i.e. there is at most one thread doing
    // this GarbageCollection.

    // find the oldest not deletable record
    // since we can't copy a concurrent list iterator, we must use two
    // separate ones, while ensuring they point to the same record
    // in the beginning of the search-loop
    auto it = deleted_list_.begin();
    auto oldest_not_deletable = deleted_list_.begin();
    while (oldest_not_deletable != it) oldest_not_deletable++;
    // we need a bool to track if oldest_not_deletable should get
    // deleted too (we have not skipped any record due to safe_id condition)
    bool delete_all = true;
    for (; it != deleted_list_.end(); ++it) {
      if (it->first > safe_id) {
        while (oldest_not_deletable != it) ++oldest_not_deletable;
        delete_all = false;
      }
    }

    // deleted_list is already empty, nothing to delete here.
    if (oldest_not_deletable == deleted_list_.end()) return;

    // In case we didn't find anything that we can't delete we shouldn't
    // increment this because that would mean we skip over the first record
    // which is ready for destruction.
    if (!delete_all) ++oldest_not_deletable;
    int64_t destroyed = 0;
    for (auto &it = oldest_not_deletable; it != deleted_list_.end(); ++it) {
      TNode::destroy(it->second);
      it.remove();
      ++destroyed;
    }
    if (destroyed) logger.trace("Number of destroyed elements: {}", destroyed);
  }

  /**
   * @brief - Collect object for garbage collection. Call to this method means
   * that no new accessor can possibly access the object by iterating over some
   * storage.
   */
  void Collect(TNode *object) {
    // We can afford some inaccuary here - it's possible that some new accessor
    // incremented the last_accessor_id after we enter this method and as such
    // we might be a bit pessimistic here.
    deleted_list_.begin().push(
        std::make_pair(last_accessor_id_.load(), object));
  }

 private:
  int64_t executor_job_id_{-1};
  std::mutex mutex_;
  std::mutex singleton_mutex_;

  // List of accesssors from begin to end by an increasing id.
  std::list<AccessorStatus> accessors_;
  std::atomic<int64_t> last_accessor_id_{0};

  // List of pairs of accessor_ids and pointers to entries which should be
  // destroyed sorted approximately descendingly by id.
  ConcurrentList<std::pair<int64_t, TNode *>> deleted_list_;
};
