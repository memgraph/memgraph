#pragma once

// TODO: remove from here and from the project
#include <functional>
#include <iostream>
#include <thread>

#include "logging/loggable.hpp"
#include "memory/freelist.hpp"
#include "memory/lazy_gc.hpp"
#include "threading/pool.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/assert.hpp"

template <class T, class lock_t = SpinLock>
class SkiplistGC : public LazyGC<SkiplistGC<T, lock_t>, lock_t>,
                   public Loggable {
 public:
  SkiplistGC() : Loggable("SkiplistGC") {}

  /**
   * ReleaseRef method should be called by some thread which finishes access to
   * skiplist. If thread reference_count_ becomes zero, all objects in the
   * local_freelist are going to be deleted. The only problem with this approach
   * is that GC may never be called, but for now we can deal with that.
   */
  void ReleaseRef() {
    // This has to be a shared_ptr since std::function requires that the
    // callable object be copy-constructable.
    std::shared_ptr<std::vector<T *>> local_freelist =
        std::make_shared<std::vector<T *>>();

    // take freelist if there is no more threads
    {
      auto lock = this->acquire_unique();
      debug_assert(this->reference_count_ > 0, "Count is equal to zero.");
      --this->reference_count_;
      if (this->reference_count_ == 0) {
        freelist_.swap(*local_freelist);
      }
    }

    if (local_freelist->size() > 0) {
      thread_pool_.run(std::bind(
          [this](std::shared_ptr<std::vector<T *>> local_freelist) {
            logger.trace("GC started");
            logger.trace("Local list size: {}", local_freelist->size());
            long long destroyed = 0;
            // destroy all elements from local_freelist
            for (auto element : *local_freelist) {
              if (element->flags.is_marked()) {
                T::destroy(element);
                destroyed++;
              } else {
                logger.warn(
                    "Unmarked node appeared in the collection ready for "
                    "destruction.");
              }
            }
            logger.trace("Number of destroyed elements: {}", destroyed);
          },
          local_freelist));
    }
  }

  void Collect(T *node) { freelist_.add(node); }

 private:
  // We use FreeList since it's thread-safe.
  FreeList<T *> freelist_;
  Pool thread_pool_;
};
