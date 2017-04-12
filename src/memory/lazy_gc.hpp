#pragma once

// TODO: remove from here and from the project
#include <atomic>
#include <iostream>

#include "threading/sync/lockable.hpp"
#include "utils/crtp.hpp"

template <class Derived, class lock_t = SpinLock>
class LazyGC : public Crtp<Derived>, public Lockable<lock_t> {
 public:
  // AddRef method should be called by a thread
  // when the thread has to do something over
  // object which has to be lazy cleaned when
  // the thread finish it job
  void AddRef() {
    auto lock = this->acquire_unique();
    ++reference_count_;
  }

 protected:
  size_t reference_count_{0};
};
