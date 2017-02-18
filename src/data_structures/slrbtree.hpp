#pragma once

#include <map>

#include "threading/sync/spinlock.hpp"

template <class K, class T>
class SlRbTree : Lockable<SpinLock> {
 public:
 private:
  std::map<K, T> tree;
};
