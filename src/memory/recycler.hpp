#pragma once

#include <memory>
#include <queue>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <class T, class Allocator = std::allocator<T>>
class Recycler : public Lockable<SpinLock> {
  static constexpr size_t default_max_reserved = 100;

 public:
  Recycler() = default;
  Recycler(size_t max_reserved) : max_reserved(max_reserved) {}

  template <class... Args>
  T* acquire(Args&&... args) {
    auto guard = acquire_unique();
    return fetch_or_create(std::forward<Args>(args)...);
  }

  void release(T* item) {
    auto guard = acquire_unique();
    return recycle_or_delete(item);
  }

 protected:
  Allocator alloc;
  size_t max_reserved{default_max_reserved};
  std::queue<T*> items;

  template <class... Args>
  T* fetch_or_create(Args&&... args) {
    return new T(std::forward<Args>(args)...);  // todo refactor :D
  }

  void recycle_or_delete(T* item) {
    delete item;  // todo refactor :D
  }
};
