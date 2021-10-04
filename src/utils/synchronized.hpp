// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <concepts>
#include <mutex>
#include <shared_mutex>
#include <utility>

namespace utils {

template <typename TMutex>
concept SharedMutex = requires(TMutex mutex) {
  mutex.lock();
  mutex.unlock();
  mutex.lock_shared();
  mutex.unlock_shared();
};

/// A simple utility for easier mutex-based concurrency (influenced by
/// Facebook's Folly)
///
/// Many times we have an object that is accessed from multiple threads so it
/// has an associated lock:
///
/// utils::SpinLock my_important_map_lock_;
/// std::map<uint64_t, std::string> my_important_map_;
///
/// Whenever we want to access the object, we have to remember that we have to
/// acquire the corresponding lock:
///
/// std::lock_guard<utils::SpinLock>
/// my_important_map_guard(my_important_map_lock_);
/// my_important_map_[key] = value;
///
/// Correctness of this approach depends on the programmer never forgetting to
/// acquire the lock.
///
/// Synchronized encodes that information in the type information, and it is
/// much harder to use the object incorrectly.
///
/// Synchronized<std::map<uint64_t, std::string>, utils::SpinLock>
///     my_important_map_;
///
/// Now we have multiple ways of accessing the map:
///
///  1. Acquiring a locked pointer:
///     auto my_map_ptr = my_important_map_.Lock();
///     my_map_ptr->emplace(key, value);
///
///  2. Using the indirection operator:
///
///     my_important_map_->emplace(key, value);
///
///  3. Using a lambda:
///     my_important_map_.WithLock([](auto &my_important_map) {
///       my_important_map[key] = value;
///     });
///
///  Approach 2 is probably the best to use for one-line operations, and
///  approach 3 for multi-line ops.
template <class T, class TMutex = std::mutex>
class Synchronized {
 public:
  template <class... Args>
  explicit Synchronized(Args &&...args) : object_(std::forward<Args>(args)...) {}

  Synchronized(const Synchronized &) = delete;
  Synchronized(Synchronized &&) = delete;
  Synchronized &operator=(const Synchronized &) = delete;
  Synchronized &operator=(Synchronized &&) = delete;
  ~Synchronized() = default;

  class LockedPtr {
   private:
    friend class Synchronized<T, TMutex>;

    LockedPtr(T *object_ptr, TMutex *mutex) : object_ptr_(object_ptr), guard_(*mutex) {}

   public:
    T *operator->() { return object_ptr_; }
    T &operator*() { return *object_ptr_; }

   private:
    T *object_ptr_;
    std::lock_guard<TMutex> guard_;
  };

  class ReadLockedPtr {
   private:
    friend class Synchronized<T, TMutex>;

    ReadLockedPtr(const T *object_ptr, TMutex *mutex) : object_ptr_(object_ptr), guard_(*mutex) {}

   public:
    const T *operator->() const { return object_ptr_; }
    const T &operator*() const { return *object_ptr_; }

   private:
    const T *object_ptr_;
    std::shared_lock<TMutex> guard_;
  };

  LockedPtr Lock() { return LockedPtr(&object_, &mutex_); }

  template <class TCallable>
  decltype(auto) WithLock(TCallable &&callable) {
    return callable(*Lock());
  }

  LockedPtr operator->() { return LockedPtr(&object_, &mutex_); }

  template <typename = void>
  requires SharedMutex<TMutex> ReadLockedPtr ReadLock()
  const { return ReadLockedPtr(&object_, &mutex_); }

  template <class TCallable>
  requires SharedMutex<TMutex>
  decltype(auto) WithReadLock(TCallable &&callable) const { return callable(*ReadLock()); }

  template <typename = void>
  requires SharedMutex<TMutex> ReadLockedPtr operator->() const { return ReadLockedPtr(&object_, &mutex_); }

 private:
  T object_;
  mutable TMutex mutex_;
};

}  // namespace utils
