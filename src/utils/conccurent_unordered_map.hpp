// Copyright 2024 Memgraph Ltd.
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

#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "utils/rw_lock.hpp"

namespace memgraph::utils {

// TODO Expand template to alloc hash eq
template <typename K, typename T>
class ConcurrentUnorderedMap {
 public:
  template <typename TAnyKey>
  T &get_or_insert(const TAnyKey &key) {
    // Attempt 1: Look for an already existing element
    {
      auto l = std::shared_lock{mtx_};
      auto itr = map_.find(key);
      if (itr != map_.end()) return itr->second;
    }
    // Attempt 2: Need to insert (unique lock)
    {
      // NOTE: Since we lock and unlock, this emplace can still fail if someone inserted in the meantime
      auto l = std::unique_lock{mtx_};
      if constexpr (std::is_same_v<K, TAnyKey>) {
        const auto &[itr, success] = map_.emplace(key, 0);
        return itr->second;
      } else {
        static_assert(std::is_constructible_v<K, TAnyKey>);
        const auto &[itr, success] = map_.emplace(K{key}, 0);
        return itr->second;
      }
    }
  }

  template <typename TAnyKey>
  T &operator[](const TAnyKey &key) {
    return get_or_insert(key);
  }

  void clear() {
    auto l = std::unique_lock{mtx_};
    map_.clear();
  }

  template <typename TAnyKey>
  auto find(const TAnyKey &key) const {
    auto l = std::shared_lock{mtx_};
    auto itr = map_.find(key);
    return ConstIterator{std::move(itr), std::move(l)};
  }

  size_t size() const {
    auto l = std::shared_lock{mtx_};
    return map_.size();
  }

  void erase(const K &key) {
    auto l = std::unique_lock{mtx_};
    map_.erase(key);
  }

  template <typename... Args>
  auto emplace(Args &&...args) {
    auto l = std::unique_lock{mtx_};
    return map_.emplace(std::forward<Args>(args)...);
  }

  template <typename F>
  auto erase_if(F &&f) {
    auto l = std::unique_lock{mtx_};
    return std::erase_if(map_, std::forward<F>(f));
  }

  template <typename F>
  void for_each(F &&f) const {
    auto l = std::shared_lock{mtx_};
    for (const auto &[k, v] : map_) {
      f(k, v);
    }
  }

  template <typename F>
  void for_each(F &&f) {
    auto l = std::unique_lock{mtx_};
    for (auto &[k, v] : map_) {
      f(k, v);
    }
  }

  struct Iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using reference = value_type &;
    using pointer = value_type *;
    using difference_type = std::ptrdiff_t;

    Iterator(auto itr, auto lock) : itr_{std::move(itr)}, lock_{std::move(lock)} {}
    Iterator(auto itr) : itr_{std::move(itr)} {}

    auto operator++() -> Iterator & {
      ++itr_;
      return *this;
    }

    auto &operator*() const { return *itr_; }

    auto *operator->() const { return &*itr_; }

    friend bool operator==(Iterator const &lhs, Iterator const &rhs) { return lhs.itr_ == rhs.itr_; }

   private:
    std::unordered_map<K, T>::iterator itr_;
    std::unique_lock<ReadPrioritizedRWLock> lock_;
  };

  struct ConstIterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using reference = value_type const &;
    using pointer = value_type *;
    using difference_type = std::ptrdiff_t;

    ConstIterator(auto itr, auto lock) : itr_{std::move(itr)}, lock_{std::move(lock)} {}
    ConstIterator(auto itr) : itr_{std::move(itr)} {}

    auto operator++() -> ConstIterator & {
      ++itr_;
      return *this;
    }

    const auto &operator*() const { return *itr_; }

    const auto *operator->() const { return &*itr_; }

    friend bool operator==(ConstIterator const &lhs, ConstIterator const &rhs) { return lhs.itr_ == rhs.itr_; }

   private:
    std::unordered_map<K, T>::const_iterator itr_;
    std::shared_lock<ReadPrioritizedRWLock> lock_;
  };

  ConstIterator begin() const {
    auto lock = std::shared_lock{mtx_};
    return ConstIterator{map_.begin(), std::move(lock)};
  }

  ConstIterator end() const { return ConstIterator{map_.end()}; }

  Iterator begin() {
    auto lock = std::unique_lock{mtx_};
    return Iterator{map_.begin(), std::move(lock)};
  }

  Iterator end() { return Iterator{map_.end()}; }

 private:
  std::unordered_map<K, T> map_;
  mutable ReadPrioritizedRWLock mtx_;
};

}  // namespace memgraph::utils
