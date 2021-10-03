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

/// @file

#pragma once

#include <optional>
#include <unordered_map>

#include "utils/logging.hpp"

namespace utils {
namespace impl {
template <typename TKey, typename TValue>
struct Node {
  TKey key;
  TValue value;
  Node *prev{nullptr};
  Node *next{nullptr};
};

/// Helper class used for maintaining lru order.
template <typename TKey, typename TValue>
class LruList {
 public:
  LruList() = default;
  LruList(const LruList &) = delete;
  LruList(LruList &&) = delete;
  LruList &operator=(const LruList &) = delete;
  LruList &operator=(LruList &&) = delete;
  ~LruList() { Clear(); }

  Node<TKey, TValue> *AddPageToHead(const TKey &key, const TValue &value) {
    auto *page = new Node<TKey, TValue>{key, value};
    if (!front_ && !rear_) {
      front_ = rear_ = page;
    } else {
      MG_ASSERT(front_ != nullptr && rear_ != nullptr, "Both front_ and rear_ must be valid");
      page->next = front_;
      front_->prev = page;
      front_ = page;
    }
    return page;
  }

  void MovePageToHead(Node<TKey, TValue> *page) {
    MG_ASSERT(front_ != nullptr && rear_ != nullptr, "Both front_ and rear_ must be valid");
    if (page == front_) {
      return;
    }
    if (page == rear_) {
      rear_ = rear_->prev;
      rear_->next = nullptr;
    } else {
      page->prev->next = page->next;
      page->next->prev = page->prev;
    }

    page->next = front_;
    page->prev = nullptr;
    front_->prev = page;
    front_ = page;
  }

  void RemoveRearPage() {
    if (IsEmpty()) {
      return;
    }
    if (front_ == rear_) {
      delete rear_;
      front_ = rear_ = nullptr;
    } else {
      auto *temp = rear_;
      rear_ = rear_->prev;
      rear_->next = nullptr;
      delete temp;
    }
  }

  Node<TKey, TValue> *Rear() { return rear_; }

  void Clear() {
    while (!IsEmpty()) {
      RemoveRearPage();
    }
  }

  bool IsEmpty() const { return rear_ == nullptr; }

 private:
  Node<TKey, TValue> *front_{nullptr};
  Node<TKey, TValue> *rear_{nullptr};
};
}  // namespace impl

/// Used for caching objects. Uses least recently used page replacement
/// algorithm for evicting elements when maximum size is reached. This class
/// is NOT thread safe.
///
/// @tparam TKey - any object that has hash() defined
/// @tparam TValue - any object
template <typename TKey, typename TValue>
class LruCache {
 public:
  explicit LruCache(size_t capacity) : capacity_(capacity) {}

  LruCache(const LruCache &) = delete;
  LruCache(LruCache &&) = delete;
  LruCache &operator=(const LruCache &) = delete;
  LruCache &operator=(LruCache &&) = delete;
  ~LruCache() = default;

  std::optional<TValue> Find(const TKey &key) {
    auto found = access_map_.find(key);
    if (found == access_map_.end()) {
      return std::nullopt;
    }

    // move the page to front
    lru_order_.MovePageToHead(found->second);
    return std::make_optional(found->second->value);
  }

  /// Inserts given key, value pair to cache. If key already exists in a
  /// cache, then the value is overwritten.
  void Insert(const TKey &key, const TValue &value) {
    auto found = access_map_.find(key);
    if (found != access_map_.end()) {
      // if key already present, update value and move page to head
      found->second->value = value;
      lru_order_.MovePageToHead(found->second);
      return;
    }

    if (access_map_.size() == capacity_) {
      // remove rear page
      auto to_del_key = lru_order_.Rear()->key;
      access_map_.erase(to_del_key);
      lru_order_.RemoveRearPage();
    }

    // add new page to head to List
    auto *page = lru_order_.AddPageToHead(key, value);
    access_map_.emplace(key, page);
  }

  void Clear() {
    access_map_.clear();
    lru_order_.Clear();
  }

 private:
  size_t capacity_;
  impl::LruList<TKey, TValue> lru_order_;
  std::unordered_map<TKey, impl::Node<TKey, TValue> *> access_map_;
};

/// Used for caching objects. Uses least recently used page replacement
/// algorithm for evicting elements when maximum size is reached. This class
/// is NOT thread safe.
///
/// @see ThreadSafeCache
/// @tparam TKey - any object that has hash() defined
/// @tparam TValue - any object
template <typename TKey, typename TValue>
class Cache {
 public:
  using Iterator = typename std::unordered_map<TKey, TValue>::iterator;

  Cache() = default;

  Iterator find(const TKey &key) { return cache_.find(key); }

  std::pair<Iterator, bool> emplace(TKey &&key, TValue &&value) {
    return cache_.emplace(std::forward<TKey>(key), std::forward<TValue>(value));
  }

  void erase(const TKey &key) { cache_.erase(key); }

  Iterator end() { return cache_.end(); }

  bool contains(const TKey &key) { return find(key) != end(); }

  void clear() { cache_.clear(); }

 private:
  std::unordered_map<TKey, TValue> cache_;
};

}  // namespace utils
