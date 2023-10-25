// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/vector.hpp"
namespace memgraph::query {

// Key is hash output, value is vector of unique elements
using CachedType = utils::pmr::unordered_map<size_t, std::vector<TypedValue>>;

struct CachedValue {
  // Cached value, this can be probably templateized
  CachedType cache_;

  explicit CachedValue(utils::MemoryResource *mem) : cache_(mem) {}

  CachedValue(CachedType &&cache, memgraph::utils::MemoryResource *memory) : cache_(std::move(cache), memory) {}

  CachedValue(const CachedValue &other, memgraph::utils::MemoryResource *memory) : cache_(other.cache_, memory) {}

  CachedValue(CachedValue &&other, memgraph::utils::MemoryResource *memory) : cache_(std::move(other.cache_), memory) {}

  CachedValue(CachedValue &&other) noexcept = delete;

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  CachedValue(const CachedValue &) = delete;

  CachedValue &operator=(const CachedValue &) = delete;
  CachedValue &operator=(CachedValue &&) = delete;

  ~CachedValue() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept {
    return cache_.get_allocator().GetMemoryResource();
  }

  bool CacheValue(const TypedValue &maybe_list) {
    if (!maybe_list.IsList()) {
      return false;
    }
    const auto &list = maybe_list.ValueList();
    TypedValue::Hash hash{};
    for (const TypedValue &list_elem : list) {
      const auto list_elem_key = hash(list_elem);
      auto &vector_values = cache_[list_elem_key];
      if (!IsValueInVec(vector_values, list_elem)) {
        vector_values.push_back(list_elem);
      }
    }
    return true;
  }

  bool ContainsValue(const TypedValue &value) const {
    TypedValue::Hash hash{};
    const auto key = hash(value);
    if (cache_.contains(key)) {
      return IsValueInVec(cache_.at(key), value);
    }
    return false;
  }

 private:
  static bool IsValueInVec(const std::vector<TypedValue> &vec_values, const TypedValue &value) {
    return std::any_of(vec_values.begin(), vec_values.end(), [&value](auto &vec_value) {
      const auto is_value_equal = vec_value == value;
      if (is_value_equal.IsNull()) return false;
      return is_value_equal.ValueBool();
    });
  }
};

// Class tracks keys for which user can cache values which help with faster search or faster retrieval
// in the future. Used for IN LIST operator.
class FrameChangeCollector {
 public:
  explicit FrameChangeCollector(utils::MonotonicBufferResource memory_resource)
      : memory_resource_(std::move(memory_resource)), tracked_values_(&memory_resource_){};

  CachedValue &AddTrackingKey(const std::string &key) {
    const auto &[it, _] = tracked_values_.emplace(key, tracked_values_.get_allocator().GetMemoryResource());
    return it->second;
  }

  bool IsKeyTracked(const std::string &key) const { return tracked_values_.contains(key); }

  bool IsKeyValueCached(const std::string &key) const {
    return IsKeyTracked(key) && !tracked_values_.at(key).cache_.empty();
  }

  bool ResetTrackingValue(const std::string &key) {
    if (!tracked_values_.contains(key)) {
      return false;
    }
    tracked_values_.erase(key);
    AddTrackingKey(key);
    return true;
  }

  CachedValue &GetCachedValue(const std::string &key) { return tracked_values_.at(key); }

  bool IsTrackingValues() const { return !tracked_values_.empty(); }

 private:
  utils::MonotonicBufferResource memory_resource_;
  memgraph::utils::pmr::unordered_map<std::string, CachedValue> tracked_values_;
};
}  // namespace memgraph::query
