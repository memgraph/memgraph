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

#include <unordered_map>
#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

using CachedType = std::unordered_map<size_t, std::vector<TypedValue>>;

struct CachedValue {
  // Cached value, this can be probably templateized
  CachedType cache_;

  // Func to check if cache_ contains value
  bool CacheValue(const TypedValue &value) {
    if (!value.IsList()) {
      return false;
    }
    const auto &list = value.ValueList();
    TypedValue::Hash hash{};
    for (const TypedValue &element : list) {
      auto key = hash(element);
      if (!cache_.contains(key)) {
        cache_.emplace(key, std::vector<TypedValue>{element});
        continue;
      }
      auto &vector_values = cache_[key];
      const auto contains_element =
          std::any_of(vector_values.begin(), vector_values.end(), [&element](auto &vec_value) {
            auto result = vec_value == element;
            if (result.IsNull()) return false;
            return result.ValueBool();
          });
      if (!contains_element) [[unlikely]] {
        vector_values.push_back(element);
      }
    }
    return true;
  }
  // Func to cache_value inside cache_
  bool ContainsValue(const TypedValue &value) {
    TypedValue::Hash hash{};
    auto key = hash(value);
    if (cache_.contains(key)) {
      const auto &vec_values = cache_.at(key);
      auto result = std::any_of(vec_values.begin(), vec_values.end(), [&value](auto &vec_value) {
        auto result = vec_value == value;
        if (result.IsNull()) return false;
        return result.ValueBool();
      });
      return result;
    }
    return false;
  }
};

// Class tracks keys for which user can cache values which help with faster search or faster retrieval
// in the future.
class FrameChangeCollector {
 public:
  explicit FrameChangeCollector(utils::MemoryResource *mem) : tracked_values_(mem){};

  // Add tracking key to cache later value
  CachedValue &AddTrackingKey(const std::string &key) {
    tracked_values_.emplace(key, CachedValue{});
    return tracked_values_[key];
  }

  // Is key tracked
  bool IsKeyTracked(const std::string &key) const { return tracked_values_.contains(key); }

  // Is value for given key cached
  bool IsKeyValueCached(const std::string &key) const {
    return tracked_values_.contains(key) && !tracked_values_.at(key).cache_.empty();
  }

  // Reset value for tracking key
  bool ResetTrackingValue(const std::string &key) {
    if (tracked_values_.contains(key)) {
      tracked_values_[key].cache_.clear();
    }

    return true;
  }

  // Get value cached for tracking key
  CachedValue &GetCachedValue(const std::string &key) { return tracked_values_[key]; }

  // Checks for keys tracked
  bool IsTrackingValues() const { return !tracked_values_.empty(); }

 private:
  memgraph::utils::pmr::unordered_map<std::string, CachedValue> tracked_values_;
};
}  // namespace memgraph::query
