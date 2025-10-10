// Copyright 2025 Memgraph Ltd.
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

#include <tuple>
#include <utility>
#include "query/typed_value.hpp"
#include "utils/fnv.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"

#include "absl/container/flat_hash_set.h"
#include "utils/frame_change_id.hpp"

namespace memgraph::query {

struct CachedValue {
  using allocator_type = utils::Allocator<CachedValue>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  // Cached value, this can be probably templateized
  absl::flat_hash_set<TypedValue, absl::DefaultHashContainerHash<TypedValue>, TypedValue::BoolEqual, allocator_type>
      cache_;

  explicit CachedValue(allocator_type alloc) : cache_{alloc} {}
  CachedValue(const CachedValue &other, allocator_type alloc) : cache_(other.cache_, alloc) {}
  CachedValue(CachedValue &&other, allocator_type alloc) : cache_(std::move(other.cache_), alloc) {}

  CachedValue(CachedValue &&other) noexcept : CachedValue(std::move(other), other.get_allocator()) {}

  CachedValue(const CachedValue &other)
      : CachedValue(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())) {}

  auto get_allocator() const -> allocator_type { return cache_.get_allocator(); }

  CachedValue &operator=(const CachedValue &) = delete;
  CachedValue &operator=(CachedValue &&) = delete;

  ~CachedValue() = default;

  void Reset() { cache_.clear(); }

  bool CacheValue(const TypedValue &maybe_list) {
    if (!maybe_list.IsList()) {
      return false;
    }
    const auto &list = maybe_list.ValueList();
    for (const auto &element : list) {
      cache_.insert(element);
      ;
    }
    return true;
  }

  // Func to check if cache_ contains value
  bool ContainsValue(const TypedValue &value) const { return cache_.contains(value); }
};

// Class tracks keys for which user can cache values which help with faster search or faster retrieval
// in the future. Used for IN LIST operator.
class FrameChangeCollector {
  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<FrameChangeCollector>;
  using alloc_traits = std::allocator_traits<allocator_type>;

 public:
  explicit FrameChangeCollector(allocator_type alloc = {}) : tracked_values_{alloc} {}

  FrameChangeCollector(FrameChangeCollector &&other, allocator_type alloc)
      : tracked_values_(std::move(other.tracked_values_), alloc) {}
  FrameChangeCollector(const FrameChangeCollector &other, allocator_type alloc)
      : tracked_values_(other.tracked_values_, alloc) {}

  FrameChangeCollector(const FrameChangeCollector &other)
      : FrameChangeCollector(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())){};

  FrameChangeCollector(FrameChangeCollector &&other) noexcept
      : FrameChangeCollector(std::move(other), other.get_allocator()) {}

  /** Copy assign other, utils::MemoryResource of `this` is used */
  FrameChangeCollector &operator=(const FrameChangeCollector &) = default;

  /** Move assign other, utils::MemoryResource of `this` is used. */
  FrameChangeCollector &operator=(FrameChangeCollector &&) noexcept = default;

  auto get_allocator() const -> allocator_type { return tracked_values_.get_allocator(); }

  CachedValue &AddTrackingKey(utils::FrameChangeId const &key) {
    const auto &[it, _] =
        tracked_values_.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
    return it->second;
  }

  bool IsKeyTracked(utils::FrameChangeId const &key) const { return tracked_values_.contains(key); }

  auto TryGetCachedValue(utils::FrameChangeId const &key) const
      -> std::optional<std::reference_wrapper<CachedValue const>> {
    auto const it = tracked_values_.find(key);
    if (it == tracked_values_.cend()) {
      return std::nullopt;
    }
    // Empty is considered unpopulated
    if (it->second.cache_.empty()) {
      return std::nullopt;
    }
    return std::optional{std::cref(it->second)};
  }

  bool ResetTrackingValue(utils::FrameChangeId const &key) {
    auto const it = tracked_values_.find(key);
    if (it == tracked_values_.cend()) {
      return false;
    }
    it->second.Reset();
    return true;
  }

  CachedValue &GetCachedValue(utils::FrameChangeId const &key) {
    auto it = tracked_values_.find(key);
    DMG_ASSERT(it != tracked_values_.cend());
    return it->second;
  }

  void AddSymbolDependency(utils::FrameChangeId const &symbol_id, utils::FrameChangeId const &dependent_key) {
    symbol_dependencies_[symbol_id].push_back(dependent_key);
  }

  void ResetTrackingValueForSymbol(utils::FrameChangeId const &symbol_id) {
    ResetTrackingValue(symbol_id);

    auto const it = symbol_dependencies_.find(symbol_id);
    if (it != symbol_dependencies_.cend()) {
      for (auto const &dependent_key : it->second) {
        ResetTrackingValue(dependent_key);
      }
    }
  }

  bool IsTrackingValues() const { return !tracked_values_.empty(); }

  ~FrameChangeCollector() = default;

 private:
  // Transparent hasher
  struct PmrStringHash {
    using is_transparent = void;  // enables heterogeneous lookup

    std::size_t operator()(std::string_view const sv) const noexcept { return utils::Fnv(sv); }
  };

  // Transparent equality comparator
  struct PmrStringEqual {
    using is_transparent = void;

    bool operator()(std::string_view const lhs, std::string_view const rhs) const noexcept { return lhs == rhs; }
  };

  utils::pmr::unordered_map<utils::FrameChangeId, CachedValue> tracked_values_;
  // could be just list literal -> list literals that depend on it
  utils::pmr::unordered_map<utils::FrameChangeId, utils::pmr::vector<utils::FrameChangeId>> symbol_dependencies_;
};
}  // namespace memgraph::query
