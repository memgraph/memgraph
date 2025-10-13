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
#include "query/frontend/ast/query/named_expression.hpp"
#include "query/typed_value.hpp"
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
  explicit FrameChangeCollector(allocator_type alloc = {}) : caches_{alloc}, invalidators_{alloc} {}

  FrameChangeCollector(const FrameChangeCollector &) = delete;
  FrameChangeCollector(FrameChangeCollector &&) = delete;
  FrameChangeCollector &operator=(const FrameChangeCollector &) = delete;
  FrameChangeCollector &operator=(FrameChangeCollector &&) noexcept = delete;

  auto get_allocator() const -> allocator_type { return caches_.get_allocator(); }

  auto AddInListKey(utils::FrameChangeId const &key) -> CachedValue & {
    const auto &[it, _] =
        caches_.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
    return it->second;
  }

  bool IsKeyTracked(utils::FrameChangeId const &key) const { return caches_.contains(key); }

  auto TryGetCachedValue(utils::FrameChangeId const &key) const
      -> std::optional<std::reference_wrapper<CachedValue const>> {
    auto const it = caches_.find(key);
    if (it == caches_.cend()) {
      return std::nullopt;
    }
    // Empty is considered unpopulated
    if (it->second.cache_.empty()) {
      return std::nullopt;
    }
    return std::optional{std::cref(it->second)};
  }

  void ResetInListCache(Symbol const &symbol) { ResetInListCacheInternal(symbol.position_); }

  void ResetInListCache(NamedExpression const &named_expression) {
    ResetInListCacheInternal(named_expression.symbol_pos_);
  }

  auto GetCachedValue(utils::FrameChangeId const &key) -> CachedValue & {
    auto const it = caches_.find(key);
    DMG_ASSERT(it != caches_.cend());
    return it->second;
  }

  void AddInListInvalidator(utils::FrameChangeId const &key, Symbol::Position_t symbol_pos) {
    invalidators_[symbol_pos].push_back(key);
  }

  bool AnyInListCaches() const { return !caches_.empty(); }

 private:
  void ResetInListCacheInternal(Symbol::Position_t const &symbol_pos) {
    auto const it = invalidators_.find(symbol_pos);
    if (it == invalidators_.cend()) [[likely]]
      return;
    for (auto const &key : it->second) {
      if (auto const it2 = caches_.find(key); it2 != caches_.cend()) {
        it2->second.Reset();
      }
    }
  }

  utils::pmr::unordered_map<utils::FrameChangeId, CachedValue> caches_;
  utils::pmr::unordered_map<Symbol::Position_t, utils::pmr::vector<utils::FrameChangeId>> invalidators_;
};
}  // namespace memgraph::query
