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

struct CachedSet {
  using allocator_type = utils::Allocator<CachedSet>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  // Cached value, this can be probably templateized
  absl::flat_hash_set<TypedValue, absl::DefaultHashContainerHash<TypedValue>, TypedValue::BoolEqual, allocator_type>
      cache_;

  explicit CachedSet(allocator_type alloc) : cache_{alloc} {}
  CachedSet(const CachedSet &other, allocator_type alloc) : cache_(other.cache_, alloc) {}
  CachedSet(CachedSet &&other, allocator_type alloc) : cache_(std::move(other.cache_), alloc) {}

  CachedSet(CachedSet &&other) noexcept : CachedSet(std::move(other), other.get_allocator()) {}

  CachedSet(const CachedSet &other)
      : CachedSet(other, alloc_traits::select_on_container_copy_construction(other.get_allocator())) {}

  auto get_allocator() const -> allocator_type { return cache_.get_allocator(); }

  CachedSet &operator=(const CachedSet &) = delete;
  CachedSet &operator=(CachedSet &&) = delete;

  ~CachedSet() = default;

  void Reset() { cache_.clear(); }

  bool SetValue(const TypedValue &maybe_list) {
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
  bool Contains(const TypedValue &value) const { return cache_.contains(value); }
};

// Class tracks keys for which user can cache values which help with faster search or faster retrieval
// in the future. Used for IN LIST operator.
class FrameChangeCollector {
  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<FrameChangeCollector>;
  using alloc_traits = std::allocator_traits<allocator_type>;

 public:
  explicit FrameChangeCollector(allocator_type alloc = {})
      : inlist_cache_{alloc}, regex_cache_{alloc}, invalidators_{alloc} {}

  FrameChangeCollector(const FrameChangeCollector &) = delete;
  FrameChangeCollector(FrameChangeCollector &&) = delete;
  FrameChangeCollector &operator=(const FrameChangeCollector &) = delete;
  FrameChangeCollector &operator=(FrameChangeCollector &&) noexcept = delete;
  ~FrameChangeCollector() = default;

  auto get_allocator() const -> allocator_type { return inlist_cache_.get_allocator(); }

  auto AddInListKey(utils::FrameChangeId const &key) -> CachedSet & {
    const auto &[it, _] =
        inlist_cache_.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
    return it->second;
  }

  // This will only happen when walking through the AST to see what is cacheable
  auto AddRegexKey(utils::FrameChangeId const &key) -> std::optional<std::reference_wrapper<std::regex const>> {
    regex_cache_.try_emplace(key, std::nullopt);
    return std::nullopt;
  }

  auto AddRegexKey(utils::FrameChangeId const &key, std::regex regex)
      -> std::optional<std::reference_wrapper<std::regex const>> {
    auto it = regex_cache_.find(key);
    DMG_ASSERT(it != regex_cache_.cend(), "Regex key should be tracked");
    it->second = std::move(regex);
    return std::optional{std::cref(*it->second)};
  }

  bool IsInlistKeyTracked(utils::FrameChangeId const &key) const { return inlist_cache_.contains(key); }

  bool IsRegexKeyTracked(utils::FrameChangeId const &key) const { return regex_cache_.contains(key); }

  auto TryGetInlistCachedValue(utils::FrameChangeId const &key) const
      -> std::optional<std::reference_wrapper<CachedSet const>> {
    auto const it = inlist_cache_.find(key);
    if (it == inlist_cache_.cend()) {
      return std::nullopt;
    }
    // Empty is considered unpopulated
    if (it->second.cache_.empty()) {
      return std::nullopt;
    }
    return std::optional{std::cref(it->second)};
  }

  auto TryGetRegexCachedValue(utils::FrameChangeId const &key) const
      -> std::optional<std::reference_wrapper<std::regex const>> {
    auto const it = regex_cache_.find(key);
    if (it == regex_cache_.cend()) {
      return std::nullopt;
    }
    // nullopt if tracked but not populated
    if (!it->second.has_value()) {
      return std::nullopt;
    }
    return std::optional{std::cref(*it->second)};
  }

  void ResetCache(Symbol const &symbol) {
    ResetInListCacheInternal(symbol.position_);
    ResetRegexCacheInternal(symbol.position_);
  }

  void ResetCache(NamedExpression const &named_expression) {
    ResetInListCacheInternal(named_expression.symbol_pos_);
    ResetRegexCacheInternal(named_expression.symbol_pos_);
  }

  void ResetInListCache(Symbol const &symbol) { ResetInListCacheInternal(symbol.position_); }

  void ResetInListCache(NamedExpression const &named_expression) {
    ResetInListCacheInternal(named_expression.symbol_pos_);
  }

  auto GetInlistCachedValue(utils::FrameChangeId const &key) -> CachedSet & {
    auto const it = inlist_cache_.find(key);
    DMG_ASSERT(it != inlist_cache_.cend());
    return it->second;
  }

  auto GetRegexCachedValue(utils::FrameChangeId const &key) -> std::optional<std::reference_wrapper<std::regex const>> {
    auto const it = regex_cache_.find(key);
    if (it == regex_cache_.cend() || !it->second) {
      return std::nullopt;
    }
    return std::optional{std::cref(*it->second)};
  }

  void AddInvalidator(utils::FrameChangeId const &key, Symbol::Position_t symbol_pos) {
    invalidators_[symbol_pos].emplace_back(key);
  }

  bool AnyCaches() const { return !inlist_cache_.empty() || !regex_cache_.empty(); }

  bool AnyInListCaches() const { return !inlist_cache_.empty(); }

  bool AnyRegexCaches() const { return !regex_cache_.empty(); }

 private:
  void ResetInListCacheInternal(Symbol::Position_t const &symbol_pos) {
    auto const it = invalidators_.find(symbol_pos);
    if (it == invalidators_.cend()) [[likely]]
      return;
    for (auto const &key : it->second) {
      if (auto const it2 = inlist_cache_.find(key); it2 != inlist_cache_.cend()) {
        it2->second.Reset();
      }
    }
  }

  void ResetRegexCacheInternal(Symbol::Position_t const &symbol_pos) {
    auto const it = invalidators_.find(symbol_pos);
    if (it == invalidators_.cend()) [[likely]]
      return;
    for (auto const &key : it->second) {
      if (auto it2 = regex_cache_.find(key); it2 != regex_cache_.cend()) {
        it2->second = std::nullopt;  // tracked but not populated
      }
    }
  }

  utils::pmr::unordered_map<utils::FrameChangeId, CachedSet> inlist_cache_;
  utils::pmr::unordered_map<utils::FrameChangeId, std::optional<std::regex>> regex_cache_;
  utils::pmr::unordered_map<Symbol::Position_t, utils::pmr::vector<utils::FrameChangeId>> invalidators_;
};
}  // namespace memgraph::query
