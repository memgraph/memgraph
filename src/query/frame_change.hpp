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
#include <tuple>
#include <utility>
#include "query/typed_value.hpp"
#include "utils/fnv.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"
#include "utils/pmr/vector.hpp"
namespace memgraph::query {

// Key is hash output, value is vector of unique elements
using CachedType = utils::pmr::unordered_map<size_t, utils::pmr::vector<TypedValue>>;

struct CachedValue {
  using allocator_type = utils::Allocator<CachedValue>;
  using alloc_traits = std::allocator_traits<allocator_type>;

  // Cached value, this can be probably templateized
  CachedType cache_;
  // TODO: swap with absl::flat_hash_set

  explicit CachedValue(allocator_type alloc) : cache_{alloc} {};
  CachedValue(const CachedValue &other, allocator_type alloc) : cache_(other.cache_, alloc) {}
  CachedValue(CachedValue &&other, allocator_type alloc) : cache_(std::move(other.cache_), alloc){};

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
    TypedValue::Hash hash{};
    for (const auto &element : list) {
      const auto key = hash(element);
      auto &vector_values = cache_[key];
      if (!IsValueInVec(vector_values, element)) {
        vector_values.emplace_back(element);
      }
    }
    return true;
  }

  // Func to check if cache_ contains value
  bool ContainsValue(const TypedValue &value) const {
    const auto key = TypedValue::Hash{}(value);
    if (cache_.contains(key)) {
      return IsValueInVec(cache_.at(key), value);
    }
    return false;
  }

 private:
  static bool IsValueInVec(const utils::pmr::vector<TypedValue> &vec_values, const TypedValue &value) {
    return std::ranges::any_of(vec_values, [&value](auto &vec_value) {
      const auto is_value_equal = vec_value == value;
      if (is_value_equal.IsNull()) return false;
      return is_value_equal.ValueBool();
    });
  }
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

  CachedValue &AddTrackingKey(const std::string &key) {
    const auto &[it, _] = tracked_values_.emplace(
        std::piecewise_construct, std::forward_as_tuple(utils::pmr::string(key, utils::NewDeleteResource())),
        std::forward_as_tuple());
    return it->second;
  }

  bool IsKeyTracked(std::string_view const key) const { return tracked_values_.contains(key); }

  auto TryGetCachedValue(std::string_view const key) const -> std::optional<std::reference_wrapper<CachedValue const>> {
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

  bool ResetTrackingValue(std::string_view const key) {
    auto const it = tracked_values_.find(key);
    if (it == tracked_values_.cend()) {
      return false;
    }
    it->second.Reset();
    return true;
  }

  CachedValue &GetCachedValue(std::string_view const key) {
    auto it = tracked_values_.find(key);
    DMG_ASSERT(it != tracked_values_.cend());
    return it->second;
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

  utils::pmr::unordered_map<utils::pmr::string, CachedValue, PmrStringHash, PmrStringEqual> tracked_values_;
};
}  // namespace memgraph::query
