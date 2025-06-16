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

#include <numeric>
#include <ranges>
#include <span>
#include <storage/v2/id_types.hpp>
#include <utils/string.hpp>
#include <vector>

namespace memgraph::storage {

/**
 * `PropertyPath` identifies one or more properties for indexing, which may
 * either be a single `PropertyId` (for the case of `CREATE INDEX ON L1(a)`), or
 * an ordered hierarchy of nested `PropertyId`s (for the case of
 * `CREATE INDEX ON :L1(a.b.c.d)`). This light-weight `vector<PropertyId>`
 * wrapper is used mainly to cleanly distinguish the inner and outer vector when
 * we have composite nested indices. As such, the constructors are deliberately
 * not explicit so we can implicitly wrap one or more `PropertyId`s.
 */
struct PropertyPath {
  PropertyPath() = default;
  PropertyPath(std::vector<PropertyId> properties) : properties_{std::move(properties)} {}
  PropertyPath(std::initializer_list<PropertyId> properties) : properties_{properties} {}
  PropertyPath(PropertyId property) : properties_{{property}} {}

  using const_iterator = std::vector<PropertyId>::const_iterator;

  auto operator[](std::size_t pos) const -> PropertyId const & { return properties_[pos]; }
  auto size() const noexcept -> std::size_t { return properties_.size(); }
  bool empty() const { return properties_.empty(); };
  auto begin() const -> const_iterator { return properties_.begin(); }
  auto end() const -> const_iterator { return properties_.end(); }
  auto cbegin() const -> const_iterator { return properties_.cbegin(); }
  auto cend() const -> const_iterator { return properties_.cend(); }
  void insert(PropertyId property_id) { properties_.push_back(property_id); }
  void reserve(std::size_t size) { properties_.reserve(size); }
  auto as_span() const -> std::span<PropertyId const> { return std::span{properties_}; }

  friend bool operator==(PropertyPath const &lhs, PropertyPath const &rhs) = default;
  friend auto operator<=>(PropertyPath const &lhs, PropertyPath const &rhs) = default;

  PropertyId front() const { return properties_.front(); }
  PropertyId back() const { return properties_.back(); }

 private:
  std::vector<PropertyId> properties_;
};

/** Converts a PropertyPath to std::string using the given `context` object,
 * which must provide `PropertyToName(PropertyId)`
 */
template <typename Context>
requires requires(Context *context, PropertyId prop) {
  { context->PropertyToName(prop) } -> std::convertible_to<std::string>;
}
std::string ToString(PropertyPath const &path, Context *context) {
  return utils::Join(
      path | std::ranges::views::transform([&](PropertyId prop) { return context->PropertyToName(prop); }), ".");
}

}  // namespace memgraph::storage

namespace std {

template <>
struct hash<memgraph::storage::PropertyPath> {
  std::size_t operator()(memgraph::storage::PropertyPath const &path) const noexcept {
    return std::accumulate(path.cbegin(), path.cend(), size_t{},
                           [](size_t seed, memgraph::storage::PropertyId property_id) {
                             boost::hash_combine(seed, property_id.AsUint());
                             return seed;
                           });
  }
};

}  // namespace std
