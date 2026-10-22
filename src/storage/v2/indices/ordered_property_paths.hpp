// Copyright 2026 Memgraph Ltd.
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

#include <cstddef>
#include <span>
#include <vector>

#include "storage/v2/indices/property_path.hpp"

namespace memgraph::storage {

/**
 * A set of `PropertyPath` held in the order a `PropertyStore` must be asked for them, together with
 * the permutation back to the order the caller gave.
 *
 * A store reads several properties in one forward pass over the record, finding each from where the
 * previous one stopped, so it can only find a path it has not already passed. Asking out of order
 * reads Null for a property that is set, a wrong answer rather than a refusal. Holding the order in
 * the type means a caller cannot express the bad one.
 */
class OrderedPropertyPaths {
 public:
  OrderedPropertyPaths() = default;

  /// Sorts in place, so constructing one costs no allocation beyond the permutation itself.
  explicit OrderedPropertyPaths(std::vector<PropertyPath> paths);

  explicit OrderedPropertyPaths(std::span<PropertyPath const> paths)
      : OrderedPropertyPaths{std::vector<PropertyPath>{paths.begin(), paths.end()}} {}

  /// Each id as a path of its own, for callers naming properties rather than paths into them.
  static auto FromPropertyIds(std::span<PropertyId const> properties) -> OrderedPropertyPaths;

  /// The paths in the order a `PropertyStore` must be asked for them.
  auto paths() const -> std::span<PropertyPath const> { return sorted_; }

  /// `paths()[i]` is the path the caller supplied at `position_lookup()[i]`.
  auto position_lookup() const -> std::span<std::size_t const> { return position_lookup_; }

  auto size() const -> std::size_t { return sorted_.size(); }

  auto empty() const -> bool { return sorted_.empty(); }

 private:
  std::vector<PropertyPath> sorted_;
  std::vector<std::size_t> position_lookup_;
};

}  // namespace memgraph::storage
