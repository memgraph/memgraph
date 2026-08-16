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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/v2/property_value.hpp"

namespace memgraph::storage::test {

/// Keeps what a materialising read hands over, so it can be compared against reading the same
/// properties as values. Every value arrives at the index of the property that produced it.
struct CollectingMaterialiser {
  std::vector<PropertyValue> values;

  explicit CollectingMaterialiser(std::size_t count) : values(count) {}

  void EmitNull(std::size_t index) { values[index] = PropertyValue{}; }

  void Emit(std::size_t index, std::string_view value) { values[index] = PropertyValue{std::string{value}}; }

  void Emit(std::size_t index, PropertyValue &&value) { values[index] = std::move(value); }

  template <typename T>
  void Emit(std::size_t index, T value) {
    values[index] = PropertyValue{value};
  }
};

}  // namespace memgraph::storage::test
