// Copyright 2022 Memgraph Ltd.
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

#include <concepts>
#include <type_traits>

#include "storage/v3/vertex.hpp"
#include "utils/concepts.hpp"

namespace memgraph::storage::v3 {

struct LexicographicallyOrderedVertex {
  Vertex vertex;

  friend bool operator==(const LexicographicallyOrderedVertex &lhs, const LexicographicallyOrderedVertex &rhs) {
    return lhs.vertex.keys == rhs.vertex.keys;
  }

  friend bool operator<(const LexicographicallyOrderedVertex &lhs, const LexicographicallyOrderedVertex &rhs) {
    return lhs.vertex.keys < rhs.vertex.keys;
  }

  // TODO(antaljanosbenjamin): maybe it worth to overload this for std::array to avoid heap construction of the vector
  friend bool operator==(const LexicographicallyOrderedVertex &lhs, const std::vector<PropertyValue> &rhs) {
    return lhs.vertex.keys == rhs;
  }

  friend bool operator<(const LexicographicallyOrderedVertex &lhs, const std::vector<PropertyValue> &rhs) {
    return lhs.vertex.keys < rhs;
  }
};
}  // namespace memgraph::storage::v3
