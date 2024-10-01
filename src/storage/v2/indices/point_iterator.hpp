// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {
struct PointIterator {
  using value_type = VertexAccessor;
  friend bool operator==(PointIterator const &, PointIterator const &) = default;
  auto operator=(PointIterator const &) -> PointIterator & = default;
  auto operator++() -> PointIterator & {
    // TODO
    return *this;
  }

  auto operator*() -> value_type & {
    // TODO
    static auto x = VertexAccessor(nullptr, nullptr, nullptr);
    return x;
  }

  auto operator*() const -> value_type const & {
    // TODO
    static auto const x = VertexAccessor(nullptr, nullptr, nullptr);
    return x;
  }
};
}  // namespace memgraph::storage
