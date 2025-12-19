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

#include <cstdint>
#include <optional>

namespace memgraph::query {

struct HopsLimit {
  std::optional<int64_t> limit;
  int64_t hops_counter{0};
  bool limit_reached{false};

  constexpr bool IsUsed() const { return limit.has_value(); }

  constexpr int64_t GetLimit() const { return *limit; }
  constexpr int64_t GetHopsCounter() const { return hops_counter; }

  constexpr bool IsLimitReached() const { return limit_reached; }

  constexpr int64_t LeftHops() const { return *limit - hops_counter; }

  constexpr void IncrementHopsCount(int64_t increment) {
    if (limit) {
      hops_counter += increment;
      limit_reached = hops_counter > *limit;
    }
  }
};

}  // namespace memgraph::query
