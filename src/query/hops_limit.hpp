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

  bool IsUsed() const { return limit.has_value(); }

  int64_t GetLimit() const { return *limit; }
  int64_t GetHopsCounter() const { return hops_counter; }

  bool IsLimitReached() const { return limit_reached; }

  int64_t LeftHops() const { return *limit - hops_counter; }

  void IncrementHopsCount(int64_t increment) {
    if (limit) {
      hops_counter += increment;
      limit_reached = hops_counter > *limit;
    }
  }
};

}  // namespace memgraph::query
