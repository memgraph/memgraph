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

#include <algorithm>
#include <cstdint>
#include <optional>

#include "utils/shared_quota.hpp"

namespace memgraph::query {

struct HopsLimit {
  std::optional<uint64_t> limit{std::nullopt};
  uint64_t batch{0};
  std::optional<utils::SharedQuota> shared_quota_{
      std::nullopt};  // Supports both fast single-threaded and multi-threaded execution
  bool is_limit_reached{false};

  HopsLimit() = default;

  ~HopsLimit() { Free(); }

  explicit HopsLimit(uint64_t limit, uint64_t batch = 1U)
      : limit(limit), batch(batch), shared_quota_{std::in_place, limit, std::max(batch, 1UL)} {}

  bool IsUsed() const { return limit.has_value(); }

  uint64_t GetLimit() const { return limit.value(); }

  bool IsLimitReached() const { return is_limit_reached; }

  // Used for multi-threaded execution where each thread needs to free its left quota.
  void Free() {
    if (shared_quota_) shared_quota_->Free();
  }

  // Return the number of available hops (or consumed amount which behaves similarly for check > 0)
  uint64_t IncrementHopsCount(uint64_t increment = 1) {
    if (IsUsed()) {
      if (is_limit_reached) return 0;

      if (!shared_quota_) {
        shared_quota_.emplace(limit.value(), std::max(batch, 1UL));
      }

      auto consumed = shared_quota_->Decrement(increment);
      if (consumed < increment) {
        is_limit_reached = true;
        shared_quota_.reset();  // free any left over quota
      }
      return consumed;
    }
    return increment;
  }
};

}  // namespace memgraph::query
