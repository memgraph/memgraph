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

#include "utils/shared_quota.hpp"

namespace memgraph::query {

struct HopsLimit {
  std::optional<int64_t> limit{std::nullopt};
  int64_t batch{0};
  std::optional<utils::SharedQuota> shared_quota_{std::nullopt};
  bool is_limit_reached{false};

  HopsLimit() = default;
  HopsLimit(int64_t limit, int64_t batch)
      : limit(limit), batch(batch), shared_quota_{std::in_place, limit, batch > 0 ? batch : 1} {}

  bool IsUsed() const { return limit.has_value(); }

  int64_t GetLimit() const { return limit.value(); }
  // Deprecated/Removed as SharedQuota doesn't track global counter in the same way
  // int64_t GetHopsCounter() const { ... }

  bool IsLimitReached() const { return is_limit_reached; }

  // Used for multi-threaded execution where each thread needs to free its left quota.
  void Free() {
    if (shared_quota_) shared_quota_->Free();
  }

  // Return the number of available hops (or consumed amount which behaves similarly for check > 0)
  int64_t IncrementHopsCount(int64_t increment = 1) {
    if (IsUsed()) {
      if (is_limit_reached) return 0;

      if (!shared_quota_) {
        shared_quota_.emplace(limit.value(), batch > 0 ? batch : 1);
      }

      auto consumed = shared_quota_->Decrement(increment);
      if (consumed < increment) {
        is_limit_reached = true;
      }
      return consumed;
    }
    return increment;
  }
};

}  // namespace memgraph::query
