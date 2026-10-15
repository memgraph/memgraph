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

namespace memgraph::storage {
struct alignas(16) CommitTsInfo {
  uint64_t ldt_{0};  // last durable ts
  uint64_t num_committed_txns_{0};

  // Advance-only merge: floor each field independently to the larger observed value. Used to fold a freshly
  // observed value into the cached one without regressing either field when they move independently.
  friend CommitTsInfo Max(CommitTsInfo const &a, CommitTsInfo const &b) {
    return {.ldt_ = std::max(a.ldt_, b.ldt_),
            .num_committed_txns_ = std::max(a.num_committed_txns_, b.num_committed_txns_)};
  }
};
}  // namespace memgraph::storage
