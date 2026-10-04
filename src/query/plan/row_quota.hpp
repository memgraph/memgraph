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
#include <cstdint>
#include <optional>
#include <utility>

#include "utils/shared_quota.hpp"

namespace memgraph::query::plan {

/// The row count of a SKIP or LIMIT, armed once per execution. A serial cursor counts alone. The branches of a
/// parallel operator draw from one coordinator, which that operator re-arms between executions.
class RowQuota {
 public:
  RowQuota() = default;

  RowQuota(utils::SharedQuota branch_share, size_t num_workers)
      : branch_share_(std::move(branch_share)), num_batches_(utils::SharedQuota::WorkersToBatch(num_workers)) {}

  void Arm(uint64_t count) {
    if (branch_share_) {
      quota_.emplace(*branch_share_);
      quota_->Initialize(count, num_batches_);
    } else {
      quota_.emplace(count);
    }
  }

  bool IsArmed() const { return quota_.has_value(); }

  uint64_t Decrement() { return quota_->Decrement(); }

  void Increment() { quota_->Increment(); }

  // Returns the unused count, so that the other branches can draw it.
  void Release() { quota_.reset(); }

 private:
  std::optional<utils::SharedQuota> branch_share_;  // Never armed itself; each execution arms a copy
  uint64_t num_batches_{1};
  std::optional<utils::SharedQuota> quota_;
};

}  // namespace memgraph::query::plan
