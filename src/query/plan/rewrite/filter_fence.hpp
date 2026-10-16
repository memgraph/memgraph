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

#include <optional>
#include <utility>
#include <vector>

#include "query/plan/preprocess.hpp"

namespace memgraph::query::plan {

/// A filter above a write or a command barrier reads the graph after that write. A scan or join below it reads the
/// graph before. So a rewriter must not move such a filter below the fence: it hides the collected filters while it
/// walks the fenced operator's input and restores them afterwards.
class FilterFence {
 public:
  void Enter(Filters &filters, bool active = true) {
    if (active) {
      stashed_.emplace_back(std::exchange(filters, Filters{}));
    } else {
      stashed_.emplace_back(std::nullopt);
    }
  }

  void Exit(Filters &filters) {
    if (stashed_.back()) filters = std::move(*stashed_.back());
    stashed_.pop_back();
  }

 private:
  std::vector<std::optional<Filters>> stashed_;
};

}  // namespace memgraph::query::plan
