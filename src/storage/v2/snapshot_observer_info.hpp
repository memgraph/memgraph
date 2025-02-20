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
#include <memory>

#include "utils/counter.hpp"
#include "utils/observer.hpp"

namespace memgraph::storage {
struct SnapshotObserverInfo {
  explicit SnapshotObserverInfo(std::shared_ptr<utils::Observer<void>> observer, uint32_t const item_batch_size)
      : observer_(std::move(observer)), cnt_(item_batch_size) {}

  auto IncrementCounter() -> bool { return cnt_(); }

  void Update() { observer_->Update(); }

 private:
  std::shared_ptr<utils::Observer<void>> observer_{nullptr};
  utils::ResettableRuntimeCounter cnt_;
};
}  // namespace memgraph::storage
