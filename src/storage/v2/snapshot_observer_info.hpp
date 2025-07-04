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

#include <array>
#include <cstdint>
#include <memory>

#include "utils/counter.hpp"
#include "utils/observer.hpp"

namespace memgraph::storage {

enum class UpdateType : uint8_t { VERTICES, EDGES, TEXT_IDX, POINT_IDX, VECTOR_IDX, VECTOR_EDGE_IDX };

struct SnapshotObserverInfo {
  explicit SnapshotObserverInfo(std::shared_ptr<utils::Observer<void>> observer,
                                uint32_t const items_per_progress_batch)
      : observer_(std::move(observer)), cnt_(items_per_progress_batch) {}

  void Update(UpdateType const update_type) const {
    if (auto const update_factor = update_factors[static_cast<uint8_t>(update_type)].second; cnt_(update_factor)) {
      observer_->Update();
    }
  }

 private:
  static constexpr std::array<std::pair<UpdateType, uint16_t>, 6> update_factors{
      std::pair{UpdateType::VERTICES, 1},      std::pair{UpdateType::EDGES, 1},
      std::pair{UpdateType::TEXT_IDX, 10},     std::pair{UpdateType::POINT_IDX, 10},
      std::pair{UpdateType::VECTOR_IDX, 1000}, std::pair{UpdateType::VECTOR_EDGE_IDX, 1000}};
  std::shared_ptr<utils::Observer<void>> observer_{nullptr};
  mutable utils::ResettableAtomicCounter cnt_;
};
}  // namespace memgraph::storage
