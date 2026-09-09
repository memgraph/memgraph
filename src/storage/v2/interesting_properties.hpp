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
#include <span>

#include "storage/v2/id_types.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

/// Which properties a write has to be reported against, so that whatever validates later can
/// find the object again.
///
/// Answering "not interesting" suppresses the report, which does not make a later check cheaper
/// but removes it. A holder that cannot enumerate the properties it is keyed on therefore has to
/// answer `Everything()`; handing over a set that is merely incomplete loses writes silently.
/// The default is `Everything()` for the same reason: a value nobody filled in reports
/// everything rather than nothing.
class InterestingProperties {
 public:
  InterestingProperties() = default;

  static InterestingProperties Everything() { return {}; }

  /// `narrow` is borrowed, must be sorted, and must outlive every use of the result. It is held
  /// by whatever owns the snapshot of constraints the properties were read from, which a
  /// transaction keeps for its whole lifetime.
  static InterestingProperties Only(std::span<PropertyId const> narrow) {
    // An unsorted set makes the search below answer false for a property that is in it, which is
    // the direction that loses writes.
    DMG_ASSERT(std::ranges::is_sorted(narrow), "InterestingProperties::Only needs a sorted set");
    return InterestingProperties{narrow};
  }

  bool IsInteresting(PropertyId property) const { return all_ || std::ranges::binary_search(narrow_, property); }

 private:
  explicit InterestingProperties(std::span<PropertyId const> narrow) : narrow_{narrow}, all_{false} {}

  std::span<PropertyId const> narrow_{};
  bool all_{true};
};

}  // namespace memgraph::storage
