// Copyright 2023 Memgraph Ltd.
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

#include <atomic>
#include <cstdlib>
#include <memory>

namespace memgraph::metrics {

enum class OneShotEvents {
  kFirstSuccessfulQueryTs,
  kFirstFailedQueryTs,
  kNum /* leave at the end */
};

class EventOneShot {
 public:
  explicit EventOneShot(std::atomic<double> *allocated_values) noexcept : values_(allocated_values) {}

  auto &operator[](const OneShotEvents event) { return values_[(int)event]; }

  const auto &operator[](const OneShotEvents event) const { return values_[(int)event]; }

  bool Trigger(OneShotEvents event, double val, double expected = 0) {
    return values_[(int)event].compare_exchange_weak(expected, val);
  }

 private:
  std::atomic<double> *values_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventOneShot global_one_shot_events;

void FirstSuccessfulQuery();

void FirstFailedQuery();

}  // namespace memgraph::metrics
