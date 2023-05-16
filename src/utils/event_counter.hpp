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
using Event = uint64_t;
using Count = uint64_t;
using Counter = std::atomic<Count>;

class EventCounters {
 public:
  explicit EventCounters(Counter *allocated_counters) noexcept : counters_(allocated_counters) {}

  auto &operator[](const Event event) { return counters_[event]; }

  const auto &operator[](const Event event) const { return counters_[event]; }

  void Increment(Event event, Count amount = 1);

  void Decrement(Event event, Count amount = 1);

  static const Event num_counters;

 private:
  Counter *counters_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventCounters global_counters;

void IncrementCounter(Event event, Count amount = 1);
void DecrementCounter(Event event, Count amount = 1);

const char *GetCounterName(Event event);
const char *GetCounterDocumentation(Event event);
const char *GetCounterType(Event event);

Event CounterEnd();
}  // namespace memgraph::metrics
