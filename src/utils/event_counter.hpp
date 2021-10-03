// Copyright 2021 Memgraph Ltd.
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

namespace EventCounter {
using Event = uint64_t;
using Count = uint64_t;
using Counter = std::atomic<Count>;

class EventCounters {
 public:
  explicit EventCounters(Counter *allocated_counters) noexcept : counters_(allocated_counters) {}

  auto &operator[](const Event event) { return counters_[event]; }

  const auto &operator[](const Event event) const { return counters_[event]; }

  void Increment(Event event, Count amount = 1);

  static const Event num_counters;

 private:
  Counter *counters_;
};

extern EventCounters global_counters;

void IncrementCounter(Event event, Count amount = 1);

const char *GetName(Event event);
const char *GetDocumentation(Event event);

Event End();

}  // namespace EventCounter
