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

#include "utils/event_gauge.hpp"

// We don't have any gauges for now
#define APPLY_FOR_GAUGES(M)

namespace memgraph::metrics {

// define every Event as an index in the array of gauges
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) extern const Event NAME = __COUNTER__;
APPLY_FOR_GAUGES(M)
#undef M

inline constexpr Event END = __COUNTER__;

// Initialize array for the global gauges with all values set to 0
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Gauge global_gauges_array[END]{};

// Initialize global counters
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventGauges global_gauges(global_gauges_array);

const Event EventGauges::num_gauges = END;

void EventGauges::SetValue(const Event event, Value value) { gauges_[event].store(value, std::memory_order_seq_cst); }

void SetGaugeValue(const Event event, Value value) { global_gauges.SetValue(event, value); }

const char *GetGaugeName(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) #NAME,
      APPLY_FOR_GAUGES(M)
#undef M
  };

  return strings[event];
}

const char *GetGaugeDocumentation(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) DOCUMENTATION,
      APPLY_FOR_GAUGES(M)
#undef M
  };

  return strings[event];
}

const char *GetGaugeType(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) #TYPE,
      APPLY_FOR_GAUGES(M)
#undef M
  };

  return strings[event];
}

Event GaugeEnd() { return END; }

}  // namespace memgraph::metrics
