// Copyright 2024 Memgraph Ltd.
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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define APPLY_FOR_GAUGES(M) M(PeakMemoryRes, MAX, Memory, "Peak res memory in the system.")

namespace memgraph::metrics {

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, GAUGE_TYPE, TYPE, DOCUMENTATION) extern const Event NAME = __COUNTER__;
APPLY_FOR_GAUGES(M)
#undef M

inline constexpr Event END = __COUNTER__;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Gauge global_gauges_array[END]{};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
GaugeType global_gauge_types_array[END] = {
// Initialize with corresponding types
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, GAUGE_TYPE, TYPE, DOCUMENTATION) GaugeType::GAUGE_TYPE,
    APPLY_FOR_GAUGES(M)
#undef M
};

// Initialize global gauges and types
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventGauges global_gauges(global_gauges_array);

const Event EventGauges::num_gauges = END;

void EventGauges::SetValue(const Event event, Value value) {
  auto current_value = gauges_[event].load(std::memory_order_acquire);
  switch (GetGaugeType(event)) {
    case GaugeType::MAX:
      if (value > current_value) {
        gauges_[event].store(value, std::memory_order_seq_cst);
      }
      break;
    case GaugeType::MIN:
      if (value < current_value) {
        gauges_[event].store(value, std::memory_order_seq_cst);
      }
      break;
    case GaugeType::CURRENT_VALUE:
      gauges_[event].store(value, std::memory_order_seq_cst);
      break;
  }
}

Value EventGauges::GetValue(const Event event) {
  auto current_value = gauges_[event].load(std::memory_order_acquire);
  return current_value;
}

void SetGaugeValue(const Event event, Value value) { global_gauges.SetValue(event, value); }
Value GetGaugeValue(const Event event) { return global_gauges.GetValue(event); }

const char *GetGaugeName(const Event event) {
  static const char *strings[] = {
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, GAUGE_TYPE, TYPE, DOCUMENTATION) #NAME,
      APPLY_FOR_GAUGES(M)
#undef M
  };
  return strings[event];
}

const char *GetGaugeDocumentation(const Event event) {
  static const char *strings[] = {
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, GAUGE_TYPE, TYPE, DOCUMENTATION) DOCUMENTATION,
      APPLY_FOR_GAUGES(M)
#undef M
  };
  return strings[event];
}

const char *GetGaugeTypeString(const Event event) {
  static const char *strings[] = {
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, GAUGE_TYPE, TYPE, DOCUMENTATION) #TYPE,
      APPLY_FOR_GAUGES(M)
#undef M
  };
  return strings[event];
}

GaugeType GetGaugeType(const Event event) { return global_gauge_types_array[event]; }

Event GaugeEnd() { return END; }

}  // namespace memgraph::metrics
