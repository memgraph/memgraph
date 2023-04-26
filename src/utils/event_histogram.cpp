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

#include "utils/event_histogram.hpp"

#define APPLY_FOR_HISTOGRAMS(M)                                                                  \
  M(QueryExecutionLatency_us, "Query execution latency in microseconds", 0, 25, 50, 75, 99, 100) \
  M(SnapshotCreationLatency_us, "Snapshot creation latency in microseconds", 50, 90, 100)

namespace Statistics {

// define every Event as an index in the array of counters
#define M(NAME, DOCUMENTATION, ...) extern const Event NAME = __COUNTER__;
APPLY_FOR_HISTOGRAMS(M)
#undef M

inline constexpr Event END = __COUNTER__;

// Initialize array for the global histogram with all named histograms and their percentiles
Histogram global_histograms_array[END]{
#define M(NAME, DOCUMENTATION, ...) Histogram({__VA_ARGS__}),
    APPLY_FOR_HISTOGRAMS(M)
#undef M
};
// Initialize global histograms
EventHistograms global_histograms(global_histograms_array);

const Event EventHistograms::num_histograms = END;

void Measure(const Event event, Value value) { global_histograms.Measure(event, value); }

void EventHistograms::Measure(const Event event, Value value) { histograms_[event].Measure(value); }

const char *GetHistogramName(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION, ...) #NAME,
      APPLY_FOR_HISTOGRAMS(M)
#undef M
  };

  return strings[event];
}

const char *GetHistogramDocumentation(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION, ...) DOCUMENTATION,
      APPLY_FOR_HISTOGRAMS(M)
#undef M
  };

  return strings[event];
}

Event HistogramEnd() { return END; }
}  // namespace Statistics
