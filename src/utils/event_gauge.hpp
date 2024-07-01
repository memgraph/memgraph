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

#pragma once

#include <atomic>
#include <cstdlib>
#include <memory>

namespace memgraph::metrics {

using Event = uint64_t;
using Value = uint64_t;
using Gauge = std::atomic<Value>;

enum class GaugeType { MAX, MIN, CURRENT_VALUE };

void SetGaugeValue(Event event, Value value);
Value GetGaugeValue(Event event);

const char *GetGaugeName(Event event);
const char *GetGaugeDocumentation(Event event);
const char *GetGaugeTypeString(Event event);
GaugeType GetGaugeType(Event event);

Event GaugeEnd();

class EventGauges {
 public:
  explicit EventGauges(Gauge *allocated_gauges) noexcept : gauges_(allocated_gauges) {
    for (auto i = 0; i < memgraph::metrics::GaugeEnd(); i++) {
      if (GetGaugeType(i) == GaugeType::MIN) {
        gauges_[i].store(UINT64_MAX, std::memory_order_seq_cst);
      }
    }
  }

  auto &operator[](const Event event) { return gauges_[event]; }

  const auto &operator[](const Event event) const { return gauges_[event]; }

  void SetValue(Event event, Value value);
  Value GetValue(Event event);

  static const Event num_gauges;

 private:
  Gauge *gauges_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventGauges global_gauges;
}  // namespace memgraph::metrics
