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

#include <functional>

extern "C" {
#include <librdtsc/rdtsc.h>
}

#include "utils/tsc.hpp"

namespace memgraph::utils {
uint64_t ReadTSC() { return rdtsc(); }

bool IsAvailableTSC() {
  // init is only needed for fetching frequency
  static bool available = [] { return rdtsc_init() == 0; }();  // iile
  return available;
}

std::optional<double> GetTSCFrequency() { return IsAvailableTSC() ? std::optional{rdtsc_get_tsc_hz()} : std::nullopt; }

TSCTimer::TSCTimer(std::optional<double> frequency) : frequency_(frequency) {
  if (!frequency_) return;
  start_value_ = utils::ReadTSC();
}

double TSCTimer::Elapsed() const {
  if (!frequency_) return 0.0;
  auto current_value = utils::ReadTSC();
  auto delta = current_value - start_value_;
  return static_cast<double>(delta) / *frequency_;
}

}  // namespace memgraph::utils
