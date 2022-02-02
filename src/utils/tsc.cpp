// Copyright 2022 Memgraph Ltd.
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

namespace utils {
uint64_t ReadTSC() { return rdtsc(); }

std::optional<double> GetTSCFrequency() {
  // init is only needed for fetching frequency
  static auto result = std::invoke([] { return rdtsc_init(); });
  return result == 0 ? std::optional{rdtsc_get_tsc_hz()} : std::nullopt;
}

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

}  // namespace utils
