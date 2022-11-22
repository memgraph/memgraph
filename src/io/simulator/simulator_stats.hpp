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

#pragma once

#include <cstdint>

#include <fmt/format.h>

#include "io/time.hpp"

namespace memgraph::io::simulator {
struct SimulatorStats {
  uint64_t total_messages = 0;
  uint64_t dropped_messages = 0;
  uint64_t timed_out_requests = 0;
  uint64_t total_requests = 0;
  uint64_t total_responses = 0;
  uint64_t simulator_ticks = 0;
  Duration elapsed_time;

  friend bool operator==(const SimulatorStats & /* lhs */, const SimulatorStats & /* rhs */) = default;

  friend std::ostream &operator<<(std::ostream &in, const SimulatorStats &stats) {
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stats.elapsed_time).count() / 1000;

    std::string formated = fmt::format(
        "SimulatorStats {{ total_messages: {}, dropped_messages: {}, timed_out_requests: {}, total_requests: {}, "
        "total_responses: {}, simulator_ticks: {}, elapsed_time: {}ms }}",
        stats.total_messages, stats.dropped_messages, stats.timed_out_requests, stats.total_requests,
        stats.total_responses, stats.simulator_ticks, elapsed_ms);

    in << formated;

    return in;
  }
};
};  // namespace memgraph::io::simulator
