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

#include <chrono>
#include <compare>
#include <ctime>
#include <iomanip>

#include "io/time.hpp"

namespace memgraph::coordinator {

using Time = memgraph::io::Time;

/// Hybrid-logical clock
struct Hlc {
  uint64_t logical_id = 0;
  Time coordinator_wall_clock = Time::min();

  auto operator<=>(const Hlc &other) const { return logical_id <=> other.logical_id; }

  bool operator==(const Hlc &other) const = default;
  bool operator<(const Hlc &other) const = default;
  bool operator==(const uint64_t other) const { return logical_id == other; }
  bool operator<(const uint64_t other) const { return logical_id < other; }
  bool operator>=(const uint64_t other) const { return logical_id >= other; }

  friend std::ostream &operator<<(std::ostream &in, const Hlc &hlc) {
    auto wall_clock = std::chrono::system_clock::to_time_t(hlc.coordinator_wall_clock);
    in << "Hlc { logical_id: " << (int)hlc.logical_id;
    in << ", coordinator_wall_clock: " << std::put_time(std::localtime(&wall_clock), "%F %T");
    in << " }";

    return in;
  }
};

}  // namespace memgraph::coordinator
