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

#include "io/time.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Time;

struct SimulatorConfig {
  uint8_t drop_percent = 0;
  bool perform_timeouts = false;
  bool scramble_messages = true;
  uint64_t rng_seed = 0;
  Time start_time = Time::min();
  Time abort_time = Time::max();
};
};  // namespace memgraph::io::simulator
