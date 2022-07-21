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

#include <memory>

#include "io/address.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_handle.hpp"
#include "io/simulator/simulator_transport.hpp"

namespace memgraph::io::simulator {
class Simulator {
  std::mt19937 rng_{};
  std::shared_ptr<SimulatorHandle> simulator_handle_;

 public:
  explicit Simulator(SimulatorConfig config)
      : rng_(std::mt19937{config.rng_seed}), simulator_handle_{std::make_shared<SimulatorHandle>(config)} {}

  void ShutDown() { simulator_handle_->ShutDown(); }

  Io<SimulatorTransport> Register(Address address) {
    std::uniform_int_distribution<uint64_t> seed_distrib{};
    uint64_t seed = seed_distrib(rng_);
    return Io(SimulatorTransport(simulator_handle_, address, seed), address);
  }

  void IncrementServerCountAndWaitForQuiescentState(Address address) {
    simulator_handle_->IncrementServerCountAndWaitForQuiescentState(address);
  }

  SimulatorStats Stats() { return simulator_handle_->Stats(); }
};
};  // namespace memgraph::io::simulator
