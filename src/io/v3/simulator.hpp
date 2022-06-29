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

#include <variant>

#include "address.hpp"
#include "errors.hpp"
#include "future.hpp"
#include "simulator_handle.hpp"
#include "transport.hpp"

struct SimulatorStats {
  uint64_t total_messages_;
  uint64_t dropped_messages_;
  uint64_t total_requests_;
  uint64_t total_responses_;
  uint64_t simulator_ticks_;
}

struct SimulatorConfig {
  uint8_t drop_percent_;
  uint64_t rng_seed_;
}

class SimulatorHandle {
 public:
  void NotifySimulator() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_sim_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_sim_;
  std::condition_variable cv_srv_;
};

class SimulatorTransport {
 public:
  SimulatorTransport(std::shared_ptr<SimulatorHandle> simulator_handle, Address address)
      : simulator_handle_(simulator_handle), address_(address) {}

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
  Address address_;
};

class Simulator {
 public:
  SimulatorTransport Register(Address address, bool is_server) {
    return SimulatorTransport(simulator_handle_, address);
  }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
};

namespace _compile_test {
void use_it() {
  auto simulator = Simulator();
  auto addr_1 = Address();
  auto addr_2 = Address();
  auto addr_3 = Address();

  auto sim_transport_1 = simulator.Register(addr_1, true);
  auto sim_transport_2 = simulator.Register(addr_2, true);
  auto sim_transport_3 = simulator.Register(addr_3, true);
}
}  // namespace _compile_test
