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

#include <memory>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/cfg/env.h>

#include "io/message_histogram_collector.hpp"
#include "io/simulator/simulator.hpp"
#include "utils/print_helpers.hpp"

using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::LatencyHistogramSummaries;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;

struct CounterRequest {
  uint64_t proposal;
};

struct CounterResponse {
  uint64_t highest_seen;
};

void run_server(Io<SimulatorTransport> io) {
  uint64_t highest_seen = 0;

  while (!io.ShouldShutDown()) {
    std::cout << "[SERVER] Is receiving..." << std::endl;
    auto request_result = io.Receive<CounterRequest>();
    if (request_result.HasError()) {
      std::cout << "[SERVER] Error, continue" << std::endl;
      continue;
    }
    std::cout << "[SERVER] Got message" << std::endl;
    auto request_envelope = request_result.GetValue();
    auto req = std::get<CounterRequest>(request_envelope.message);

    highest_seen = std::max(highest_seen, req.proposal);
    auto srv_res = CounterResponse{highest_seen};

    io.Send(request_envelope.from_address, request_envelope.request_id, std::move(srv_res));
  }
}

std::pair<SimulatorStats, LatencyHistogramSummaries> RunWorkload(SimulatorConfig &config) {
  auto simulator = Simulator(config);

  auto cli_addr = Address::TestAddress(1);
  auto srv_addr = Address::TestAddress(2);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> srv_io = simulator.Register(srv_addr);

  auto srv_thread = std::jthread(run_server, std::move(srv_io));
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr);

  for (int i = 1; i < 3; ++i) {
    // send request
    CounterRequest cli_req;
    cli_req.proposal = i;
    spdlog::info("[CLIENT] calling Request");
    auto res_f = cli_io.Request<CounterResponse>(srv_addr, std::move(cli_req));
    spdlog::info("[CLIENT] calling Wait");
    auto res_rez = std::move(res_f).Wait();
    spdlog::info("[CLIENT] Wait returned");
    if (!res_rez.HasError()) {
      spdlog::info("[CLIENT] Got a valid response");
      auto env = res_rez.GetValue();
      MG_ASSERT(env.message.highest_seen == i);
      spdlog::info("response latency: {} microseconds", env.response_latency.count());
    } else {
      spdlog::info("[CLIENT] Got an error");
    }
  }

  simulator.ShutDown();

  return std::make_pair(simulator.Stats(), cli_io.ResponseLatencies());
}

int main() {
  spdlog::cfg::load_env_levels();

  auto config = SimulatorConfig{
      .drop_percent = 0,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
  };

  auto [sim_stats_1, latency_stats_1] = RunWorkload(config);
  auto [sim_stats_2, latency_stats_2] = RunWorkload(config);

  if (sim_stats_1 != sim_stats_2 || latency_stats_1 != latency_stats_2) {
    spdlog::error("simulator stats diverged across runs");
    spdlog::error("run 1 simulator stats: {}", sim_stats_1);
    spdlog::error("run 2 simulator stats: {}", sim_stats_2);
    spdlog::error("run 1 latency:\n{}", latency_stats_1.SummaryTable());
    spdlog::error("run 2 latency:\n{}", latency_stats_2.SummaryTable());
    std::terminate();
  }

  return 0;
}
