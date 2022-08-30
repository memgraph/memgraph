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

#include "io/simulator/simulator_handle.hpp"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_stats.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Duration;
using memgraph::io::Time;

void SimulatorHandle::ShutDown() {
  std::unique_lock<std::mutex> lock(mu_);
  should_shut_down_ = true;
  cv_.notify_all();
}

bool SimulatorHandle::ShouldShutDown() const {
  std::unique_lock<std::mutex> lock(mu_);
  return should_shut_down_;
}

void SimulatorHandle::IncrementServerCountAndWaitForQuiescentState(Address address) {
  std::unique_lock<std::mutex> lock(mu_);
  server_addresses_.insert(address);

  while (true) {
    const size_t blocked_servers = BlockedServers();

    const bool all_servers_blocked = blocked_servers == server_addresses_.size();

    if (all_servers_blocked) {
      return;
    }

    cv_.wait(lock);
  }
}

size_t SimulatorHandle::BlockedServers() {
  size_t blocked_servers = blocked_on_receive_;

  return blocked_servers;
}

bool SimulatorHandle::MaybeTickSimulator() {
  std::unique_lock<std::mutex> lock(mu_);

  const size_t blocked_servers = BlockedServers();

  if (blocked_servers < server_addresses_.size()) {
    // we only need to advance the simulator when all
    // servers have reached a quiescent state, blocked
    // on their own futures or receive methods.
    return false;
  }

  stats_.simulator_ticks++;

  cv_.notify_all();

  TimeoutPromisesPastDeadline();

  if (in_flight_.empty()) {
    // return early here because there are no messages to schedule

    // We tick the clock forward when all servers are blocked but
    // there are no in-flight messages to schedule delivery of.
    std::poisson_distribution<> time_distrib(50);
    Duration clock_advance = std::chrono::microseconds{time_distrib(rng_)};
    cluster_wide_time_microseconds_ += clock_advance;

    MG_ASSERT(cluster_wide_time_microseconds_ < config_.abort_time,
              "Cluster has executed beyond its configured abort_time, and something may be failing to make progress "
              "in an expected amount of time.");
    return true;
  }

  if (config_.scramble_messages) {
    // scramble messages
    std::uniform_int_distribution<size_t> swap_distrib(0, in_flight_.size() - 1);
    const size_t swap_index = swap_distrib(rng_);
    std::swap(in_flight_[swap_index], in_flight_.back());
  }

  auto [to_address, opaque_message] = std::move(in_flight_.back());
  in_flight_.pop_back();

  std::uniform_int_distribution<int> drop_distrib(0, 99);
  const int drop_threshold = drop_distrib(rng_);
  const bool should_drop = drop_threshold < config_.drop_percent;

  if (should_drop) {
    stats_.dropped_messages++;
  }

  PromiseKey promise_key{.requester_address = to_address,
                         .request_id = opaque_message.request_id,
                         .replier_address = opaque_message.from_address};

  if (promises_.contains(promise_key)) {
    // complete waiting promise if it's there
    DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
    promises_.erase(promise_key);

    const bool normal_timeout = config_.perform_timeouts && (dop.deadline < cluster_wide_time_microseconds_);

    if (should_drop || normal_timeout) {
      stats_.timed_out_requests++;
      dop.promise.TimeOut();
    } else {
      stats_.total_responses++;
      dop.promise.Fill(std::move(opaque_message));
    }
  } else if (should_drop) {
    // don't add it anywhere, let it drop
  } else {
    // add to can_receive_ if not
    const auto &[om_vec, inserted] = can_receive_.try_emplace(to_address, std::vector<OpaqueMessage>());
    om_vec->second.emplace_back(std::move(opaque_message));
  }

  return true;
}

Time SimulatorHandle::Now() const {
  std::unique_lock<std::mutex> lock(mu_);
  return cluster_wide_time_microseconds_;
}

SimulatorStats SimulatorHandle::Stats() {
  std::unique_lock<std::mutex> lock(mu_);
  return stats_;
}
}  // namespace memgraph::io::simulator
