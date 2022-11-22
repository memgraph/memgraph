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
#include "utils/exceptions.hpp"

namespace memgraph::io::simulator {

void SimulatorHandle::ShutDown() {
  std::unique_lock<std::mutex> lock(mu_);
  should_shut_down_ = true;
  for (auto it = promises_.begin(); it != promises_.end();) {
    auto &[promise_key, dop] = *it;
    std::move(dop).promise.TimeOut();
    it = promises_.erase(it);
  }
  can_receive_.clear();
  cv_.notify_all();
}

bool SimulatorHandle::ShouldShutDown() const {
  std::unique_lock<std::mutex> lock(mu_);
  return should_shut_down_;
}

LatencyHistogramSummaries SimulatorHandle::ResponseLatencies() {
  std::unique_lock<std::mutex> lock(mu_);
  return histograms_.ResponseLatencies();
}

void SimulatorHandle::IncrementServerCountAndWaitForQuiescentState(Address address) {
  std::unique_lock<std::mutex> lock(mu_);
  server_addresses_.insert(address);

  while (true) {
    const size_t blocked_servers = blocked_on_receive_.size();

    const bool all_servers_blocked = blocked_servers == server_addresses_.size();

    if (all_servers_blocked) {
      spdlog::trace("quiescent state detected - {} out of {} servers now blocked on receive", blocked_servers,
                    server_addresses_.size());
      return;
    }

    spdlog::trace("not returning from quiescent because we see {} blocked out of {}", blocked_servers,
                  server_addresses_.size());
    cv_.wait(lock);
  }
}

bool SortInFlight(const std::pair<Address, OpaqueMessage> &lhs, const std::pair<Address, OpaqueMessage> &rhs) {
  // NB: never sort based on the request ID etc...
  // This should only be used from std::stable_sort
  // because by comparing on the from_address alone,
  // we expect the sender ordering to remain
  // deterministic.
  const auto &[addr_1, om_1] = lhs;
  const auto &[addr_2, om_2] = rhs;
  return om_1.from_address < om_2.from_address;
}

bool SimulatorHandle::MaybeTickSimulator() {
  std::unique_lock<std::mutex> lock(mu_);

  const size_t blocked_servers = blocked_on_receive_.size();

  if (should_shut_down_ || blocked_servers < server_addresses_.size()) {
    // we only need to advance the simulator when all
    // servers have reached a quiescent state, blocked
    // on their own futures or receive methods.
    return false;
  }

  // We allow the simulator to progress the state of the system only
  // after all servers are blocked on receive.
  spdlog::trace("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ simulator tick ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  stats_.simulator_ticks++;
  blocked_on_receive_.clear();
  cv_.notify_all();

  bool timed_anything_out = TimeoutPromisesPastDeadline();

  if (timed_anything_out) {
    spdlog::trace("simulator progressing: timed out a request");
  }

  const Duration clock_advance = std::chrono::microseconds{time_distrib_(rng_)};

  // We don't always want to advance the clock with every message that we deliver because
  // when we advance it for every message, it causes timeouts to occur for any "request path"
  // over a certain length. Alternatively, we don't want to simply deliver multiple messages
  // in a single simulator tick because that would reduce the amount of concurrent message
  // mixing that may naturally occur in production. This approach is to mod the random clock
  // advance by a prime number (hopefully avoiding most harmonic effects that would be introduced
  // by only advancing the clock by an even amount etc...) and only advancing the clock close to
  // half of the time.
  if (clock_advance.count() % 97 > 49) {
    spdlog::trace("simulator progressing: clock advanced by {}", clock_advance.count());
    cluster_wide_time_microseconds_ += clock_advance;
    stats_.elapsed_time = cluster_wide_time_microseconds_ - config_.start_time;
  }

  if (cluster_wide_time_microseconds_ >= config_.abort_time) {
    spdlog::error(
        "Cluster has executed beyond its configured abort_time, and something may be failing to make progress "
        "in an expected amount of time.");
    throw utils::BasicException{"Cluster has executed beyond its configured abort_time"};
  }

  if (in_flight_.empty()) {
    return true;
  }

  std::stable_sort(in_flight_.begin(), in_flight_.end(), SortInFlight);

  if (config_.scramble_messages && in_flight_.size() > 1) {
    // scramble messages
    std::uniform_int_distribution<size_t> swap_distrib(0, in_flight_.size() - 1);
    const size_t swap_index = swap_distrib(rng_);
    std::swap(in_flight_[swap_index], in_flight_.back());
  }

  auto [to_address, opaque_message] = std::move(in_flight_.back());
  in_flight_.pop_back();

  const int drop_threshold = drop_distrib_(rng_);
  const bool should_drop = drop_threshold < config_.drop_percent;

  if (should_drop) {
    stats_.dropped_messages++;
  }

  PromiseKey promise_key{.requester_address = to_address, .request_id = opaque_message.request_id};

  if (promises_.contains(promise_key)) {
    // complete waiting promise if it's there
    DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
    promises_.erase(promise_key);

    const bool normal_timeout = config_.perform_timeouts && (dop.deadline < cluster_wide_time_microseconds_);

    if (should_drop || normal_timeout) {
      stats_.timed_out_requests++;
      dop.promise.TimeOut();
      spdlog::trace("simulator timing out request ");
    } else {
      stats_.total_responses++;
      Duration response_latency = cluster_wide_time_microseconds_ - dop.requested_at;
      auto type_info = opaque_message.type_info;
      dop.promise.Fill(std::move(opaque_message), response_latency);
      histograms_.Measure(type_info, response_latency);
      spdlog::trace("simulator replying to request");
    }
  } else if (should_drop) {
    // don't add it anywhere, let it drop
    spdlog::trace("simulator silently dropping request");
  } else {
    // add to can_receive_ if not
    spdlog::trace("simulator adding message to can_receive_ from {} to {}", opaque_message.from_address.last_known_port,
                  opaque_message.to_address.last_known_port);
    const auto &[om_vec, inserted] =
        can_receive_.try_emplace(to_address.ToPartialAddress(), std::vector<OpaqueMessage>());
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
