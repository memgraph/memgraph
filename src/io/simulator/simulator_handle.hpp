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

#include <any>
#include <compare>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <variant>
#include <vector>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/simulator/message_conversion.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_stats.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Duration;
using memgraph::io::Time;

struct PromiseKey {
  Address requester_address;
  uint64_t request_id;
  // TODO(tyler) possibly remove replier_address from promise key
  // once we want to support DSR.
  Address replier_address;

 public:
  bool operator<(const PromiseKey &other) const {
    if (requester_address != other.requester_address) {
      return requester_address < other.requester_address;
    }

    if (request_id != other.request_id) {
      return request_id < other.request_id;
    }

    return replier_address < other.replier_address;
  }
};

struct DeadlineAndOpaquePromise {
  Time deadline;
  OpaquePromise promise;
};

class SimulatorHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;

  // messages that have not yet been scheduled or dropped
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::map<Address, std::vector<OpaqueMessage>> can_receive_;

  Time cluster_wide_time_microseconds_;
  bool should_shut_down_ = false;
  SimulatorStats stats_;
  size_t blocked_on_receive_ = 0;
  std::set<Address> server_addresses_;
  std::mt19937 rng_;
  SimulatorConfig config_;

  void TimeoutPromisesPastDeadline() {
    const Time now = cluster_wide_time_microseconds_;

    for (auto &[promise_key, dop] : promises_) {
      if (dop.deadline < now) {
        spdlog::debug("timing out request from requester {} to replier {}.", promise_key.requester_address.ToString(),
                      promise_key.replier_address.ToString());
        std::move(dop).promise.TimeOut();
        promises_.erase(promise_key);

        stats_.timed_out_requests++;
      }
    }
  }

 public:
  explicit SimulatorHandle(SimulatorConfig config)
      : cluster_wide_time_microseconds_(config.start_time), rng_(config.rng_seed), config_(config) {}

  void IncrementServerCountAndWaitForQuiescentState(Address address);

  /// This method causes most of the interesting simulation logic to happen, wrt network behavior.
  /// It checks to see if all background "server" threads are blocked on new messages, and if so,
  /// it will decide whether to drop, reorder, or deliver in-flight messages based on the SimulatorConfig
  /// that was used to create the Simulator.
  bool MaybeTickSimulator();

  void ShutDown();

  bool ShouldShutDown() const;

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, uint64_t request_id, Request &&request, Duration timeout,
                     ResponsePromise<Response> &&promise) {
    std::unique_lock<std::mutex> lock(mu_);

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    std::any message(request);
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message)};
    in_flight_.emplace_back(std::make_pair(to_address, std::move(om)));

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id, .replier_address = to_address};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(promise_key), std::move(dop));

    stats_.total_messages++;
    stats_.total_requests++;

    cv_.notify_all();
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout) {
    std::unique_lock<std::mutex> lock(mu_);

    blocked_on_receive_ += 1;

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    while (!should_shut_down_ && (cluster_wide_time_microseconds_ < deadline)) {
      if (can_receive_.contains(receiver)) {
        std::vector<OpaqueMessage> &can_rx = can_receive_.at(receiver);
        if (!can_rx.empty()) {
          OpaqueMessage message = std::move(can_rx.back());
          can_rx.pop_back();

          // TODO(tyler) search for item in can_receive_ that matches the desired types, rather
          // than asserting that the last item in can_rx matches.
          auto m_opt = std::move(message).Take<Ms...>();

          blocked_on_receive_ -= 1;

          return std::move(m_opt).value();
        }
      }

      lock.unlock();
      bool made_progress = MaybeTickSimulator();
      lock.lock();
      if (!should_shut_down_ && !made_progress) {
        cv_.wait(lock);
      }
    }

    blocked_on_receive_ -= 1;

    return TimedOut{};
  }

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, M message) {
    std::unique_lock<std::mutex> lock(mu_);
    std::any message_any(std::move(message));
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message_any)};
    in_flight_.emplace_back(std::make_pair(std::move(to_address), std::move(om)));

    stats_.total_messages++;

    cv_.notify_all();
  }

  Time Now() const;

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    std::unique_lock<std::mutex> lock(mu_);
    return distrib(rng_);
  }

  SimulatorStats Stats();
};
};  // namespace memgraph::io::simulator
