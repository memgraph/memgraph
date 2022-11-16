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
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "fmt/format.h"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/message_conversion.hpp"
#include "io/message_histogram_collector.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_stats.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::io::simulator {

struct EventDescriptor {
  boost::asio::ip::address from_address_ip;
  uint16_t from_address_port;
  boost::asio::ip::address to_address_ip;
  uint16_t to_address_port;
  std::string_view caller;
  utils::TypeInfoRef type_info;
  uint64_t event_id;
};

class EventLog {
  std::vector<EventDescriptor> event_log_;
  static constexpr const char *eventlog_signal_string_ = "EVENTLOG_UPDATE";

  void LogEvent(const EventDescriptor &event) const {
    spdlog::info(
        fmt::format("{} -> caller: {}, from_address: {}:{}, to_address: {}:{}, type: {}, id: {}, thread_id: {}",
                    eventlog_signal_string_, event.caller, event.from_address_ip.to_string(), event.from_address_port,
                    event.to_address_ip.to_string(), event.to_address_port, event.type_info.get().name(),
                    event.event_id, std::this_thread::get_id()));
  }

 public:
  void AddAndLog(const OpaqueMessage &message, std::string_view caller) {
    EventDescriptor event{.from_address_ip = message.from_address.last_known_ip,
                          .from_address_port = message.from_address.last_known_port,
                          .to_address_ip = message.to_address.last_known_ip,
                          .to_address_port = message.to_address.last_known_port,
                          .caller = caller,
                          .type_info = message.type_info,
                          .event_id = event_log_.size()};
    event_log_.push_back(event);
    LogEvent(event);
  }

  void LogAllEvents() const {
    for (const auto &event : event_log_) {
      LogEvent(event);
    }
  }
};

class SimulatorHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;

  // messages that have not yet been scheduled or dropped
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::map<PartialAddress, std::vector<OpaqueMessage>> can_receive_;

  // maybe a boolean here
  bool is_quiescent_state_achieved_ = false;
  std::atomic<int> server_count_;

  Time cluster_wide_time_microseconds_;
  bool should_shut_down_ = false;
  SimulatorStats stats_;
  std::set<Address> blocked_on_receive_;
  std::set<Address> server_addresses_;
  std::mt19937 rng_;
  std::uniform_int_distribution<int> time_distrib_{5, 50};
  std::uniform_int_distribution<int> drop_distrib_{0, 99};
  SimulatorConfig config_;
  MessageHistogramCollector histograms_;
  RequestId request_id_counter_{0};
  EventLog event_log_;

  void TimeoutPromisesPastDeadline() {
    const Time now = cluster_wide_time_microseconds_;
    for (auto it = promises_.begin(); it != promises_.end();) {
      auto &[promise_key, dop] = *it;
      const bool timed_out = dop.deadline < now;
      if (timed_out && config_.perform_timeouts) {
        // spdlog::info("timing out request from requester {}.", promise_key.requester_address.ToString());
        spdlog::info("timing out request from requester {}. bool timed_out: {}",
                     promise_key.requester_address.ToString(), timed_out);
        std::move(dop).promise.TimeOut();
        it = promises_.erase(it);

        stats_.timed_out_requests++;
      } else {
        ++it;
      }
    }
  }

 public:
  explicit SimulatorHandle(SimulatorConfig config)
      : server_count_(0), cluster_wide_time_microseconds_(config.start_time), rng_(config.rng_seed), config_(config) {
    spdlog::info("SimulatorHandle constructed.");
  }

  LatencyHistogramSummaries ResponseLatencies();

  ~SimulatorHandle() {
    for (auto it = promises_.begin(); it != promises_.end();) {
      auto &[promise_key, dop] = *it;
      std::move(dop).promise.TimeOut();
      it = promises_.erase(it);
    }
  }

  void IncrementServerCountAndWaitForQuiescentState(Address address);

  /// This method causes most of the interesting simulation logic to happen, wrt network behavior.
  /// It checks to see if all background "server" threads are blocked on new messages, and if so,
  /// it will decide whether to drop, reorder, or deliver in-flight messages based on the SimulatorConfig
  /// that was used to create the Simulator.
  bool MaybeTickSimulator();

  void ShutDown();

  bool ShouldShutDown() const;

  template <Message Request, Message Response>
  ResponseFuture<Response> SubmitRequest(Address to_address, Address from_address, Request &&request, Duration timeout,
                                         std::function<bool()> &&maybe_tick_simulator) {
    auto type_info = TypeInfoFor(request);

    auto [future, promise] = memgraph::io::FuturePromisePairWithNotifier<ResponseResult<Response>>(
        std::forward<std::function<bool()>>(maybe_tick_simulator));

    std::unique_lock<std::mutex> lock(mu_);

    RequestId request_id = ++request_id_counter_;

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    std::any message(request);
    OpaqueMessage om{.to_address = to_address,
                     .from_address = from_address,
                     .request_id = request_id,
                     .message = std::move(message),
                     .type_info = type_info};
    event_log_.AddAndLog(om, "SubmitRequest");
    in_flight_.emplace_back(std::make_pair(to_address, std::move(om)));

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{
        .requested_at = cluster_wide_time_microseconds_,
        .deadline = deadline,
        .promise = std::move(opaque_promise),
    };

    MG_ASSERT(!promises_.contains(promise_key));

    promises_.emplace(std::move(promise_key), std::move(dop));

    stats_.total_messages++;
    stats_.total_requests++;

    cv_.notify_all();

    return std::move(future);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout) {
    std::unique_lock<std::mutex> lock(mu_);

    blocked_on_receive_.emplace(receiver);

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    auto partial_address = receiver.ToPartialAddress();

    while (!should_shut_down_ && (cluster_wide_time_microseconds_ < deadline)) {
      if (can_receive_.contains(partial_address)) {
        std::vector<OpaqueMessage> &can_rx = can_receive_.at(partial_address);
        if (!can_rx.empty()) {
          OpaqueMessage message = std::move(can_rx.back());
          can_rx.pop_back();

          event_log_.AddAndLog(message, "Receivexxxxxx");

          // TODO(tyler) search for item in can_receive_ that matches the desired types, rather
          // than asserting that the last item in can_rx matches.
          auto m_opt = std::move(message).Take<Ms...>();
          MG_ASSERT(m_opt.has_value(), "Wrong message type received compared to the expected type");

          blocked_on_receive_.erase(receiver);

          return std::move(m_opt).value();
        }
      }

      lock.unlock();
      auto simulator_progress = MaybeTickSimulator();
      lock.lock();
      if (!should_shut_down_ && !simulator_progress) {
        cv_.wait(lock);
      }
    }

    blocked_on_receive_.erase(receiver);

    return TimedOut{};
  }

  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, M message) {
    auto type_info = TypeInfoFor(message);
    std::unique_lock<std::mutex> lock(mu_);
    std::any message_any(std::move(message));
    OpaqueMessage om{.to_address = to_address,
                     .from_address = from_address,
                     .request_id = request_id,
                     .message = std::move(message_any),
                     .type_info = type_info};
    event_log_.AddAndLog(om, "Sendxxxxxxxxx");
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
