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
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>

#include "io/errors.hpp"
#include "io/message_conversion.hpp"
#include "io/message_histogram_collector.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::local_transport {

class LocalTransportHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;
  bool should_shut_down_ = false;
  MessageHistogramCollector histograms_;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::vector<OpaqueMessage> can_receive_;

 public:
  ~LocalTransportHandle() {
    for (auto &&[pk, promise] : promises_) {
      std::move(promise.promise).TimeOut();
    }
    promises_.clear();
  }

  void ShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    should_shut_down_ = true;
    cv_.notify_all();
  }

  bool ShouldShutDown() const {
    std::unique_lock<std::mutex> lock(mu_);
    return should_shut_down_;
  }

  std::unordered_map<std::string, LatencyHistogramSummary> ResponseLatencies() {
    std::unique_lock<std::mutex> lock(mu_);
    return histograms_.ResponseLatencies();
  }

  static Time Now() {
    auto nano_time = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::microseconds>(nano_time);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address /* receiver_address */, Duration timeout) {
    std::unique_lock lock(mu_);

    Time before = Now();

    spdlog::info("can_receive_ size: {}", can_receive_.size());

    while (can_receive_.empty()) {
      Time now = Now();

      // protection against non-monotonic timesources
      auto maxed_now = std::max(now, before);
      auto elapsed = maxed_now - before;

      if (timeout < elapsed) {
        return TimedOut{};
      }

      Duration relative_timeout = timeout - elapsed;

      std::cv_status cv_status_value = cv_.wait_for(lock, relative_timeout);

      if (cv_status_value == std::cv_status::timeout) {
        return TimedOut{};
      }
    }

    auto current_message = std::move(can_receive_.back());
    can_receive_.pop_back();

    auto m_opt = std::move(current_message).Take<Ms...>();

    return std::move(m_opt).value();
  }

  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, M &&message) {
    std::any message_any(std::forward<M>(message));
    OpaqueMessage opaque_message{.to_address = to_address,
                                 .from_address = from_address,
                                 .request_id = request_id,
                                 .message = std::move(message_any)};

    PromiseKey promise_key{
        .requester_address = to_address, .request_id = opaque_message.request_id, .replier_address = from_address};

    {
      std::unique_lock<std::mutex> lock(mu_);

      if (promises_.contains(promise_key)) {
        spdlog::info("using message to fill promise");
        // complete waiting promise if it's there
        DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
        promises_.erase(promise_key);

        Duration response_latency = Now() - dop.requested_at;

        dop.promise.Fill(std::move(opaque_message), response_latency);
        histograms_.Measure(dop.response_type_id, response_latency);
      } else {
        spdlog::info("placing message in can_receive_");
        can_receive_.emplace_back(std::move(opaque_message));
      }
    }  // lock dropped

    cv_.notify_all();
  }

  template <Message RequestT, Message ResponseT>
  void SubmitRequest(Address to_address, Address from_address, RequestId request_id, RequestT &&request,
                     Duration timeout, ResponsePromise<ResponseT> promise) {
    const bool port_matches = to_address.last_known_port == from_address.last_known_port;
    const bool ip_matches = to_address.last_known_ip == from_address.last_known_ip;

    MG_ASSERT(port_matches && ip_matches);

    const auto now = Now();
    const Time deadline = now + timeout;

    {
      std::unique_lock<std::mutex> lock(mu_);

      PromiseKey promise_key{
          .requester_address = from_address, .request_id = request_id, .replier_address = to_address};
      OpaquePromise opaque_promise(std::move(promise).ToUnique());
      DeadlineAndOpaquePromise dop{.requested_at = now,
                                   .deadline = deadline,
                                   .promise = std::move(opaque_promise),
                                   .response_type_id = typeid(ResponseT)};
      promises_.emplace(std::move(promise_key), std::move(dop));
    }  // lock dropped

    Send(to_address, from_address, request_id, std::forward<RequestT>(request));
  }
};

}  // namespace memgraph::io::local_transport
