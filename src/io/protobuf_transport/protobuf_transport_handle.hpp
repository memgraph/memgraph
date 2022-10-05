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

#include "google/protobuf/message.h"

#include "io/address.hpp"
#include "io/transport.hpp"
//#include "protobuf/messages.pb.cc"
#include "protobuf/messages.pb.h"

namespace memgraph::io::protobuf_transport {

using PbAddress = memgraph::protobuf::Address;
using memgraph::protobuf::UberMessage;

class ProtobufTransportHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;
  bool should_shut_down_ = false;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::vector<std::string> can_receive_;

  // serialized outbound messages
  std::vector<std::string> outbox_;

 public:
  ~ProtobufTransportHandle() {
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
    PromiseKey promise_key{.requester_address = to_address, .request_id = request_id, .replier_address = from_address};

    {
      std::unique_lock<std::mutex> lock(mu_);

      if (promises_.contains(promise_key)) {
        // hair-pin local message optimization
        spdlog::info("using message to fill local promise");
        DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
        promises_.erase(promise_key);

        std::any message_any(std::forward<M>(message));
        OpaqueMessage opaque_message{.to_address = to_address,
                                     .from_address = from_address,
                                     .request_id = request_id,
                                     .message = std::move(message_any)};

        dop.promise.Fill(std::move(opaque_message));
      } else {
        spdlog::info("placing message in outbox");

        // serialize protobuf message and place it in the outbox

        std::string bytes;
        bool success = message.SerializeToString(&bytes);
        MG_ASSERT(success);

        outbox_.emplace_back(std::move(bytes));
      }
    }  // lock dropped

    cv_.notify_all();
  }

  template <Message RequestT, Message ResponseT>
  void SubmitRequest(Address to_address, Address from_address, RequestId request_id, RequestT &&request,
                     Duration timeout, ResponsePromise<ResponseT> promise) {
    const Time deadline = Now() + timeout;

    {
      std::unique_lock<std::mutex> lock(mu_);

      PromiseKey promise_key{
          .requester_address = from_address, .request_id = request_id, .replier_address = to_address};
      OpaquePromise opaque_promise(std::move(promise).ToUnique());
      DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
      promises_.emplace(std::move(promise_key), std::move(dop));
    }  // lock dropped

    Send(to_address, from_address, request_id, std::forward<RequestT>(request));
  }
};

}  // namespace memgraph::io::protobuf_transport
