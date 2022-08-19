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

#include <condition_variable>
#include <map>
#include <mutex>

#include "io/errors.hpp"
#include "io/message_conversion.hpp"
#include "io/transport.hpp"

namespace memgraph::io::thrift {

using memgraph::io::Address;
using memgraph::io::OpaqueMessage;
using memgraph::io::OpaquePromise;
using memgraph::io::TimedOut;
using RequestId = uint64_t;

class ThriftHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;
  const Address address_ = Address::TestAddress(0);

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::vector<OpaqueMessage> can_receive_;

  // TODO(tyler) thrift clients for each outbound address combination
  // std::map<Address, void *> clients_;

  // TODO(gabor) make this to a threadpool
  // uuid of the address -> port number where the given rsm is residing.
  // TODO(gabor) The RSM map should not be a part of this class.
  // std::map<boost::uuids::uuid, uint16_t /*this should be the actual RSM*/> rsm_map_;

 public:
  ThriftHandle(Address our_address) : address_(our_address) {}

  Time Now() const {
    auto nano_time = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::microseconds>(nano_time);
  }

  template <Message M>
  void DeliverMessage(Address to_address, Address from_address, RequestId request_id, M &&message) {
    std::any message_any(std::move(message));
    OpaqueMessage opaque_message{
        .from_address = from_address, .request_id = request_id, .message = std::move(message_any)};

    PromiseKey promise_key{.requester_address = to_address,
                           .request_id = opaque_message.request_id,
                           .replier_address = opaque_message.from_address};

    {
      std::unique_lock<std::mutex> lock(mu_);

      if (promises_.contains(promise_key)) {
        // complete waiting promise if it's there
        DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
        promises_.erase(promise_key);

        dop.promise.Fill(std::move(opaque_message));
      } else {
        can_receive_.emplace_back(std::move(opaque_message));
      }
    }  // lock dropped

    cv_.notify_all();
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, RequestId request_id, Request &&request,
                     Duration timeout, ResponsePromise<Response> &&promise) {
    const Time deadline = Now() + timeout;
    Address our_address = address_;

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id, .replier_address = to_address};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(promise_key), std::move(dop));

    cv_.notify_all();

    bool port_matches = to_address.last_known_port == our_address.last_known_port;
    bool ip_matches = to_address.last_known_ip == our_address.last_known_ip;

    if (port_matches && ip_matches) {
      // hairpin routing optimization
      DeliverMessage(to_address, from_address, request_id, std::move(request));
    } else {
      // send using a thrift client to remove service
      Send(to_address, from_address, request_id, request);
    }
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Duration timeout) {
    // TODO(tyler) block for the specified duration on the Inbox's receipt of a message of this type.
    std::unique_lock lock(mu_);

    Time before = Now();

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
  void Send(Address to_address, Address from_address, RequestId request_id, M message) {
    // TODO(tyler) call thrift client for address (or create one if it doesn't exist yet)
  }
};

}  // namespace memgraph::io::thrift
