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

#include "io/message_conversion.hpp"
#include "io/transport.hpp"

namespace memgraph::io::thrift {

using memgraph::io::Address;
using memgraph::io::OpaqueMessage;
using memgraph::io::OpaquePromise;
using RequestId = uint64_t;

class ThriftHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;

  // the responses to requests that are being waited on
  std::map<RequestId, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::vector<OpaqueMessage> can_receive_;

  // TODO(tyler) thrift clients for each outbound address combination
  std::map<Address, void *> clients_;

  // TODO(gabor) make this to a threadpool
  // uuid of the address -> port number where the given rsm is residing.
  // TODO(gabor) The RSM map should not be a part of this class.
  std::map<boost::uuids::uuid, uint16_t /*this should be the actual RSM*/> rsm_map_;

 public:
  template <Message M>
  void DeliverMessage(Address from_address, RequestId request_id, M &&message) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      std::any message_any(std::move(message));
      OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message_any)};
      can_receive_.emplace_back(std::move(om));
    }  // lock dropped

    cv_.notify_all();
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, RequestId request_id, Request &&request,
                     Duration timeout, ResponsePromise<Response> &&promise) {
    // TODO(tyler) simular to simulator transport, add the promise to the promises_ map

    Send(to_address, from_address, request_id, request);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout) {
    // TODO(tyler) block for the specified duration on the Inbox's receipt of a message of this type.
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !can_receive_.empty(); });

    while (!can_receive_.empty()) {
      auto current_message = can_receive_.back();
      can_receive_.pop_back();

      // Logic to determine who to send the message.
      //
      auto destination_id = current_message.to_address.unique_id;
      auto destination_port = rsm_map_.at(destination_id);

      // Send it to the port of the destination -how?
      // TODO(tyler) search for item in can_receive_ that matches the desired types, rather
      // than asserting that the last item in can_rx matches.
      auto m_opt = std::move(current_message).Take<Ms...>();

      return (std::move(m_opt));
    }
  }

  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, M message) {
    // TODO(tyler) call thrift client for address (or create one if it doesn't exist yet)
  }
};

}  // namespace memgraph::io::thrift
