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

#include "io/message_conversion.hpp"
#include "io/transport.hpp"

namespace memgraph::io::thrift {

using memgraph::io::Address;
using memgraph::io::OpaqueMessage;
using memgraph::io::OpaquePromise;

class ThriftHandle {
  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::map<Address, std::vector<OpaqueMessage>> can_receive_;

  // TODO(tyler) thrift clients for each outbound address combination
  std::map<Address, void> clients_;

 public:
  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, uint64_t request_id, Request &&request, Duration timeout,
                     ResponsePromise<Response> &&promise) {
    // TODO(tyler) simular to simulator transport, add the promise to the promises_ map

    Send(to_address, from_address, request_id, request);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout) {
    // TODO(tyler) block for the specified duration on the Inbox's receipt of a message of this type.
  }

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, M message) {
    // TODO(tyler) call thrift client for address (or create one if it doesn't exist yet)
  }
};

}  // namespace memgraph::io::thrift
