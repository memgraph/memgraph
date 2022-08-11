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

#include "io/transport.hpp"

namespace memgraph::io::thrift {

using memgraph::io::Address;

class ThriftHandle {
 public:
  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, uint64_t request_id, Request &&request, Duration timeout,
                     ResponsePromise<Response> &&promise);

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout);

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, M message);
};

}  // namespace memgraph::io::thrift
