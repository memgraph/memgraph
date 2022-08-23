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
#include <memory>
#include <random>
#include <utility>

#include "io/address.hpp"
#include "io/local_transport/local_transport_handle.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::local_transport {

using memgraph::io::Duration;
using memgraph::io::Time;

class LocalTransport {
  std::shared_ptr<LocalTransportHandle> local_transport_handle_;
  const Address address_;
  std::random_device rng_;

 public:
  LocalTransport(std::shared_ptr<LocalTransportHandle> local_transport_handle, Address address)
      : local_transport_handle_(local_transport_handle), address_(address) {}

  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, uint64_t request_id, Request request, Duration timeout) {
    auto [future, promise] = memgraph::io::FuturePromisePair<ResponseResult<Response>>();

    local_transport_handle_->SubmitRequest(address, address_, request_id, std::move(request), timeout,
                                           std::move(promise));

    return std::move(future);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Duration timeout) {
    return local_transport_handle_->template Receive<Ms...>(address_, timeout);
  }

  template <Message M>
  void Send(Address address, uint64_t request_id, M message) {
    return local_transport_handle_->template Send<M>(address, address_, request_id, message);
  }

  Time Now() const { return local_transport_handle_->Now(); }

  bool ShouldShutDown() const { return false; }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return distrib(rng_);
  }
};
};  // namespace memgraph::io::local_transport
