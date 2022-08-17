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
#include "io/thrift/thrift_handle.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::thrift {

using memgraph::io::Duration;
using memgraph::io::Time;

class ThriftTransport {
  std::shared_ptr<ThriftHandle> simulator_handle_;
  const Address address_;
  std::random_device rng_;

 public:
  ThriftTransport(std::shared_ptr<ThriftHandle> simulator_handle, Address address)
      : simulator_handle_(simulator_handle), address_(address) {}

  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, uint64_t request_id, Request request, Duration timeout) {
    auto [future, promise] = memgraph::io::FuturePromisePairWithNotifier<ResponseResult<Response>>();

    simulator_handle_->SubmitRequest(address, address_, request_id, std::move(request), timeout, std::move(promise));

    return std::move(future);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Duration timeout) {
    return simulator_handle_->template Receive<Ms...>(address_, timeout);
  }

  template <Message M>
  void Send(Address address, uint64_t request_id, M message) {
    return simulator_handle_->template Send<M>(address, address_, request_id, message);
  }

  Time Now() const {
    auto nano_time = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::milliseconds>(nano_time);
    return Time::now();
  }

  bool ShouldShutDown() const { return false; }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return distrib(rng_);
  }
};
};  // namespace memgraph::io::thrift
