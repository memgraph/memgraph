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

#include <memory>
#include <utility>

#include "io/v3/address.hpp"
#include "io/v3/simulator_handle.hpp"

class SimulatorTransport {
  std::shared_ptr<SimulatorHandle> simulator_handle_;
  Address address_;
  std::mt19937 rng_{};

 public:
  SimulatorTransport(std::shared_ptr<SimulatorHandle> simulator_handle, Address address, uint64_t seed)
      : simulator_handle_(simulator_handle), address_(address), rng_(std::mt19937{seed}) {}

  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, uint64_t request_id, Request request,
                                   uint64_t timeout_microseconds) {
    std::function<bool()> maybe_tick_simulator = [=] { return simulator_handle_->MaybeTickSimulator(); };
    auto [future, promise] = FuturePromisePairWithNotifier<ResponseResult<Response>>(maybe_tick_simulator);

    simulator_handle_->SubmitRequest(address, address_, request_id, std::move(request), timeout_microseconds,
                                     std::move(promise));

    return std::move(future);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(uint64_t timeout_microseconds) {
    return simulator_handle_->template Receive<Ms...>(address_, timeout_microseconds);
  }

  template <Message M>
  void Send(Address address, uint64_t request_id, M message) {
    return simulator_handle_->template Send<M>(address, address_, request_id, message);
  }

  uint64_t Now() { return simulator_handle_->Now(); }

  bool ShouldShutDown() { return simulator_handle_->ShouldShutDown(); }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return distrib(rng_);
  }
};
