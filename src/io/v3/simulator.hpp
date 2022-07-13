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

#include <variant>

#include "address.hpp"
#include "errors.hpp"
#include "future.hpp"
#include "simulator_config.hpp"
#include "simulator_handle.hpp"
#include "transport.hpp"

class SimulatorTransport {
 public:
  SimulatorTransport(std::shared_ptr<SimulatorHandle> simulator_handle, Address address)
      : simulator_handle_(simulator_handle), address_(address) {}

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
    return simulator_handle_->Rand<D, Return>(distrib);
  }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
  Address address_;
};

class Simulator {
 public:
  Simulator(SimulatorConfig config) { simulator_handle_->SetConfig(config); }

  void ShutDown() { simulator_handle_->ShutDown(); }

  Io<SimulatorTransport> Register(Address address, bool is_server) {
    if (is_server) {
      simulator_handle_->IncrementServerCount(address);
    }
    return Io(SimulatorTransport(simulator_handle_, address), address);
  }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_{std::make_shared<SimulatorHandle>()};
};
