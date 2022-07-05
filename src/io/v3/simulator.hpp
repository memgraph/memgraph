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
#include "simulator_handle.hpp"
#include "transport.hpp"

struct SimulatorConfig {
  uint8_t drop_percent_;
  uint64_t rng_seed_;
};

class SimulatorTransport {
 public:
  SimulatorTransport(std::shared_ptr<SimulatorHandle> simulator_handle, Address address)
      : simulator_handle_(simulator_handle), address_(address) {}

  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, uint64_t request_id, Request request,
                                   uint64_t timeout_microseconds) {
    std::function<void()> notifier = [=] { simulator_handle_->NotifySimulator(); };
    auto [future, promise] = FuturePromisePairWithNotifier<ResponseResult<Response>>(notifier);

    simulator_handle_->SubmitRequest(address, address_, request_id, std::move(request), timeout_microseconds,
                                     std::move(promise));

    return std::move(future);
  }

  /*
    template <Message... Ms>
    RequestResult<Ms...> Receive(uint64_t timeout_microseconds) {
      return simulator_handle_->template Receive<Ms...>(timeout_microseconds);
    }

    template <Message M>
    void Send(Address address, uint64_t request_id, M message) {
      return simulator_handle_->template Send<M>(address, request_id, message);
    }
    */

  std::time_t Now() { return std::time(nullptr); }

  bool ShouldShutDown() { return simulator_handle_->ShouldShutDown(); }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
  Address address_;
};

class Simulator {
 public:
  Io<SimulatorTransport> Register(Address address, bool is_server) {
    return Io(SimulatorTransport(simulator_handle_, address), address);
  }

 private:
  std::shared_ptr<SimulatorHandle> simulator_handle_;
};
