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

#include <map>

#include "address.hpp"
#include "transport.hpp"

struct OpaqueMessage {
  Address address;
  uint64_t request_id;
  std::unique_ptr<std::any> message;
};

struct PromiseKey {
  Address requester;
  uint64_t request_id;
  Address replier;
};

struct OpaquePromise {
  time_t deadline;
  std::unique_ptr<std::any> promise;
};

class SimulatorHandle {
 public:
  void NotifySimulator() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_sim_.notify_all();
  }

  bool ShouldShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    return shut_down_;
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address address, uint64_t request_id, Request request, uint64_t timeout_microseconds,
                     MgPromise<ResponseResult<Response>> promise) {
    std::unique_lock<std::mutex> lock(mu_);
  }

  /*
    template <Message... Ms>
    RequestResult<Ms...> ReceiveTimeout(uint64_t timeout_microseconds) {
      std::abort();
    }

    template <Message M>
    void Send(Address address, uint64_t request_id, M message) {
      std::abort();
    }
    */

 private:
  std::mutex mu_;
  std::condition_variable cv_sim_;
  std::condition_variable cv_srv_;
  std::map<Address, std::vector<OpaqueMessage>> in_flight_;
  std::map<PromiseKey, OpaquePromise> promises;
  std::map<Address, OpaqueMessage> can_receive;
  bool shut_down_;
};
