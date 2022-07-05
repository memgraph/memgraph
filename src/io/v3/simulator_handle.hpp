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
#include <vector>

#include "address.hpp"
#include "simulator_stats.hpp"
#include "transport.hpp"

struct OpaqueMessage {
  Address address;
  uint64_t request_id;
  std::any message;
};

struct PromiseKey {
  Address requester;
  uint64_t request_id;
  Address replier;
};

struct OpaquePromise {
  uint64_t deadline;
  std::any promise;
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
  void SubmitRequest(Address to_addr, Address from_addr, uint64_t request_id, Request &&request,
                     uint64_t timeout_microseconds, MgPromise<ResponseResult<Response>> &&promise) {
    std::unique_lock<std::mutex> lock(mu_);

    uint64_t deadline = cluster_wide_time_microseconds_ + timeout_microseconds;

    std::any message(std::move(request));
    OpaqueMessage om{.address = from_addr, .request_id = request_id, .message = std::move(message)};
    in_flight_.emplace_back(std::make_pair(std::move(to_addr), std::move(om)));

    /*
    std::any opaque_promise(std::move(promise));
    PromiseKey pk { .requester=from_addr, .request_id=request_id, .replier=to_addr };
    OpaquePromise op { .deadline=deadline, .promise=std::move(opaque_promise) };
    promises_.insert(std::make_pair(std::move(pk), std::move(op)));
    */

    stats_.total_messages_++;
    stats_.total_requests_++;

    return;
  }

  /*
    template <Message... Ms>
    RequestResult<Ms...> Receive(uint64_t timeout_microseconds) {
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
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;
  std::map<PromiseKey, OpaquePromise> promises_;
  std::map<Address, OpaqueMessage> can_receive_;
  uint64_t cluster_wide_time_microseconds_ = 0;
  bool shut_down_;
  SimulatorStats stats_;
};
