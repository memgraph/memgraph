// Copyright 2023 Memgraph Ltd.
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

#include <mutex>
#include <stack>
#include <utility>

#include "rpc/client.hpp"

namespace memgraph::rpc {

/**
 * A simple client pool that creates new RPC clients on demand. Useful when you
 * want to send RPCs to the same server from multiple threads without them
 * blocking each other.
 */
class ClientPool {
 public:
  ClientPool(io::network::Endpoint endpoint, communication::ClientContext *context)
      : endpoint_(std::move(endpoint)), context_(context) {}

  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response Call(Args &&...args) {
    return WithUnusedClient(
        [&](const auto &client) { return client->template Call<TRequestResponse>(std::forward<Args>(args)...); });
  }

  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response CallWithLoad(Args &&...args) {
    return WithUnusedClient([&](const auto &client) {
      return client->template CallWithLoad<TRequestResponse>(std::forward<Args>(args)...);
    });
  }

 private:
  template <class TFun>
  auto WithUnusedClient(const TFun &fun) {
    std::unique_ptr<Client> client;

    std::unique_lock<std::mutex> lock(mutex_);
    if (unused_clients_.empty()) {
      client = std::make_unique<Client>(endpoint_, context_);
    } else {
      client = std::move(unused_clients_.top());
      unused_clients_.pop();
    }
    lock.unlock();

    auto res = fun(client);

    lock.lock();
    unused_clients_.push(std::move(client));
    return res;
  }

  io::network::Endpoint endpoint_;
  communication::ClientContext *context_;

  std::mutex mutex_;
  std::stack<std::unique_ptr<Client>> unused_clients_;
};

}  // namespace memgraph::rpc
