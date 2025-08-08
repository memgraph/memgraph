// Copyright 2025 Memgraph Ltd.
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
#include <mutex>

#include "communication/server.hpp"
#include "io/network/endpoint.hpp"
#include "rpc/messages.hpp"
#include "rpc/protocol.hpp"
#include "slk/streams.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::rpc {

class Server {
 public:
  Server(io::network::Endpoint endpoint, communication::ServerContext *context,
         size_t workers_count = std::thread::hardware_concurrency());
  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  bool Start();
  void Shutdown();
  void AwaitShutdown();
  bool IsRunning();

  const io::network::Endpoint &endpoint() const;

  template <class TRequestResponse>
  void Register(std::function<void(slk::Reader *, slk::Builder *)> callback) {
    auto guard = std::lock_guard{lock_};
    MG_ASSERT(!server_.IsRunning(), "You can't register RPCs when the server is running!");
    RpcCallback rpc{.req_type = TRequestResponse::Request::kType, .callback = std::move(callback)};

    // Here I could retrieve the type of the response needed
    auto got = callbacks_.insert({TRequestResponse::Request::kType.id, std::move(rpc)});
    MG_ASSERT(got.second, "Callback for that message type already registered");
    spdlog::trace("[RpcServer] register {} -> {}", TRequestResponse::Request::kType.name,
                  TRequestResponse::Response::kType.name);
  }

 private:
  friend class RpcMessageDeliverer;

  struct RpcCallback {
    utils::TypeInfo req_type;
    std::function<void(slk::Reader *, slk::Builder *)> callback;
  };

  std::mutex lock_;
  std::map<utils::TypeId, RpcCallback> callbacks_;
  communication::Server<RpcMessageDeliverer, Server> server_;
};

}  // namespace memgraph::rpc
