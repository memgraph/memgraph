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

#include <map>
#include <mutex>
#include <vector>

#include "communication/server.hpp"
#include "io/network/endpoint.hpp"
#include "rpc/messages.hpp"
#include "rpc/protocol.hpp"
#include "slk/streams.hpp"

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
    std::lock_guard<std::mutex> guard(lock_);
    MG_ASSERT(!server_.IsRunning(), "You can't register RPCs when the server is running!");
    RpcCallback rpc;
    rpc.req_type = TRequestResponse::Request::kType;
    rpc.res_type = TRequestResponse::Response::kType;
    rpc.callback = callback;

    if (extended_callbacks_.find(TRequestResponse::Request::kType.id) != extended_callbacks_.end()) {
      LOG_FATAL("Callback for that message type already registered!");
    }

    auto got = callbacks_.insert({TRequestResponse::Request::kType.id, rpc});
    MG_ASSERT(got.second, "Callback for that message type already registered");
    SPDLOG_TRACE("[RpcServer] register {} -> {}", rpc.req_type.name, rpc.res_type.name);
  }

  template <class TRequestResponse>
  void Register(std::function<void(const io::network::Endpoint &, slk::Reader *, slk::Builder *)> callback) {
    std::lock_guard<std::mutex> guard(lock_);
    MG_ASSERT(!server_.IsRunning(), "You can't register RPCs when the server is running!");
    RpcExtendedCallback rpc;
    rpc.req_type = TRequestResponse::Request::kType;
    //    rpc.req_ver = TRequestResponse::Request::kVer;
    rpc.res_type = TRequestResponse::Response::kType;
    //    rpc.res_ver = TRequestResponse::Response::kVer;
    rpc.callback = callback;

    auto got = extended_callbacks_.insert({TRequestResponse::Request::kType.id, rpc});
    MG_ASSERT(got.second, "Callback for that message type already registered");
    SPDLOG_TRACE("[RpcServer] register {} -> {}", rpc.req_type.name, rpc.res_type.name);
  }

 private:
  friend class Session;

  struct RpcCallback {
    utils::TypeInfo req_type;
    std::function<void(slk::Reader *, slk::Builder *)> callback;
    utils::TypeInfo res_type;
  };

  struct RpcExtendedCallback {
    utils::TypeInfo req_type;
    std::function<void(const io::network::Endpoint &, slk::Reader *, slk::Builder *)> callback;
    utils::TypeInfo res_type;
  };

  std::mutex lock_;
  std::map<utils::TypeId, RpcCallback> callbacks_;
  std::map<utils::TypeId, RpcExtendedCallback> extended_callbacks_;

  communication::Server<Session, Server> server_;
};

}  // namespace memgraph::rpc
