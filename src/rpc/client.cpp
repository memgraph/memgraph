// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "rpc/client.hpp"

namespace memgraph::rpc {

Connection::Connection(io::network::Endpoint endpoint, communication::ClientContext *context,
                       std::chrono::milliseconds const connect_timeout_ms)
    : endpoint_(std::move(endpoint)), context_(context), connect_timeout_ms_(connect_timeout_ms) {}

void Connection::Interrupt() {
  if (!client_) return;
  client_->Shutdown();
}

void Connection::Abort() {
  // Latch first so any thread that subsequently tries to open a stream fails fast instead of reconnecting, then break
  // the in-flight RPC. Nothing is destroyed, so this is safe with an RPC in flight.
  aborted_.store(true, std::memory_order_release);
  Interrupt();
}

void Connection::Shutdown() {
  // Retire the socket rather than destroy it: the in-flight RPC owns mutex_ and has to be the one that finishes with
  // it. EnsureConnected replaces it on the next stream, under that same lock.
  needs_reconnect_.store(true, std::memory_order_release);
  Interrupt();
}

void Connection::EnsureConnected() {
  // Retired by Shutdown, or broken while idle (a long-unused connection whose server has since died). IsConnected
  // gates ErrorStatus because the latter is a getsockopt on the raw descriptor and asserts on a closed socket.
  if (needs_reconnect_.exchange(false, std::memory_order_acq_rel) ||
      (client_ && (!client_->IsConnected() || client_->ErrorStatus()))) {
    client_ = std::nullopt;
  }

  if (client_) return;

  client_.emplace(context_, connect_timeout_ms_);
  if (!client_->Connect(endpoint_)) {
    spdlog::error("Couldn't connect to remote address {}", endpoint_.SocketAddress());
    client_ = std::nullopt;
    throw RpcFailedToConnectException();
  }
}

Client::Client(io::network::Endpoint endpoint, communication::ClientContext *context,
               std::unordered_map<std::string_view, int> const &rpc_timeouts_ms,
               std::chrono::milliseconds const connect_timeout_ms)
    : conn_(std::make_shared<Connection>(std::move(endpoint), context, connect_timeout_ms)),
      rpc_timeouts_ms_(rpc_timeouts_ms) {}

void Client::Abort() { conn_->Abort(); }

void Client::Shutdown() { conn_->Shutdown(); }

}  // namespace memgraph::rpc
