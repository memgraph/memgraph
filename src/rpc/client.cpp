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
  // The load pins the socket, so a concurrent reconnect cannot destroy it while we shut it down. That it
  // is the *right* socket comes from mutex_ rather than from the pin: a thread blocked in an RPC holds
  // mutex_, so EnsureConnected cannot run and client_ cannot change under it. Once that thread has
  // released the lock a reconnect may intervene and this interrupts the newer socket instead, which is
  // harmless -- nobody is blocked on the older one by then.
  if (auto const sock = client_.load(std::memory_order_acquire)) sock->Shutdown();
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

auto Connection::EnsureConnected() -> std::shared_ptr<communication::Client> {
  auto sock = client_.load(std::memory_order_acquire);

  // Retired by Shutdown, or broken while idle (a long-unused connection whose server has since died). IsConnected
  // gates ErrorStatus because the latter is a getsockopt on the raw descriptor and asserts on a closed socket.
  if (needs_reconnect_.exchange(false, std::memory_order_acq_rel) ||
      (sock && (!sock->IsConnected() || sock->ErrorStatus()))) {
    sock.reset();
  }

  if (sock) return sock;

  sock = std::make_shared<communication::Client>(context_, connect_timeout_ms_);
  if (!sock->Connect(endpoint_)) {
    spdlog::error("Couldn't connect to remote address {}", endpoint_.SocketAddress());
    // Retire the previous socket too: whatever it was, it is not what the next attempt should reuse.
    client_.store(nullptr, std::memory_order_release);
    throw RpcFailedToConnectException();
  }

  // Publishing drops the caller's share of the old socket, but not any share an in-flight RPC or a
  // concurrent Interrupt still holds -- those keep it alive until they are done with it.
  client_.store(sock, std::memory_order_release);
  return sock;
}

Client::Client(io::network::Endpoint endpoint, communication::ClientContext *context,
               std::unordered_map<std::string_view, int> const &rpc_timeouts_ms,
               std::chrono::milliseconds const connect_timeout_ms)
    : conn_(std::make_shared<Connection>(std::move(endpoint), context, connect_timeout_ms)),
      rpc_timeouts_ms_(rpc_timeouts_ms) {}

void Client::Abort() { conn_->Abort(); }

void Client::Shutdown() { conn_->Shutdown(); }

}  // namespace memgraph::rpc
