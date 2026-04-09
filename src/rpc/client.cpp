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

Client::Client(io::network::Endpoint endpoint, communication::ClientContext *context,
               std::unordered_map<std::string_view, int> const &rpc_timeouts_ms)
    : endpoint_(std::move(endpoint)), context_(context), rpc_timeouts_ms_(rpc_timeouts_ms) {}

void Client::Abort() {
  // Shut down the socket to abort any pending read, write, or connect
  // operations on another thread. Does NOT destroy the client object,
  // so it is safe to call concurrently with an in-flight RPC.
  if (!client_) return;
  client_->Shutdown();
}

void Client::Shutdown() {
  if (!client_) return;
  client_->Shutdown();
  client_ = std::nullopt;
}

}  // namespace memgraph::rpc
