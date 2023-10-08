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

#include "rpc/server.hpp"

namespace memgraph::rpc {

Server::Server(io::network::Endpoint endpoint, communication::ServerContext *context, size_t workers_count)
    : server_(std::move(endpoint), this, context, -1, context->use_ssl() ? "RPCS" : "RPC", workers_count) {}

bool Server::Start() { return server_.Start(); }

void Server::Shutdown() { server_.Shutdown(); }

void Server::AwaitShutdown() { server_.AwaitShutdown(); }

bool Server::IsRunning() { return server_.IsRunning(); }

const io::network::Endpoint &Server::endpoint() const { return server_.endpoint(); }
}  // namespace memgraph::rpc
