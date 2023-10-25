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

#include "replication/replication_server.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::storage {

constexpr utils::TypeInfo FrequentHeartbeatReq::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ, "FrequentHeartbeatReq",
                                                      nullptr};

constexpr utils::TypeInfo FrequentHeartbeatRes::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_RES, "FrequentHeartbeatRes",
                                                      nullptr};

}  // namespace memgraph::storage

namespace memgraph::slk {

// Serialize code for FrequentHeartbeatRes

void Save(const memgraph::storage::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::storage::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// void FrequentHeartbeatReq::Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder) {
//   memgraph::slk::Save(self, builder);
// }
// void FrequentHeartbeatReq::Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader) {
//   memgraph::slk::Load(self, reader);
// }
// void FrequentHeartbeatRes::Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
//   memgraph::slk::Save(self, builder);
// }
// void FrequentHeartbeatRes::Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
//   memgraph::slk::Load(self, reader);
// }

// Serialize code for FrequentHeartbeatReq

void Save(const memgraph::storage::FrequentHeartbeatReq &self, memgraph::slk::Builder *builder) {}

void Load(memgraph::storage::FrequentHeartbeatReq *self, memgraph::slk::Reader *reader) {}
}  // namespace memgraph::slk

namespace memgraph::storage {
namespace {

auto CreateServerContext(const memgraph::replication::ReplicationServerConfig &config) -> communication::ServerContext {
  return (config.ssl) ? communication::ServerContext{config.ssl->key_file, config.ssl->cert_file, config.ssl->ca_file,
                                                     config.ssl->verify_peer}
                      : communication::ServerContext{};
}

static void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  FrequentHeartbeatReq req;
  memgraph::slk::Load(&req, req_reader);
  FrequentHeartbeatRes res{true};
  memgraph::slk::Save(res, res_builder);
}

// NOTE: The replication server must have a single thread for processing
// because there is no need for more processing threads - each replica can
// have only a single main server. Also, the single-threaded guarantee
// simplifies the rest of the implementation.
constexpr auto kReplictionServerThreads = 1;
}  // namespace

ReplicationServer::ReplicationServer(const memgraph::replication::ReplicationServerConfig &config)
    : rpc_server_context_{CreateServerContext(config)},
      rpc_server_{io::network::Endpoint{config.ip_address, config.port}, &rpc_server_context_,
                  kReplictionServerThreads} {
  rpc_server_.Register<FrequentHeartbeatRpc>([](auto *req_reader, auto *res_builder) {
    spdlog::debug("Received FrequentHeartbeatRpc");
    FrequentHeartbeatHandler(req_reader, res_builder);
  });
}

ReplicationServer::~ReplicationServer() {
  if (rpc_server_.IsRunning()) {
    auto const &endpoint = rpc_server_.endpoint();
    spdlog::trace("Closing replication server on {}:{}", endpoint.address, endpoint.port);
    rpc_server_.Shutdown();
  }
  rpc_server_.AwaitShutdown();
}

bool ReplicationServer::Start() { return rpc_server_.Start(); }

}  // namespace memgraph::storage
