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

#include "replication/config.hpp"
#include "rpc/server.hpp"
#include "slk/streams.hpp"

namespace memgraph::replication {

struct FrequentHeartbeatReq {
  static const utils::TypeInfo kType;                            // TODO: make constexpr?
  static const utils::TypeInfo &GetTypeInfo() { return kType; }  // WHAT?

  static void Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReq() = default;
};

struct FrequentHeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatRes() = default;
  explicit FrequentHeartbeatRes(bool success) : success(success) {}

  bool success;
};

// TODO: move to own header
using FrequentHeartbeatRpc = rpc::RequestResponse<FrequentHeartbeatReq, FrequentHeartbeatRes>;

class ReplicationServer {
 public:
  explicit ReplicationServer(const memgraph::replication::ReplicationServerConfig &config);
  ReplicationServer(const ReplicationServer &) = delete;
  ReplicationServer(ReplicationServer &&) = delete;
  ReplicationServer &operator=(const ReplicationServer &) = delete;
  ReplicationServer &operator=(ReplicationServer &&) = delete;

  virtual ~ReplicationServer();

  bool Start();

 protected:
  communication::ServerContext rpc_server_context_;

 public:
  rpc::Server rpc_server_;  // TODO: Interface or something
};

}  // namespace memgraph::replication
