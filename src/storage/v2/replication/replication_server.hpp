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
#include "storage/v2/replication/global.hpp"

namespace memgraph::storage {

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
  static void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  communication::ServerContext rpc_server_context_;
  rpc::Server rpc_server_;
};

}  // namespace memgraph::storage
