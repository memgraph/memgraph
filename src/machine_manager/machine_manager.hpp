// Copyright 2022 Memgraph Ltd.
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

#include <boost/uuid/uuid.hpp>
#include "io/rsm/coordinator_rsm.hpp"
#include "io/rsm/rsm_client.hpp"

namespace memgraph::machine_manager {

using boost::uuids::uuid;
using memgraph::coordinator;
using memgraph::io::rsm::RsmClient;

/// The MachineManager is responsible for:
/// * starting the entire system and ensuring that high-level
///   operational requirements continue to be met
/// * acting as a machine's single caller of Io::Receive
/// * routing incoming messages from the Io interface to the
///   appropriate Coordinator or to the StorageManager
///   (it's not necessary to route anything in this layer
///   to the query engine because the query engine only
///   communicates using higher-level Futures that will
///   be filled immediately when the response comes in
///   at the lower-level transport layer.
///
/// Every storage engine has exactly one RsmEngine.
template <typename IoImpl>
class MachineManager {
  Io<IoImpl> io_;
  Coordinator coordinator_;
  ShardManager shard_manager_;

 public:
  MachineManager(Io<IoImpl> io, MachineConfig config) : io_(io) {
    for (const auto &initial_shard : config.initial_shards) {
      // TODO(tyler) initialize shard
    }
  }

 private:
  void Run() {
    Time last_cron = io_.Now();

    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();

      /*
      auto request_result =
          io_.template ReceiveWithTimeout<ReadRequest<ReadOperation>, AppendRequest<WriteOperation>, AppendResponse,
                                          WriteRequest<WriteOperation>, VoteRequest, VoteResponse>(receive_timeout);
      if (request_result.HasError()) {
        continue;
      }

      auto request = std::move(request_result.GetValue());

      Handle(std::move(request.message), request.request_id, request.from_address);
      */
    }
  }
};

}  // namespace memgraph::machine_manager
