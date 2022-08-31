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
#include "io/time.hpp"

namespace memgraph::machine_manager {

using boost::uuids::uuid;
using memgraph::coordinator;
using memgraph::io::Duration;

std::variant<std::monostate> CoordinatorMessages;
std::variant<std::monostate> ShardMessages;
std::variant<std::monostate> ShardManagerMessages;
std::variant<std::monostate> MachineManagerMessages;

std::variant<CoordinatorMessages, ShardMessages, ShardManagerMessages, MachineManagerMessages> UberMessage;

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

  void Run() {
    Time next_cron = io_.Now();

    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();

      if (now >= next_cron) {
        Duration next_duration = Cron();
        next_cron = now + next_duration;
      }

      Duration receive_timeout = next_cron - now;

      auto request_result = io_.template ReceiveWithTimeout<UberMessage>(receive_timeout);

      if (request_result.HasError()) {
        // time to do Cron
        continue;
      }

      auto request = std::move(request_result.GetValue());

      Dispatch(std::move(request.message), request.request_id, request.from_address);
    }
  }

 private:
  Duration Cron() { return shard_manager_.Cron(); }
};

}  // namespace memgraph::machine_manager
