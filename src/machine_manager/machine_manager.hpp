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

#include <coordinator/coordinator_rsm.hpp>
#include <io/messages.hpp>
#include <io/rsm/rsm_client.hpp>
#include <io/time.hpp>
#include <machine_manager/machine_config.hpp>
#include <storage/v3/shard_manager.hpp>

namespace memgraph::machine_manager {

using boost::uuids::uuid;

using memgraph::coordinator::Coordinator;
using memgraph::io::Duration;
using memgraph::io::RequestEnvelope;
using memgraph::io::RequestId;
using memgraph::io::Time;
using memgraph::io::messages::CoordinatorMessages;
using memgraph::io::messages::MachineManagerMessages;
using memgraph::io::messages::QueryEngineMessages;
using memgraph::io::messages::ShardManagerMessages;
using memgraph::io::messages::ShardMessages;
using memgraph::io::messages::UberMessage;
using memgraph::storage::v3::ShardManager;

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
  io::Io<IoImpl> io_;
  MachineConfig config_;
  Coordinator coordinator_;
  ShardManager<IoImpl> shard_manager_;
  Time next_cron_;

 public:
  MachineManager(io::Io<IoImpl> io, MachineConfig config, Coordinator coordinator)
      : io_(io), config_(config), coordinator_(coordinator), shard_manager_(ShardManager{io}) {
    for (const auto &initial_label_space : config.initial_label_spaces) {
      // TODO(tyler) initialize shard
    }
  }

  void Run() {
    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();

      if (now >= next_cron_) {
        next_cron_ = Cron();
      }

      Duration receive_timeout = next_cron_ - now;

      auto request_result = io_.template ReceiveWithTimeout<UberMessage>(receive_timeout);

      if (request_result.HasError()) {
        // time to do Cron
        continue;
      }

      RequestEnvelope<UberMessage> request = std::move(request_result.GetValue());
      UberMessage um = std::get<UberMessage>(request.message);

      std::visit(
          [&](auto &&msg) {
            Handle(std::forward<decltype(msg)>(msg), request.request_id, request.from_address, request.to_address);
          },
          std::move(um));
    }
  }

 private:
  Time Cron() { return shard_manager_.Cron(); }

  void Handle(QueryEngineMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(MachineManagerMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(ShardManagerMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(ShardMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(CoordinatorMessages &&, RequestId request_id, Address from, Address to) {}
};

}  // namespace memgraph::machine_manager
