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
#include <io/message_conversion.hpp>
#include <io/messages.hpp>
#include <io/rsm/rsm_client.hpp>
#include <io/time.hpp>
#include <machine_manager/machine_config.hpp>
#include <storage/v3/shard_manager.hpp>

namespace memgraph::machine_manager {

using boost::uuids::uuid;

using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorReadRequests;
using memgraph::coordinator::CoordinatorReadResponses;
using memgraph::coordinator::CoordinatorRsm;
using memgraph::coordinator::CoordinatorWriteRequests;
using memgraph::coordinator::CoordinatorWriteResponses;
using memgraph::io::ConvertVariant;
using memgraph::io::Duration;
using memgraph::io::RequestEnvelope;
using memgraph::io::RequestId;
using memgraph::io::Time;
using memgraph::io::messages::CoordinatorMessages;
using memgraph::io::messages::QueryEngineMessages;
using memgraph::io::messages::ShardManagerMessages;
using memgraph::io::messages::ShardMessages;
using memgraph::io::rsm::AppendRequest;
using memgraph::io::rsm::AppendResponse;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::VoteRequest;
using memgraph::io::rsm::VoteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
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
  CoordinatorRsm<IoImpl> coordinator_;
  ShardManager<IoImpl> shard_manager_;
  Time next_cron_;

 public:
  // TODO initialize ShardManager with "real" coordinator addresses instead of io.GetAddress
  // which is only true for single-machine config.
  MachineManager(io::Io<IoImpl> io, MachineConfig config, Coordinator coordinator)
      : io_(io),
        config_(config),
        coordinator_{std::move(io.ForkLocal()), config.coordinator_addresses, std::move(coordinator)},
        shard_manager_(ShardManager{io.ForkLocal(), io.GetAddress()}) {}

  void Run() {
    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();

      if (now >= next_cron_) {
        next_cron_ = Cron();
      }

      Duration receive_timeout = next_cron_ - now;

      using AllMessages =
          std::variant<ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>, AppendResponse,
                       WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse>;

      spdlog::info("MM waiting on Receive");
      auto request_result =
          io_.template ReceiveWithTimeout<ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>,
                                          AppendResponse, WriteRequest<CoordinatorWriteRequests>, VoteRequest,
                                          VoteResponse>(receive_timeout);

      if (request_result.HasError()) {
        // time to do Cron
        spdlog::info("MM got timeout");
        continue;
      }

      spdlog::info("MM got message");

      auto &&request_envelope = std::move(request_result.GetValue());

      // If message is for the coordinator, cast it to subset and pass it to the coordinator
      bool to_coordinator = true;
      if (to_coordinator) {
        std::optional<CoordinatorMessages> conversion_attempt =
            ConvertVariant<AllMessages, ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>,
                           AppendResponse, WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse>(
                std::move(request_envelope.message));

        if (!conversion_attempt.has_value()) {
          spdlog::error("message conversion failed");
          continue;
        }

        CoordinatorMessages &&cm = std::move(conversion_attempt.value());

        coordinator_.Handle(std::forward<CoordinatorMessages>(cm), request_envelope.request_id,
                            request_envelope.from_address);
      }

      // Otherwise pass message to ShardManager for processing
      /*
      &&um = std::move(std::get<UberMessage>(request.message));

      std::visit(
          [&](auto &&msg) {
            Handle(std::forward<decltype(msg)>(msg), request.request_id, request.from_address, request.to_address);
          },
          std::forward<UberMessage>(um));
      */
    }
  }

 private:
  Time Cron() {
    spdlog::info("running MachineManager::Cron, address {}", io_.GetAddress().ToString());
    return shard_manager_.Cron();
  }

  void Handle(QueryEngineMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(ShardManagerMessages &&smm, RequestId request_id, Address from, Address to) {
    spdlog::info("got SMM message");
    shard_manager_.Handle(from, to, request_id, std::forward<ShardManagerMessages>(smm));
  }
  void Handle(ShardMessages &&, RequestId request_id, Address from, Address to) {}
  void Handle(CoordinatorMessages &&, RequestId request_id, Address from, Address to) {
    spdlog::info("got coordinator message");
    // coordinator_.
  }
};

}  // namespace memgraph::machine_manager
