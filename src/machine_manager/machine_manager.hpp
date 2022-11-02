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

#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/coordinator_worker.hpp"
#include "io/message_conversion.hpp"
#include "io/messages.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/time.hpp"
#include "machine_manager/machine_config.hpp"
#include "storage/v3/shard_manager.hpp"

namespace memgraph::machine_manager {

using coordinator::Coordinator;
using coordinator::CoordinatorReadRequests;
using coordinator::CoordinatorReadResponses;
using coordinator::CoordinatorRsm;
using coordinator::CoordinatorWriteRequests;
using coordinator::CoordinatorWriteResponses;
using coordinator::coordinator_worker::CoordinatorWorker;
using CoordinatorRouteMessage = coordinator::coordinator_worker::RouteMessage;
using CoordinatorQueue = coordinator::coordinator_worker::Queue;
using io::ConvertVariant;
using io::Duration;
using io::RequestId;
using io::Time;
using io::messages::CoordinatorMessages;
using io::messages::ShardManagerMessages;
using io::messages::ShardMessages;
using io::messages::StorageReadRequest;
using io::messages::StorageWriteRequest;
using io::rsm::AppendRequest;
using io::rsm::AppendResponse;
using io::rsm::ReadRequest;
using io::rsm::VoteRequest;
using io::rsm::VoteResponse;
using io::rsm::WriteRequest;
using io::rsm::WriteResponse;
using storage::v3::ShardManager;

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
  Address coordinator_address_;
  CoordinatorQueue coordinator_queue_;
  std::jthread coordinator_handle_;
  ShardManager<IoImpl> shard_manager_;
  Time next_cron_ = Time::min();

 public:
  // TODO initialize ShardManager with "real" coordinator addresses instead of io.GetAddress
  // which is only true for single-machine config.
  MachineManager(io::Io<IoImpl> io, MachineConfig config, Coordinator coordinator)
      : io_(io),
        config_(config),
        coordinator_address_(io.GetAddress().ForkUniqueAddress()),
        shard_manager_{io.ForkLocal(), config.shard_worker_threads, coordinator_address_} {
    auto coordinator_io = io.ForkLocal();
    coordinator_io.SetAddress(coordinator_address_);
    CoordinatorWorker coordinator_worker{coordinator_io, coordinator_queue_, coordinator};
    coordinator_handle_ = std::jthread([coordinator = std::move(coordinator_worker)]() mutable { coordinator.Run(); });
  }

  MachineManager(MachineManager &&) = default;
  MachineManager &operator=(MachineManager &&) = default;
  MachineManager(const MachineManager &) = delete;
  MachineManager &operator=(const MachineManager &) = delete;

  ~MachineManager() {
    if (coordinator_handle_.joinable()) {
      coordinator_queue_.Push(coordinator::coordinator_worker::ShutDown{});
      std::move(coordinator_handle_).join();
    }
  }

  Address CoordinatorAddress() { return coordinator_address_; }

  void Run() {
    while (!io_.ShouldShutDown()) {
      const auto now = io_.Now();

      if (now >= next_cron_) {
        next_cron_ = Cron();
      }

      Duration receive_timeout = std::max(next_cron_, now) - now;

      // Note: this parameter pack must be kept in-sync with the ReceiveWithTimeout parameter pack below
      using AllMessages =
          std::variant<ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>, AppendResponse,
                       WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse,
                       WriteResponse<CoordinatorWriteResponses>, ReadRequest<StorageReadRequest>,
                       AppendRequest<StorageWriteRequest>, WriteRequest<StorageWriteRequest>>;

      spdlog::info("MM waiting on Receive on address {}", io_.GetAddress().ToString());

      // Note: this parameter pack must be kept in-sync with the AllMessages parameter pack above
      auto request_result = io_.template ReceiveWithTimeout<
          ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>, AppendResponse,
          WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse, WriteResponse<CoordinatorWriteResponses>,
          ReadRequest<StorageReadRequest>, AppendRequest<StorageWriteRequest>, WriteRequest<StorageWriteRequest>>(
          receive_timeout);

      if (request_result.HasError()) {
        // time to do Cron
        continue;
      }

      auto &&request_envelope = std::move(request_result.GetValue());

      spdlog::info("MM got message to {}", request_envelope.to_address.ToString());

      // If message is for the coordinator, cast it to subset and pass it to the coordinator
      bool to_coordinator = coordinator_address_ == request_envelope.to_address;
      if (to_coordinator) {
        std::optional<CoordinatorMessages> conversion_attempt =
            ConvertVariant<AllMessages, ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>,
                           AppendResponse, WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse>(
                std::move(request_envelope.message));

        MG_ASSERT(conversion_attempt.has_value(), "coordinator message conversion failed");

        spdlog::info("got coordinator message");

        CoordinatorMessages &&cm = std::move(conversion_attempt.value());

        CoordinatorRouteMessage route_message{
            .message = std::forward<CoordinatorMessages>(cm),
            .request_id = request_envelope.request_id,
            .to = request_envelope.to_address,
            .from = request_envelope.from_address,
        };
        coordinator_queue_.Push(std::move(route_message));
        continue;
      }

      bool to_sm = shard_manager_.GetAddress() == request_envelope.to_address;
      spdlog::info("smm: {}", shard_manager_.GetAddress().ToString());
      if (to_sm) {
        std::optional<ShardManagerMessages> conversion_attempt =
            ConvertVariant<AllMessages, WriteResponse<CoordinatorWriteResponses>>(std::move(request_envelope.message));

        MG_ASSERT(conversion_attempt.has_value(), "shard manager message conversion failed");

        spdlog::info("got shard manager message");

        ShardManagerMessages &&smm = std::move(conversion_attempt.value());
        shard_manager_.Receive(std::forward<ShardManagerMessages>(smm), request_envelope.request_id,
                               request_envelope.from_address);
        continue;
      }

      // treat this as a message to a specific shard rsm and cast it accordingly

      std::optional<ShardMessages> conversion_attempt =
          ConvertVariant<AllMessages, ReadRequest<StorageReadRequest>, AppendRequest<StorageWriteRequest>,
                         AppendResponse, WriteRequest<StorageWriteRequest>, VoteRequest, VoteResponse>(
              std::move(request_envelope.message));

      MG_ASSERT(conversion_attempt.has_value(), "shard rsm message conversion failed for {} - incorrect message type",
                request_envelope.to_address.ToString());

      spdlog::info("got shard rsm message");

      ShardMessages &&sm = std::move(conversion_attempt.value());
      shard_manager_.Route(std::forward<ShardMessages>(sm), request_envelope.request_id, request_envelope.to_address,
                           request_envelope.from_address);
    }
  }

 private:
  Time Cron() {
    spdlog::info("running MachineManager::Cron, address {}", io_.GetAddress().ToString());
    coordinator_queue_.Push(coordinator::coordinator_worker::Cron{});
    return shard_manager_.Cron();
  }
};

}  // namespace memgraph::machine_manager
