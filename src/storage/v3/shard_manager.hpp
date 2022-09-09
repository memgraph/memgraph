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

#include <queue>

#include <boost/uuid/uuid.hpp>

#include <coordinator/coordinator.hpp>
#include <io/address.hpp>
#include <io/messages.hpp>
#include <io/rsm/raft.hpp>
#include <io/rsm/shard_rsm.hpp>
#include <io/time.hpp>
#include <io/transport.hpp>

namespace memgraph::storage::v3 {

using boost::uuids::uuid;

using memgraph::coordinator::CoordinatorWriteRequests;
using memgraph::coordinator::CoordinatorWriteResponses;
using memgraph::coordinator::HeartbeatRequest;
using memgraph::coordinator::HeartbeatResponse;
using memgraph::io::Address;
using memgraph::io::Duration;
using memgraph::io::Message;
using memgraph::io::RequestId;
using memgraph::io::ResponseFuture;
using memgraph::io::Time;
using memgraph::io::messages::CoordinatorMessages;
using memgraph::io::messages::ShardManagerMessages;
using memgraph::io::messages::ShardMessages;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::ShardRsm;
using memgraph::io::rsm::StorageReadRequest;
using memgraph::io::rsm::StorageReadResponse;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;

using ShardManagerOrRsmMessage = std::variant<ShardMessages, ShardManagerMessages>;
using TimeUuidPair = std::pair<Time, uuid>;

template <typename IoImpl>
using ShardRaft =
    Raft<IoImpl, ShardRsm, StorageWriteRequest, StorageWriteResponse, StorageReadRequest, StorageReadResponse>;

using namespace std::chrono_literals;
static constexpr Duration kMinimumCronInterval = 1000ms;
static constexpr Duration kMaximumCronInterval = 2000ms;
static_assert(kMinimumCronInterval < kMaximumCronInterval,
              "The minimum cron interval has to be smaller than the maximum cron interval!");

/// The ShardManager is responsible for:
/// * reconciling the storage engine's local configuration with the Coordinator's
///   intentions for how it should participate in multiple raft clusters
/// * replying to heartbeat requests to the Coordinator
/// * routing incoming messages to the appropriate sRSM
///
/// Every storage engine has exactly one RsmEngine.
template <typename IoImpl>
class ShardManager {
 public:
  ShardManager(io::Io<IoImpl> io, Address coordinator_leader) : io_(io), coordinator_leader_(coordinator_leader) {}

  /// Periodic protocol maintenance. Returns the time that Cron should be called again
  /// in the future.
  Time Cron() {
    spdlog::info("running ShardManager::Cron, address {}", io_.GetAddress().ToString());
    Time now = io_.Now();

    if (now >= next_cron_) {
      Reconciliation();

      std::uniform_int_distribution time_distrib(kMinimumCronInterval.count(), kMaximumCronInterval.count());

      const auto rand = io_.Rand(time_distrib);

      next_cron_ = now + Duration{rand};
    }

    if (!cron_schedule_.empty()) {
      auto &[time, uuid] = cron_schedule_.top();

      if (time <= now) {
        auto &rsm = rsm_map_.at(uuid);
        Time next_for_uuid = rsm.Cron();

        cron_schedule_.pop();
        cron_schedule_.push(std::make_pair(next_for_uuid, uuid));

        auto &[next_time, _uuid] = cron_schedule_.top();

        return std::min(next_cron_, next_time);
      }
    }

    return next_cron_;
  }

  void Handle(Address from, Address to, RequestId request_id, ShardManagerOrRsmMessage message) {
    Address address = io_.GetAddress();

    MG_ASSERT(address.last_known_port == to.last_known_port);
    MG_ASSERT(address.last_known_ip == to.last_known_ip);

    std::visit([&](auto &&msg) { Handle(from, to, request_id, std::forward<decltype(msg)>(msg)); }, std::move(message));
  }
  void Handle(Address from, Address to, RequestId request_id, ShardManagerMessages &&message) {}

  void Handle(Address from, Address to, RequestId request_id, ShardMessages &&message) {
    auto &rsm = rsm_map_.at(to.unique_id);
    // TODO(tyler) call rsm's Raft::Handle method with message
  }

 private:
  io::Io<IoImpl> io_;
  std::map<uuid, ShardRaft<IoImpl>> rsm_map_;
  std::priority_queue<std::pair<Time, uuid>, std::vector<std::pair<Time, uuid>>, std::greater<std::pair<Time, uuid>>>
      cron_schedule_;
  Time next_cron_;
  Address coordinator_leader_;
  std::optional<ResponseFuture<ShardManagerMessages>> heartbeat_res_;

  void Reconciliation() {
    if (heartbeat_res_.has_value()) {
      if (heartbeat_res_->IsReady()) {
        io::ResponseResult<ShardManagerMessages> response_result = std::move(heartbeat_res_).value().Wait();
        heartbeat_res_.reset();
      } else {
        return;
      }
    }

    HeartbeatRequest req{};
    CoordinatorWriteRequests cwr = req;
    WriteRequest<CoordinatorWriteRequests> ww;
    ww.operation = cwr;

    spdlog::info("SM sending heartbeat");
    heartbeat_res_.emplace(std::move(
        io_.template Request<WriteRequest<CoordinatorWriteRequests>, ShardManagerMessages>(coordinator_leader_, ww)));
    spdlog::info("SM sent heartbeat");
  }
};

}  // namespace memgraph::storage::v3
