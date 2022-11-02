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
#include <set>

#include <boost/uuid/uuid.hpp>

#include <coordinator/coordinator.hpp>
#include <io/address.hpp>
#include <io/message_conversion.hpp>
#include <io/messages.hpp>
#include <io/rsm/raft.hpp>
#include <io/time.hpp>
#include <io/transport.hpp>
#include <query/v2/requests.hpp>
#include <storage/v3/shard.hpp>
#include <storage/v3/shard_rsm.hpp>
#include "coordinator/shard_map.hpp"
#include "storage/v3/config.hpp"

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
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::msgs::ReadRequests;
using memgraph::msgs::ReadResponses;
using memgraph::msgs::WriteRequests;
using memgraph::msgs::WriteResponses;
using memgraph::storage::v3::ShardRsm;

using ShardManagerOrRsmMessage = std::variant<ShardMessages, ShardManagerMessages>;
using TimeUuidPair = std::pair<Time, uuid>;

template <typename IoImpl>
using ShardRaft = Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

using namespace std::chrono_literals;
static constexpr Duration kMinimumCronInterval = 100ms;
static constexpr Duration kMaximumCronInterval = 200ms;
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
      const auto &[time, uuid] = cron_schedule_.top();

      if (time <= now) {
        auto &rsm = rsm_map_.at(uuid);
        Time next_for_uuid = rsm.Cron();

        cron_schedule_.pop();
        cron_schedule_.push(std::make_pair(next_for_uuid, uuid));

        const auto &[next_time, _uuid] = cron_schedule_.top();

        return std::min(next_cron_, next_time);
      }
    }

    return next_cron_;
  }

  /// Returns the Address for our underlying Io implementation
  Address GetAddress() { return io_.GetAddress(); }

  void Receive(ShardManagerMessages &&smm, RequestId request_id, Address from) {}

  void Route(ShardMessages &&sm, RequestId request_id, Address to, Address from) {
    Address address = io_.GetAddress();

    MG_ASSERT(address.last_known_port == to.last_known_port);
    MG_ASSERT(address.last_known_ip == to.last_known_ip);

    auto &rsm = rsm_map_.at(to.unique_id);

    rsm.Handle(std::forward<ShardMessages>(sm), request_id, from);
  }

 private:
  io::Io<IoImpl> io_;
  std::map<uuid, ShardRaft<IoImpl>> rsm_map_;
  std::priority_queue<std::pair<Time, uuid>, std::vector<std::pair<Time, uuid>>, std::greater<>> cron_schedule_;
  Time next_cron_ = Time::min();
  Address coordinator_leader_;
  std::optional<ResponseFuture<WriteResponse<CoordinatorWriteResponses>>> heartbeat_res_;

  // TODO(tyler) over time remove items from initialized_but_not_confirmed_rsm_
  // after the Coordinator is clearly aware of them
  std::set<boost::uuids::uuid> initialized_but_not_confirmed_rsm_;

  void Reconciliation() {
    if (heartbeat_res_.has_value()) {
      if (heartbeat_res_->IsReady()) {
        io::ResponseResult<WriteResponse<CoordinatorWriteResponses>> response_result =
            std::move(heartbeat_res_).value().Wait();
        heartbeat_res_.reset();

        if (response_result.HasError()) {
          spdlog::error("SM timed out while trying to reach C");
        } else {
          auto response_envelope = response_result.GetValue();
          WriteResponse<CoordinatorWriteResponses> wr = response_envelope.message;

          if (wr.retry_leader.has_value()) {
            spdlog::info("SM redirected to new C leader");
            coordinator_leader_ = wr.retry_leader.value();
          } else if (wr.success) {
            CoordinatorWriteResponses cwr = wr.write_return;
            HeartbeatResponse hr = std::get<HeartbeatResponse>(cwr);
            spdlog::info("SM received heartbeat response from C");

            EnsureShardsInitialized(hr);
          }
        }
      } else {
        return;
      }
    }

    HeartbeatRequest req{
        .from_storage_manager = GetAddress(),
        .initialized_rsms = initialized_but_not_confirmed_rsm_,
    };

    CoordinatorWriteRequests cwr = req;
    WriteRequest<CoordinatorWriteRequests> ww;
    ww.operation = cwr;

    spdlog::info("SM sending heartbeat to coordinator {}", coordinator_leader_.ToString());
    heartbeat_res_.emplace(std::move(
        io_.template Request<WriteRequest<CoordinatorWriteRequests>, WriteResponse<CoordinatorWriteResponses>>(
            coordinator_leader_, ww)));
    spdlog::info("SM sent heartbeat");
  }

  void EnsureShardsInitialized(HeartbeatResponse hr) {
    for (const auto &shard_to_initialize : hr.shards_to_initialize) {
      InitializeRsm(shard_to_initialize);
      initialized_but_not_confirmed_rsm_.emplace(shard_to_initialize.uuid);
    }
  }

  /// Returns true if the RSM was able to be initialized, and false if it was already initialized
  void InitializeRsm(coordinator::ShardToInitialize to_init) {
    if (rsm_map_.contains(to_init.uuid)) {
      // it's not a bug for the coordinator to send us UUIDs that we have
      // already created, because there may have been lag that caused
      // the coordinator not to hear back from us.
      return;
    }

    auto rsm_io = io_.ForkLocal();
    auto io_addr = rsm_io.GetAddress();
    io_addr.unique_id = to_init.uuid;
    rsm_io.SetAddress(io_addr);

    // TODO(tyler) get peers from Coordinator in HeartbeatResponse
    std::vector<Address> rsm_peers = {};

    std::unique_ptr<Shard> shard = std::make_unique<Shard>(to_init.label_id, to_init.min_key, to_init.max_key,
                                                           to_init.schema, to_init.config, to_init.id_to_names);

    ShardRsm rsm_state{std::move(shard)};

    ShardRaft<IoImpl> rsm{std::move(rsm_io), rsm_peers, std::move(rsm_state)};

    spdlog::info("SM created a new shard with UUID {}", to_init.uuid);

    rsm_map_.emplace(to_init.uuid, std::move(rsm));
  }
};

}  // namespace memgraph::storage::v3
