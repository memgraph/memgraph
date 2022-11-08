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
#include <unordered_map>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>

#include "coordinator/coordinator.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/message_conversion.hpp"
#include "io/messages.hpp"
#include "io/rsm/raft.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/config.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/shard_worker.hpp"

namespace memgraph::storage::v3 {

using boost::uuids::uuid;

using coordinator::CoordinatorWriteRequests;
using coordinator::CoordinatorWriteResponses;
using coordinator::HeartbeatRequest;
using coordinator::HeartbeatResponse;
using io::Address;
using io::Duration;
using io::Message;
using io::RequestId;
using io::ResponseFuture;
using io::Time;
using io::messages::CoordinatorMessages;
using io::messages::ShardManagerMessages;
using io::messages::ShardMessages;
using io::rsm::Raft;
using io::rsm::WriteRequest;
using io::rsm::WriteResponse;
using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;
using storage::v3::ShardRsm;

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
  ShardManager(io::Io<IoImpl> io, size_t shard_worker_threads, Address coordinator_leader)
      : io_(io), coordinator_leader_(coordinator_leader) {
    MG_ASSERT(shard_worker_threads >= 1);

    for (int i = 0; i < shard_worker_threads; i++) {
      shard_worker::Queue queue;
      shard_worker::ShardWorker worker{io, queue};
      auto worker_handle = std::jthread([worker = std::move(worker)]() mutable { worker.Run(); });

      workers_.emplace_back(queue);
      worker_handles_.emplace_back(std::move(worker_handle));
      worker_rsm_counts_.emplace_back(0);
    }
  }

  ShardManager(ShardManager &&) noexcept = default;
  ShardManager &operator=(ShardManager &&) noexcept = default;
  ShardManager(const ShardManager &) = delete;
  ShardManager &operator=(const ShardManager &) = delete;

  ~ShardManager() {
    for (auto worker : workers_) {
      worker.Push(shard_worker::ShutDown{});
    }

    workers_.clear();

    // The jthread handes for our shard worker threads will be
    // blocked on implicitly when worker_handles_ is destroyed.
  }

  size_t UuidToWorkerIndex(const uuid &to) {
    if (rsm_worker_mapping_.contains(to)) {
      return rsm_worker_mapping_.at(to);
    }

    // We will now create a mapping for this (probably new) shard
    // by choosing the worker with the lowest number of existing
    // mappings.

    size_t min_index = 0;
    size_t min_count = worker_rsm_counts_.at(min_index);

    for (int i = 0; i < worker_rsm_counts_.size(); i++) {
      size_t worker_count = worker_rsm_counts_.at(i);
      if (worker_count <= min_count) {
        min_count = worker_count;
        min_index = i;
      }
    }

    worker_rsm_counts_[min_index]++;
    rsm_worker_mapping_.emplace(to, min_index);

    return min_index;
  }

  void SendToWorkerByIndex(size_t worker_index, shard_worker::Message &&message) {
    workers_[worker_index].Push(std::forward<shard_worker::Message>(message));
  }

  void SendToWorkerByUuid(const uuid &to, shard_worker::Message &&message) {
    size_t worker_index = UuidToWorkerIndex(to);
    SendToWorkerByIndex(worker_index, std::forward<shard_worker::Message>(message));
  }

  /// Periodic protocol maintenance. Returns the time that Cron should be called again
  /// in the future.
  Time Cron() {
    spdlog::info("running ShardManager::Cron, address {}", io_.GetAddress().ToString());
    Time now = io_.Now();

    if (now >= next_reconciliation_) {
      Reconciliation();

      std::uniform_int_distribution time_distrib(kMinimumCronInterval.count(), kMaximumCronInterval.count());

      const auto rand = io_.Rand(time_distrib);

      next_reconciliation_ = now + Duration{rand};
    }

    for (auto &worker : workers_) {
      worker.Push(shard_worker::Cron{});
    }

    Time next_worker_cron = now + std::chrono::milliseconds(500);

    return std::min(next_worker_cron, next_reconciliation_);
  }

  /// Returns the Address for our underlying Io implementation
  Address GetAddress() { return io_.GetAddress(); }

  void Receive(ShardManagerMessages &&smm, RequestId request_id, Address from) {}

  void Route(ShardMessages &&sm, RequestId request_id, Address to, Address from) {
    Address address = io_.GetAddress();

    MG_ASSERT(address.last_known_port == to.last_known_port);
    MG_ASSERT(address.last_known_ip == to.last_known_ip);

    SendToWorkerByUuid(to.unique_id, shard_worker::RouteMessage{
                                         .message = std::move(sm),
                                         .request_id = request_id,
                                         .to = to,
                                         .from = from,
                                     });
  }

 private:
  io::Io<IoImpl> io_;
  std::vector<shard_worker::Queue> workers_;
  std::vector<std::jthread> worker_handles_;
  std::vector<size_t> worker_rsm_counts_;
  std::unordered_map<uuid, size_t, boost::hash<boost::uuids::uuid>> rsm_worker_mapping_;
  Time next_reconciliation_ = Time::min();
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
    for (const auto &to_init : hr.shards_to_initialize) {
      initialized_but_not_confirmed_rsm_.emplace(to_init.uuid);

      if (rsm_worker_mapping_.contains(to_init.uuid)) {
        // it's not a bug for the coordinator to send us UUIDs that we have
        // already created, because there may have been lag that caused
        // the coordinator not to hear back from us.
        return;
      }

      size_t worker_index = UuidToWorkerIndex(to_init.uuid);

      SendToWorkerByIndex(worker_index, to_init);

      rsm_worker_mapping_.emplace(to_init.uuid, worker_index);
    }
  }
};

}  // namespace memgraph::storage::v3
