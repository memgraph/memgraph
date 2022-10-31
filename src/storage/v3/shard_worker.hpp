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

#include <chrono>
#include <deque>
#include <memory>
#include <queue>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include "coordinator/coordinator.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/future.hpp"
#include "io/messages.hpp"
#include "io/rsm/raft.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/shard_rsm.hpp"

namespace memgraph::storage::v3::shard_worker {

/// Obligations:
/// * ShutDown
/// * Cron
/// * RouteMessage
/// * ShardToInitialize

using boost::uuids::uuid;

using coordinator::ShardToInitialize;
using io::Address;
using io::RequestId;
using io::Time;
using io::messages::ShardMessages;
using io::rsm::Raft;
using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;
using storage::v3::ShardRsm;

template <typename IoImpl>
using ShardRaft = Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

struct ShutDown {
  io::Promise<bool> acknowledge_shutdown;
};

struct Cron {};

struct RouteMessage {
  ShardMessages message;
  RequestId request_id;
  Address to;
  Address from;
};

using Message = std::variant<ShutDown, Cron, ShardToInitialize, RouteMessage>;

struct QueueInner {
  std::mutex mu{};
  std::condition_variable cv;
  // TODO(tyler) handle simulator communication std::shared_ptr<std::atomic<int>> blocked;

  // TODO(tyler) investigate using a priority queue that prioritizes messages in a way that
  // improves overall QoS. For example, maybe we want to schedule raft Append messages
  // ahead of Read messages or generally writes before reads for lowering the load on the
  // overall system faster etc... When we do this, we need to make sure to avoid
  // starvation by sometimes randomizing priorities, rather than following a strict
  // prioritization.
  std::deque<Message> queue;
};

/// There are two reasons to implement our own Queue instead of using
/// one off-the-shelf:
/// 1. we will need to know in the simulator when all threads are waiting
/// 2. we will want to implement our own priority queue within this for QoS
class Queue {
  std::shared_ptr<QueueInner> inner_ = std::make_shared<QueueInner>();

 public:
  void Push(Message &&message) {
    {
      std::unique_lock<std::mutex> lock(inner_->mu);

      inner_->queue.push_back(std::forward<Message>(message));
    }  // lock dropped before notifying condition variable

    inner_->cv.notify_all();
  }

  Message Pop() {
    std::unique_lock<std::mutex> lock(inner_->mu);

    while (inner_->queue.empty()) {
      inner_->cv.wait(lock);
    }

    Message message = std::move(inner_->queue.front());
    inner_->queue.pop_front();

    return message;
  }
};

/// A ShardWorker owns Raft<ShardRsm> instances. receives messages from the ShardManager.
template <class IoImpl>
class ShardWorker {
  io::Io<IoImpl> io_;
  Queue queue_;
  std::priority_queue<std::pair<Time, uuid>, std::vector<std::pair<Time, uuid>>, std::greater<>> cron_schedule_;
  Time next_cron_ = Time::min();
  std::map<uuid, ShardRaft<IoImpl>> rsm_map_;

  bool Process(ShutDown &&shut_down) {
    shut_down.acknowledge_shutdown.Fill(true);
    return false;
  }

  bool Process(Cron &&cron) {
    Cron();
    return true;
  }

  bool Process(ShardToInitialize &&shard_to_initialize) {
    InitializeRsm(std::forward<ShardToInitialize>(shard_to_initialize));

    return true;
  }

  bool Process(RouteMessage &&route_message) {
    auto &rsm = rsm_map_.at(route_message.to.unique_id);

    rsm.Handle(std::move(route_message.message), route_message.request_id, route_message.from);

    return true;
  }

  Time Cron() {
    spdlog::info("running ShardManager::Cron, address {}", io_.GetAddress().ToString());
    Time now = io_.Now();

    while (!cron_schedule_.empty()) {
      const auto &[time, uuid] = cron_schedule_.top();

      if (time <= now) {
        auto &rsm = rsm_map_.at(uuid);
        Time next_for_uuid = rsm.Cron();

        cron_schedule_.pop();
        cron_schedule_.push(std::make_pair(next_for_uuid, uuid));

        const auto &[next_time, _uuid] = cron_schedule_.top();
      } else {
        return time;
      }
    }

    return now + std::chrono::microseconds(1000);
  }

  void InitializeRsm(ShardToInitialize to_init) {
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

    // perform an initial Cron call for the new RSM
    Time next_cron = rsm.Cron();
    cron_schedule_.push(std::make_pair(next_cron, to_init.uuid));

    rsm_map_.emplace(to_init.uuid, std::move(rsm));
  }

 public:
  ShardWorker(io::Io<IoImpl> io, Queue queue) : io_(io), queue_(queue) {}
  ShardWorker(ShardWorker &&) = default;
  ShardWorker &operator=(ShardWorker &&) = default;
  ShardWorker(const ShardWorker &) = delete;
  ShardWorker &operator=(const ShardWorker &) = delete;
  ~ShardWorker() = default;

  void Run() {
    while (true) {
      Message message = queue_.Pop();

      const bool should_continue =
          std::visit([&](auto &&msg) { return Process(std::forward<decltype(msg)>(msg)); }, std::move(message));

      if (!should_continue) {
        return;
      }
    }
  }
};

}  // namespace memgraph::storage::v3::shard_worker
