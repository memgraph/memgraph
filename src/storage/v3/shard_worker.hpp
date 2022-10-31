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

#include <deque>
#include <memory>
#include <queue>
#include <variant>

#include <boost/uuid/uuid.hpp>

#include "io/future.hpp"
#include "io/rsm/raft.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/shard_rsm.hpp"

namespace memgraph::storage::v3::shard_worker {

/// Obligations:
/// * ShutDown
/// * Cron
/// * Handle
/// * InitializeRsm

using boost::uuids::uuid;

using io::Time;
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

struct Cron {
  io::Promise<io::Time> request_next_cron_at;
};

struct InitializeRsm {};

struct Handle {};

using Message = std::variant<ShutDown, Cron, InitializeRsm, Handle>;

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
    Time ret = Cron();
    return true;
  }
  bool Process(InitializeRsm &&initialize_rsm) { return true; }
  bool Process(Handle &&handle) { return true; }

  Time Cron() {
    spdlog::info("running ShardManager::Cron, address {}", io_.GetAddress().ToString());
    Time now = io_.Now();

    if (!cron_schedule_.empty()) {
      const auto &[time, uuid] = cron_schedule_.top();

      auto &rsm = rsm_map_.at(uuid);
      Time next_for_uuid = rsm.Cron();

      cron_schedule_.pop();
      cron_schedule_.push(std::make_pair(next_for_uuid, uuid));

      const auto &[next_time, _uuid] = cron_schedule_.top();

      return std::min(next_cron_, next_time);
    }

    return next_cron_;
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
