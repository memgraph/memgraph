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

#include <io/address.hpp>
#include <io/messages.hpp>
#include <io/rsm/shard_rsm.hpp>
#include <io/time.hpp>
#include <io/transport.hpp>

namespace memgraph::storage::v3 {

using boost::uuids::uuid;
using memgraph::io::Address;
using memgraph::io::Duration;
using memgraph::io::Message;
using memgraph::io::RequestId;
using memgraph::io::Time;
using memgraph::io::messages::ShardManagerMessages;
using memgraph::io::messages::ShardMessages;
using memgraph::io::rsm::ShardRsm;
using memgraph::io::rsm::StorageReadRequest;
using memgraph::io::rsm::StorageReadResponse;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;

using ShardManagerOrRsmMessage = std::variant<ShardMessages, ShardManagerMessages>;
using TimeUuidPair = std::pair<Time, uuid>;

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
  ShardManager(io::Io<IoImpl> io) : io_(io) {}

  Duration Cron() {
    using namespace std::chrono_literals;

    return 50ms;
  }

  void Handle(Address from, Address to, RequestId request_id, ShardManagerOrRsmMessage message) {
    Address address = io_.GetAddress();

    MG_ASSERT(address.last_known_port == to.last_known_port);
    MG_ASSERT(address.last_known_ip == to.last_known_ip);

    std::visit([&](auto &&msg) { Handle(from, to, request_id, std::forward<decltype(msg)>(msg)); }, std::move(message));
  }

 private:
  io::Io<IoImpl> io_;
  std::map<uuid, ShardRsm> rsm_map_;
  std::priority_queue<std::pair<Time, uuid>, std::vector<std::pair<Time, uuid>>, std::greater<std::pair<Time, uuid>>>
      cron_schedule_;

  void Handle(Address from, Address to, RequestId request_id, ShardManagerMessages &&message) {}

  void Handle(Address from, Address to, RequestId request_id, ShardMessages &&message) {
    auto &rsm = rsm_map_.at(to.unique_id);
  }
};

}  // namespace memgraph::storage::v3
