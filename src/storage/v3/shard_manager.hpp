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
#include "io/rsm/shard_rsm.hpp"

namespace memgraph::storage::v3 {

using boost::uuids::uuid;
using memgraph::io::rsm::ShardRsm;

/// The ShardManager is responsible for:
/// * reconciling the storage engine's local configuration with the Coordinator's
///   intentions for how it should participate in multiple raft clusters
/// * replying to heartbeat requests to the Coordinator
/// * routing incoming messages to the appropriate sRSM
///
/// Every storage engine has exactly one RsmEngine.
template <typename IoImpl>
class ShardManager {
  Io<IoImpl> io_;
  std::map<uuid, ShardRsm> rsm_map_;

 public:
  ShardManager(Io<IoImpl> io) : io_(io) {}

  Duration Cron() {
    using namespace std::chrono_literals;

    return 50ms;
  }
};

}  // namespace memgraph::storage::v3
