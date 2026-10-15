// Copyright 2026 Memgraph Ltd.
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

#include <cstdint>
#include <map>
#include <string>
#include <utility>

namespace memgraph::coordination {

struct ReplicaDBLagData {
  uint64_t num_committed_txns_;
  // int64_t because for a brief time window replica can be in front of new main. That's because an instance can become
  // main without having all old transactions. Possible with SYNC replication
  int64_t num_txns_behind_main_;
};

struct ReplicationLagInfo {
  // db -> num_committed_txns on main
  std::map<std::string, uint64_t> dbs_main_committed_txns_;
  // instance -> db -> data
  std::map<std::string, std::map<std::string, ReplicaDBLagData>> replicas_info_;
};

// instance -> db -> data, as reported by SHOW REPLICATION LAG.
using ReplicationLagData = std::map<std::string, std::map<std::string, ReplicaDBLagData>>;

// Why the leader has no lag data to report. Crosses the wire in CoordReplicationLagRes v2, so new values go last: an
// older peer must keep decoding the ones it already knows.
enum class ReplicationLagStatus : uint8_t {
  SUCCESS = 0,
  // Raft made this coordinator leader but its leader callback hasn't finished yet.
  LEADER_NOT_READY,
  // No instance is annotated as main in the Raft log.
  NO_CURRENT_MAIN,
  // The lag RPC to the main failed.
  MAIN_UNRESPONSIVE,
  // The instance the Raft log records as main reports that it is a replica, so the leader's view is stale.
  MAIN_IS_REPLICA,
  N
};

// The leader's answer: either the lag data or the reason it has none. A successful answer always holds at least the
// main's own entry, so empty data can only accompany a failure status.
struct ReplicationLagResult {
  ReplicationLagStatus status_{ReplicationLagStatus::SUCCESS};
  ReplicationLagData data_{};

  static auto Success(ReplicationLagData data) -> ReplicationLagResult {
    return ReplicationLagResult{.status_ = ReplicationLagStatus::SUCCESS, .data_ = std::move(data)};
  }

  static auto Failure(ReplicationLagStatus const status) -> ReplicationLagResult {
    return ReplicationLagResult{.status_ = status};
  }

  // v1 peers know only the bare map and already read an empty one as "no data", which is what every failure degrades
  // to for them.
  auto Downgrade() const -> ReplicationLagData { return data_; }
};

}  // namespace memgraph::coordination
