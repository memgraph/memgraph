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
#include <string>
#include <vector>

namespace memgraph::replication_coordination_glue {

constexpr uint64_t kDefaultDeltasBatchProgressSize{100'000};

// Multiple versions because of changes in the RPC protocol, we need this in order to support ISSU
struct InstanceDBInfoV1 {
  std::string db_uuid;
  uint64_t latest_durable_timestamp;

  friend bool operator==(InstanceDBInfoV1 const &, InstanceDBInfoV1 const &) = default;
};

struct InstanceDBInfoV2 {
  std::string db_uuid;
  uint64_t num_committed_txns;

  friend bool operator==(InstanceDBInfoV2 const &, InstanceDBInfoV2 const &) = default;
};

struct InstanceDBInfo {
  std::string db_uuid;
  uint64_t num_committed_txns;
  // Storage refused service (e.g. replica recovery failed after Clear()). Absent on the wire before V3, where it is
  // read as false.
  bool broken{false};

  friend bool operator==(InstanceDBInfo const &, InstanceDBInfo const &) = default;
};

struct InstanceInfoV1 {
  std::vector<InstanceDBInfoV1> dbs_info;
  uint64_t last_committed_system_timestamp;
};

struct InstanceInfoV2 {
  std::vector<InstanceDBInfoV2> dbs_info;
  uint64_t last_committed_system_timestamp;
};

struct InstanceInfo {
  std::vector<InstanceDBInfo> dbs_info;
  uint64_t last_committed_system_timestamp;
};

// V2 predates the broken bit; an instance answering in V2 is treated as not broken.
inline InstanceInfo ToInstanceInfo(InstanceInfoV2 const &v2) {
  InstanceInfo info{.dbs_info = {}, .last_committed_system_timestamp = v2.last_committed_system_timestamp};
  info.dbs_info.reserve(v2.dbs_info.size());
  for (auto const &db : v2.dbs_info) {
    info.dbs_info.push_back({.db_uuid = db.db_uuid, .num_committed_txns = db.num_committed_txns, .broken = false});
  }
  return info;
}

}  // namespace memgraph::replication_coordination_glue
