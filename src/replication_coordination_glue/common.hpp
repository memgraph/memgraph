// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::replication_coordination_glue {

// Multiple versions because of changes in the RPC protocol, we need this in order to support ISSU

struct InstanceDBInfoV1 {
  std::string db_uuid;
  uint64_t latest_durable_timestamp;
};

struct InstanceDBInfo {
  std::string db_uuid;
  uint64_t latest_durable_timestamp;
  uint64_t num_committed_txns;
};

struct InstanceInfoV1 {
  std::vector<InstanceDBInfoV1> dbs_info;
  uint64_t last_committed_system_timestamp;
};

struct InstanceInfo {
  std::vector<InstanceDBInfo> dbs_info;
  uint64_t last_committed_system_timestamp;
};

}  // namespace memgraph::replication_coordination_glue
