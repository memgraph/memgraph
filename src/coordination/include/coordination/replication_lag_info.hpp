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

#include <cstdint>
#include <string>

namespace memgraph::coordination {

struct ReplicaDBLagData {
  uint64_t num_committed_txns_;
  uint64_t num_txns_behind_main_;
};

struct ReplicationLagInfo {
  // db -> num_committed_txns on main
  std::map<std::string, uint64_t> dbs_main_committed_txns_;
  // instance -> db -> data
  std::map<std::string, std::map<std::string, ReplicaDBLagData>> replicas_info_;
};

}  // namespace memgraph::coordination
