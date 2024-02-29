// Copyright 2024 Memgraph Ltd.
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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_config.hpp"
#include "nuraft/raft_log_action.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/rw_lock.hpp"

#include <libnuraft/nuraft.hxx>
#include <range/v3/view.hpp>

#include <map>
#include <numeric>
#include <string>
#include <variant>

namespace memgraph::coordination {

using TRaftLog = std::variant<CoordinatorClientConfig, std::string>;

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class CoordinatorClusterState {
 public:
  auto MainExists() const -> bool;

  auto IsMain(std::string_view instance_name) const -> bool;

  auto IsReplica(std::string_view instance_name) const -> bool;

  auto InsertInstance(std::string_view instance_name, replication_coordination_glue::ReplicationRole role) -> void;

  auto DoAction(TRaftLog log_entry, RaftLogAction log_action) -> void;

  auto Serialize(ptr<buffer> &data) -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetInstances() const -> std::vector<std::pair<std::string, std::string>>;

 private:
  std::map<std::string, replication_coordination_glue::ReplicationRole, std::less<>> instance_roles;
  // TODO: (andi) Good place for separate lock
};

}  // namespace memgraph::coordination
#endif