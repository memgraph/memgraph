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

#include "nuraft/raft_log_action.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/rw_lock.hpp"

#include <libnuraft/nuraft.hxx>
#include <range/v3/view.hpp>

#include <map>
#include <numeric>
#include <string>

namespace memgraph::coordination {

using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::ptr;

class CoordinatorClusterState {
 public:
  auto MainExists() const -> bool;

  auto IsMain(std::string const &instance_name) const -> bool;

  auto IsReplica(std::string const &instance_name) const -> bool;

  auto InsertInstance(std::string const &instance_name, replication_coordination_glue::ReplicationRole role) -> void;

  auto DoAction(std::string const &instance_name, RaftLogAction log_action) -> void;

  auto Serialize(ptr<buffer> &data) -> void;

  static auto Deserialize(buffer &data) -> CoordinatorClusterState;

  auto GetInstances() const -> std::vector<std::pair<std::string, std::string>>;

 private:
  std::map<std::string, replication_coordination_glue::ReplicationRole> instance_roles;
};

}  // namespace memgraph::coordination
#endif
