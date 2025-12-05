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

module;

#include "utils/uuid.hpp"

#include <optional>

export module memgraph.coordination.instance_state;

#ifdef MG_ENTERPRISE

export namespace memgraph::coordination {

struct InstanceStateV1 {
  bool is_replica;                  // MAIN or REPLICA
  std::optional<utils::UUID> uuid;  // MAIN's UUID or the UUID which REPLICA listens
  bool is_writing_enabled;          // on replica it's never enabled. On main depends.
};

struct InstanceState {
  bool is_replica;                                               // MAIN or REPLICA
  std::optional<utils::UUID> uuid;                               // MAIN's UUID or the UUID which REPLICA listens
  bool is_writing_enabled;                                       // on replica it's never enabled. On main depends.
  std::optional<std::map<std::string, uint64_t>> main_num_txns;  // if main, returns db->num_committed_txns
  std::optional<std::map<std::string, std::map<std::string, int64_t>>>
      replicas_num_txns;  // if main, return num of committed txns for each instance

  // Follows the logic of other RPC versioning code. For responses, we downgrade newer version to the older version
  InstanceStateV1 Downgrade() const;
};

}  // namespace memgraph::coordination

module : private;

namespace memgraph::coordination {

InstanceStateV1 InstanceState::Downgrade() const {
  return {.is_replica = is_replica, .uuid = uuid, .is_writing_enabled = is_writing_enabled};
}

}  // namespace memgraph::coordination

#endif
