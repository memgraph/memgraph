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

#ifdef MG_ENTERPRISE

#include "utils/uuid.hpp"

#include <optional>

namespace memgraph::coordination {

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

  // Follows the logic of other RPC versioning code. For responses, we downgrade newer version to the older version
  InstanceStateV1 Downgrade() const {
    return {.is_replica = is_replica, .uuid = uuid, .is_writing_enabled = is_writing_enabled};
  }
};

}  // namespace memgraph::coordination

#endif
