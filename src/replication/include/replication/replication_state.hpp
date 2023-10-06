// Copyright 2023 Memgraph Ltd.
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

#include <atomic>
#include <cstdint>

#include "replication/replication_epoch.hpp"
#include "utils/result.hpp"

namespace memgraph::replication {

enum class ReplicationRole : uint8_t { MAIN, REPLICA };

struct ReplicationState {
  auto GetRole() const -> ReplicationRole { return replication_role_.load(); }
  bool IsMain() const { return replication_role_ == ReplicationRole::MAIN; }
  bool IsReplica() const { return replication_role_ == ReplicationRole::REPLICA; }

  auto GetEpoch() const -> const ReplicationEpoch & { return epoch_; }
  auto GetEpoch() -> ReplicationEpoch & { return epoch_; }

 protected:
  void SetRole(ReplicationRole role) { return replication_role_.store(role); }

 private:
  ReplicationEpoch epoch_;
  std::atomic<memgraph::replication::ReplicationRole> replication_role_{ReplicationRole::MAIN};
};

}  // namespace memgraph::replication
