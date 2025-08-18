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

#include "storage/v2/commit_args.hpp"
#include "storage/v2/replication/replication_transaction.hpp"

namespace memgraph::storage {

bool CommitArgs::two_phase_commit(TransactionReplication &replicating_txn) const {
  auto const f = utils::Overloaded{
      [&](Main const &) -> bool { return replicating_txn.ShouldRunTwoPC(); },
      [](ReplicaWrite const &replica) -> bool { return replica.two_phase_commit_; },
      [](ReplicaRead const &) -> bool { throw std::runtime_error("no durability should happen for ReplicaRead"); }};
  return std::visit(f, data);
}
}  // namespace memgraph::storage
