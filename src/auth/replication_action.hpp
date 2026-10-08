// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <utility>

#include "auth/auth.hpp"
#include "replication/replication_client.hpp"
#include "replication/state.hpp"
#include "system/action.hpp"
#include "utils/uuid.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::auth {

/// One auth transaction's replication, as a single request. A replica applies every operation in it or none, so
/// it cannot be left holding part of a transaction -- a user without the grant that accompanied it, say. A
/// statement outside a transaction takes the same path with a batch of one.
struct BatchedAuthAction final : memgraph::system::ISystemAction {
  explicit BatchedAuthAction(PendingActions ops) : ops_{std::move(ops)} {}

  void DoDurability() override { /* Done during Auth execution */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  // system::Transaction is only forward-declared here, so reading its timestamps happens in the .cpp.
  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override;

  void PostReplication(replication::RoleMainData & /*main_data*/) const override {}

 private:
  PendingActions ops_;
};

}  // namespace memgraph::auth
#endif
