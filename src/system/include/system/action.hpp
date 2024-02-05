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

#include "replication/epoch.hpp"
#include "replication/replication_client.hpp"
#include "replication/state.hpp"

namespace memgraph::system {

struct Transaction;

/// The system action interface that subsystems will implement. This OO-style separation is needed so that one common
/// mechanism can be used for all subsystem replication within a system transaction, without the need for System to
/// know about all the subsystems.
struct ISystemAction {
  /// Durability step which is defered until commit time
  virtual void DoDurability() = 0;

  /// Prepare the RPC payload that will be sent to all replicas clients
  virtual bool DoReplication(memgraph::replication::ReplicationClient &client,
                             memgraph::replication::ReplicationEpoch const &epoch,
                             Transaction const &system_tx) const = 0;

  virtual void PostReplication(memgraph::replication::RoleMainData &main_data) const = 0;

  virtual ~ISystemAction() = default;
};
}  // namespace memgraph::system
