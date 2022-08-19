// Copyright 2022 Memgraph Ltd.
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

#include <boost/uuid/uuid.hpp>
#include "io/rsm/coordinator_rsm.hpp"
#include "io/rsm/rsm_client.hpp"

namespace memgraph::storage::v3 {

using boost::uuids::uuid;
using memgraph::coordinator;
using memgraph::io::rsm::RsmClient;

/// The RsmEngine is responsible for:
/// * reconciling the storage engine's local configuration with the Coordinator's
///   intentions for how it should participate in multiple raft clusters
/// * replying to heartbeat requests to the Coordinator
/// * routing incoming messages from the Io interface to the appropriate
///   storage RSM
///
/// Every storage engine has exactly one RsmEngine.
template <typename IoImpl>
class RsmEngine {
  Io<IoImpl> io_;
  std::map<uuid, StorageRsm> rsm_map_;

 public:
  RsmEngine(Io<IoImpl> io) : io_(io) {}
};

}  // namespace memgraph::storage::v3
