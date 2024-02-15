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

#include "auth/auth.hpp"
#include "replication/state.hpp"
#include "slk/streams.hpp"
#include "system/state.hpp"

namespace memgraph::auth {

void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                  std::string_view rpc_req);

#ifdef MG_ENTERPRISE
void UpdateAuthDataHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, auth::SynchedAuth &auth,
                           slk::Reader *req_reader, slk::Builder *res_builder);
void DropAuthDataHandler(system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, auth::SynchedAuth &auth,
                         slk::Reader *req_reader, slk::Builder *res_builder);

bool SystemRecoveryHandler(auth::SynchedAuth &auth, auth::Auth::Config auth_config,
                           const std::vector<auth::User> &users, const std::vector<auth::Role> &roles);
void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              auth::SynchedAuth &auth);
#endif
}  // namespace memgraph::auth
