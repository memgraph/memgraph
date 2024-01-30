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

#include "auth/replication_handlers.hpp"

#include "auth/auth.hpp"
#include "auth/rpc.hpp"
#include "license/license.hpp"

namespace memgraph::auth {

#ifdef MG_ENTERPRISE
void UpdateAuthDataHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access, auth::SynchedAuth &auth,
                           slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::UpdateAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::replication::UpdateAuthDataRes;
  UpdateAuthDataRes res(false);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("UpdateAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Update
    if (req.user) auth->SaveUser(*req.user);
    if (req.role) auth->SaveRole(*req.role);
    // Success
    system_state_access.SetLastCommitedTS(req.new_group_timestamp);
    res = UpdateAuthDataRes(true);
    spdlog::debug("UpdateAuthDataHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
  } catch (const auth::AuthException & /* not used */) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void DropAuthDataHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access, auth::SynchedAuth &auth,
                         slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::DropAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::replication::DropAuthDataRes;
  DropAuthDataRes res(false);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Remove
    switch (req.type) {
      case replication::DropAuthDataReq::DataType::USER:
        auth->RemoveUser(req.name);
        break;
      case replication::DropAuthDataReq::DataType::ROLE:
        auth->RemoveRole(req.name);
        break;
    }
    // Success
    system_state_access.SetLastCommitedTS(req.new_group_timestamp);
    res = DropAuthDataRes(true);
    spdlog::debug("DropAuthDataHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
  } catch (const auth::AuthException & /* not used */) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

bool SystemRecoveryHandler(auth::SynchedAuth &auth, auth::Auth::Config auth_config,
                           const std::vector<auth::User> &users, const std::vector<auth::Role> &roles) {
  return auth.WithLock([&](auto &locked_auth) {
    // Update config
    locked_auth.SetConfig(std::move(auth_config));
    // Get all current users
    auto old_users = locked_auth.AllUsernames();
    // Save incoming users
    for (const auto &user : users) {
      // Missing users
      try {
        locked_auth.SaveUser(user);
      } catch (const auth::AuthException &) {
        spdlog::debug("SystemRecoveryHandler: Failed to save user");
        return false;
      }
      const auto it = std::find(old_users.begin(), old_users.end(), user.username());
      if (it != old_users.end()) old_users.erase(it);
    }
    // Delete all the leftover users
    for (const auto &user : old_users) {
      if (!locked_auth.RemoveUser(user)) {
        spdlog::debug("SystemRecoveryHandler: Failed to remove user \"{}\".", user);
        return false;
      }
    }

    // Roles are only supported with a license
    if (license::global_license_checker.IsEnterpriseValidFast()) {
      // Get all current roles
      auto old_roles = locked_auth.AllRolenames();
      // Save incoming users
      for (const auto &role : roles) {
        // Missing users
        try {
          locked_auth.SaveRole(role);
        } catch (const auth::AuthException &) {
          spdlog::debug("SystemRecoveryHandler: Failed to save user");
          return false;
        }
        const auto it = std::find(old_roles.begin(), old_roles.end(), role.rolename());
        if (it != old_roles.end()) old_roles.erase(it);
      }
      // Delete all the leftover users
      for (const auto &role : old_roles) {
        if (!locked_auth.RemoveRole(role)) {
          spdlog::debug("SystemRecoveryHandler: Failed to remove user \"{}\".", role);
          return false;
        }
      }
    }

    // Success
    return true;
  });
}

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              auth::SynchedAuth &auth) {
  // NOTE: Register even without license as the user could add a license at run-time
  data.server->rpc_server_.Register<replication::UpdateAuthDataRpc>(
      [system_state_access, &auth](auto *req_reader, auto *res_builder) mutable {
        spdlog::debug("Received UpdateAuthDataRpc");
        UpdateAuthDataHandler(system_state_access, auth, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<replication::DropAuthDataRpc>(
      [system_state_access, &auth](auto *req_reader, auto *res_builder) mutable {
        spdlog::debug("Received DropAuthDataRpc");
        DropAuthDataHandler(system_state_access, auth, req_reader, res_builder);
      });
}
#endif

}  // namespace memgraph::auth
