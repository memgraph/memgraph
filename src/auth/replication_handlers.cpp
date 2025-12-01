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

#include "auth/replication_handlers.hpp"
#include <spdlog/spdlog.h>

#include "auth/auth.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "auth/rpc.hpp"
#include "license/license.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::rpc {
class FileReplicationHandler;
}  // namespace memgraph::rpc

namespace memgraph::auth {

void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                  std::string_view rpc_req) {
  spdlog::error(fmt::format("Received {} with main_id: {} != current_main_uuid: {}", rpc_req, std::string(main_req_id),
                            current_main_uuid.has_value() ? std::string(current_main_uuid.value()) : ""));
}

#ifdef MG_ENTERPRISE
void UpdateAuthDataHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, auth::SynchedAuth &auth,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::UpdateAuthDataReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  using replication::UpdateAuthDataRes;
  UpdateAuthDataRes res(false);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, replication::UpdateAuthDataReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("UpdateAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // Update
    if (req.user) {
      spdlog::trace("Saving user '{}'", req.user->username());
      auth->SaveUser(*req.user);
    }
    if (req.role) {
      spdlog::trace("Saving role '{}'", req.role->rolename());
      auth->SaveRole(*req.role);
    }
    if (req.profile) {
      spdlog::trace("Saving profile '{}'", req.profile->name);
      if (!auth->CreateOrUpdateProfile(req.profile->name, req.profile->limits, req.profile->usernames)) {
        spdlog::warn("Failed to create or update profile '{}'", req.profile->name);
        // silent failure
      }
    }
    // Success
    res = UpdateAuthDataRes(true);
    spdlog::debug("UpdateAuthDataHandler: SUCCESS");
  } catch (const auth::AuthException &e) {
    // Failure
    spdlog::trace("Saving role '{}' exception: {}", req.role->rolename(), e.what());
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void DropAuthDataHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, auth::SynchedAuth &auth,
                         uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::DropAuthDataReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  using replication::DropAuthDataRes;
  DropAuthDataRes res(false);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, replication::DropAuthDataRes::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DropAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  try {
    // Remove
    switch (req.type) {
      case replication::DropAuthDataReq::DataType::USER: {
        auth->RemoveUser(req.name);
      } break;
      case replication::DropAuthDataReq::DataType::ROLE: {
        auth->RemoveRole(req.name);
      } break;
      case replication::DropAuthDataReq::DataType::PROFILE: {
        auth->DropProfile(req.name);
      } break;
    }
    // Success
    res = DropAuthDataRes(true);
    spdlog::debug("DropAuthDataHandler: SUCCESS");
  } catch (const auth::AuthException & /* not used */) {
    // Failure
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

bool SystemRecoveryHandler(auth::SynchedAuth &auth, auth::Auth::Config auth_config,
                           const std::vector<auth::User> &users, const std::vector<auth::Role> &roles,
                           const std::vector<auth::UserProfiles::Profile> &profiles) {
  return auth.WithLock([&](auto &locked_auth) {
    // Update config
    locked_auth.SetConfig(std::move(auth_config));

    // Profiles are only supported with a license
    if (license::global_license_checker.IsEnterpriseValidFast()) {
      // Get all current profiles
      auto old_profiles = locked_auth.AllProfiles();
      // Save incoming profiles
      for (const auto &profile : profiles) {
        // Missing profile
        if (!locked_auth.CreateOrUpdateProfile(profile.name, profile.limits, profile.usernames)) {
          spdlog::debug("SystemRecoveryHandler: Failed to save profile");
          return false;
        }
        const auto it = std::find_if(old_profiles.begin(), old_profiles.end(),
                                     [&](const auto &p) { return p.name == profile.name; });
        if (it != old_profiles.end()) old_profiles.erase(it);
      }
      // Delete all the leftover profiles
      for (const auto &profile : old_profiles) {
        if (!locked_auth.DropProfile(profile.name)) {
          spdlog::debug("SystemRecoveryHandler: Failed to remove profile \"{}\".", profile.name);
          return false;
        }
      }
    }

    // Roles are only supported with a license
    if (license::global_license_checker.IsEnterpriseValidFast()) {
      // Get all current roles
      auto old_roles = locked_auth.AllRolenames();
      // Save incoming roles
      for (const auto &role : roles) {
        // Missing roles
        try {
          locked_auth.SaveRole(role);
        } catch (const auth::AuthException &) {
          spdlog::debug("SystemRecoveryHandler: Failed to save role");
          return false;
        }
        const auto it = std::find(old_roles.begin(), old_roles.end(), role.rolename());
        if (it != old_roles.end()) old_roles.erase(it);
      }
      // Delete all the leftover roles
      for (const auto &role : old_roles) {
        if (!locked_auth.RemoveRole(role)) {
          spdlog::debug("SystemRecoveryHandler: Failed to remove role \"{}\".", role);
          return false;
        }
      }
    }

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
    // Success
    return true;
  });
}

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              auth::SynchedAuth &auth) {
  // NOTE: Register even without license as the user could add a license at run-time
  data.server->rpc_server_.Register<replication::UpdateAuthDataRpc>(
      [&data, system_state_access, &auth](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, auto *req_reader, auto *res_builder) mutable {
        UpdateAuthDataHandler(system_state_access, data.uuid_, auth, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<replication::DropAuthDataRpc>(
      [&data, system_state_access, &auth](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version, auto *req_reader, auto *res_builder) mutable {
        DropAuthDataHandler(system_state_access, data.uuid_, auth, request_version, req_reader, res_builder);
      });
}
#endif

}  // namespace memgraph::auth
