// Copyright 2026 Memgraph Ltd.
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

#include <thread>
#include <utility>

#include "auth/auth.hpp"
#include "auth/auth_layer.hpp"
#include "auth_global.hpp"
#include "glue/auth.hpp"
#include "license/license.hpp"
#include "query/auth_query_handler.hpp"
#include "utils/join_vector.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::glue {

class AuthQueryHandler final : public memgraph::query::AuthQueryHandler {
  memgraph::auth::AuthLayer layer_;

 public:
  explicit AuthQueryHandler(memgraph::auth::SynchedAuth *auth);

  [[nodiscard]] bool CommitTransaction(memgraph::auth::AuthTransaction &tx,
                                       memgraph::system::Transaction *system_tx) override {
    return layer_.Commit(tx, system_tx);
  }

  query::CreateUserResult CreateUser(const std::string &username, const std::optional<std::string> &password,
                                     memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  bool DropUser(const std::string &username, memgraph::auth::AuthTransaction *auth_tx,
                system::Transaction *system_tx) override;

  void SetPassword(const std::string &username, const std::optional<std::string> &password,
                   memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                      const std::optional<std::string> &newPassword, memgraph::auth::AuthTransaction *auth_tx,
                      system::Transaction *system_tx) override;

#ifdef MG_ENTERPRISE
  void GrantDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                     memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void DenyDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                    memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void RevokeDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                      memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetDatabasePrivileges(
      const std::string &user, const std::vector<std::string> &roles, auth::UserOrRoleType type,
      memgraph::auth::AuthTransaction *auth_tx) override;

  void SetMainDatabase(std::string_view db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                       memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void DeleteDatabase(std::string_view db_name, memgraph::auth::AuthTransaction *auth_tx,
                      system::Transaction *system_tx) override;

  std::optional<std::string> GetMainDatabase(const std::string &user_or_role, auth::UserOrRoleType type,
                                             memgraph::auth::AuthTransaction *auth_tx) override;
#endif

  bool CreateRole(const std::string &rolename, memgraph::auth::AuthTransaction *auth_tx,
                  system::Transaction *system_tx) override;

  bool DropRole(const std::string &rolename, memgraph::auth::AuthTransaction *auth_tx,
                system::Transaction *system_tx) override;

  bool HasRole(const std::string &rolename, memgraph::auth::AuthTransaction *auth_tx) override;

  std::vector<memgraph::query::TypedValue> GetUsernames(memgraph::auth::AuthTransaction *auth_tx) override;

  std::vector<memgraph::query::RolenameResult> GetRolenames(memgraph::auth::AuthTransaction *auth_tx) override;

  std::vector<memgraph::query::RolenameResult> GetRolenamesForUser(const std::string &username,
                                                                   std::optional<std::string> db_name,
                                                                   memgraph::auth::AuthTransaction *auth_tx) override;

  std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename,
                                                               memgraph::auth::AuthTransaction *auth_tx) override;

  void SetRoles(const std::string &username, const std::vector<std::string> &roles,
                const std::unordered_set<std::string> &role_databases, memgraph::auth::AuthTransaction *auth_tx,
                system::Transaction *system_tx) override;

  void RemoveRole(const std::string &username, const std::string &rolename, memgraph::auth::AuthTransaction *auth_tx,
                  system::Transaction *system_tx) override;

  void ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                  memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void AddRoles(const std::string &username, const std::vector<std::string> &roles,
                const std::unordered_set<std::string> &role_databases, memgraph::auth::AuthTransaction *auth_tx,
                system::Transaction *system_tx) override;

  void RevokeRoles(const std::string &username, const std::vector<std::string> &roles,
                   const std::unordered_set<std::string> &role_databases, memgraph::auth::AuthTransaction *auth_tx,
                   system::Transaction *system_tx) override;

  using query::AuthQueryHandler::GetPrivileges;

  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(
      const std::string &user_or_role, std::optional<std::string>, auth::UserOrRoleType type,
      memgraph::auth::AuthTransaction *auth_tx) override;

  void GrantPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<memgraph::query::AuthQuery::LabelMatchingMode> &label_matching_modes,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void DenyPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<memgraph::query::AuthQuery::LabelMatchingMode> &label_matching_modes,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

  void RevokePrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<memgraph::query::AuthQuery::LabelMatchingMode> &label_matching_modes,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;

// User profiles
#ifdef MG_ENTERPRISE
  void CreateProfile(const std::string &profile_name, const query::UserProfileQuery::limits_t &defined_limits,
                     const std::unordered_set<std::string> &usernames, memgraph::auth::AuthTransaction *auth_tx,
                     system::Transaction *system_tx) override;
  void UpdateProfile(const std::string &profile_name, const query::UserProfileQuery::limits_t &updated_limits,
                     memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;
  void DropProfile(const std::string &profile_name, memgraph::auth::AuthTransaction *auth_tx,
                   system::Transaction *system_tx) override;
  query::UserProfileQuery::limits_t GetProfile(std::string_view name,
                                               memgraph::auth::AuthTransaction *auth_tx) override;
  std::vector<std::pair<std::string, query::UserProfileQuery::limits_t>> AllProfiles(
      memgraph::auth::AuthTransaction *auth_tx) override;
  void SetProfile(const std::string &profile_name, const std::string &user_or_role,
                  memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx) override;
  void RevokeProfile(const std::string &user_or_role, memgraph::auth::AuthTransaction *auth_tx,
                     system::Transaction *system_tx) override;
  std::optional<std::string> GetProfileForUser(const std::string &user_or_role,
                                               memgraph::auth::AuthTransaction *auth_tx) override;
  std::vector<std::string> GetUsernamesForProfile(const std::string &profile_name,
                                                  memgraph::auth::AuthTransaction *auth_tx) override;
  std::optional<std::string> GetProfileForRole(const std::string &user_or_role) override;
  std::vector<std::string> GetRolenamesForProfile(const std::string &profile_name) override;

  void GrantPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                               const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                               auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                               auth::PropertyPermissionType perm_type, memgraph::auth::AuthTransaction *auth_tx,
                               system::Transaction *system_tx) override;
  void DenyPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                              const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                              auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                              auth::PropertyPermissionType perm_type, memgraph::auth::AuthTransaction *auth_tx,
                              system::Transaction *system_tx) override;
  void RevokePropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                                const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                                auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                                auth::PropertyPermissionType perm_type, memgraph::auth::AuthTransaction *auth_tx,
                                system::Transaction *system_tx) override;
#endif

 private:
  template <class TEditPermissionsFun
#ifdef MG_ENTERPRISE
            ,
            class TEditFineGrainedPermissionsFun
#endif
            >
  void EditPermissions(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<memgraph::query::AuthQuery::LabelMatchingMode> &label_matching_modes,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      const TEditPermissionsFun &edit_permissions_fun
#ifdef MG_ENTERPRISE
      ,
      const TEditFineGrainedPermissionsFun &edit_fine_grained_permissions_fun
#endif
      ,
      auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx);

#ifdef MG_ENTERPRISE
  template <typename EditFn>
  void EditPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                              const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                              auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                              memgraph::auth::AuthTransaction *auth_tx, system::Transaction *system_tx,
                              EditFn const &edit_fn);
#endif

#ifdef MG_ENTERPRISE
  void GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                            auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx,
                            system::Transaction *system_tx) override;
  void DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                           auth::UserOrRoleType type, memgraph::auth::AuthTransaction *auth_tx,
                           system::Transaction *system_tx) override;
#endif

 private:
  /// Locked access, routed through the caller's transaction when there is one.
  auto Lock(memgraph::auth::AuthTransaction *auth_tx) { return layer_.Lock(auth_tx); }

  /// Reads inside a transaction take the WRITE lock, not a shared one: installing the overlay mutates Auth's storage
  /// handle, so it cannot be shared with a concurrent reader. The exclusion lasts one statement, not the transaction.
  auto ReadLock(memgraph::auth::AuthTransaction *auth_tx) { return layer_.ReadLock(auth_tx); }
};
}  // namespace memgraph::glue
