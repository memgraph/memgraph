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

#include "auth/auth.hpp"
#include "auth_global.hpp"
#include "glue/auth.hpp"
#include "license/license.hpp"
#include "query/auth_query_handler.hpp"
#include "utils/string.hpp"

namespace memgraph::glue {

class AuthQueryHandler final : public memgraph::query::AuthQueryHandler {
  memgraph::auth::SynchedAuth *auth_;

 public:
  explicit AuthQueryHandler(memgraph::auth::SynchedAuth *auth);

  bool CreateUser(const std::string &username, const std::optional<std::string> &password,
                  system::Transaction *system_tx) override;

  bool DropUser(const std::string &username, system::Transaction *system_tx) override;

  void SetPassword(const std::string &username, const std::optional<std::string> &password,
                   system::Transaction *system_tx) override;

  void ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                      const std::optional<std::string> &newPassword, system::Transaction *system_tx) override;

#ifdef MG_ENTERPRISE
  void GrantDatabase(const std::string &db_name, const std::string &user_or_role,
                     system::Transaction *system_tx) override;

  void DenyDatabase(const std::string &db_name, const std::string &user_or_role,
                    system::Transaction *system_tx) override;

  void RevokeDatabase(const std::string &db_name, const std::string &user_or_role,
                      system::Transaction *system_tx) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetDatabasePrivileges(
      const std::string &user, const std::vector<std::string> &roles) override;

  void SetMainDatabase(std::string_view db_name, const std::string &user_or_role,
                       system::Transaction *system_tx) override;

  void DeleteDatabase(std::string_view db_name, system::Transaction *system_tx) override;

  std::optional<std::string> GetMainDatabase(const std::string &user_or_role) override;
#endif

  bool CreateRole(const std::string &rolename, system::Transaction *system_tx) override;

  bool DropRole(const std::string &rolename, system::Transaction *system_tx) override;

  bool HasRole(const std::string &rolename) override;

  std::vector<memgraph::query::TypedValue> GetUsernames() override;

  std::vector<memgraph::query::TypedValue> GetRolenames() override;

  std::vector<std::string> GetRolenamesForUser(const std::string &username,
                                               std::optional<std::string> db_name) override;

  std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename) override;

  void SetRoles(const std::string &username, const std::vector<std::string> &roles,
                const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) override;

  void RemoveRole(const std::string &username, const std::string &rolename, system::Transaction *system_tx) override;

  void ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                  system::Transaction *system_tx) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string &user_or_role,
                                                                      std::optional<std::string>) override;

  void GrantPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      system::Transaction *system_tx) override;

  void DenyPrivilege(const std::string &user_or_role,
                     const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                     system::Transaction *system_tx) override;

  void RevokePrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      system::Transaction *system_tx) override;

// User profiles
#ifdef MG_ENTERPRISE
  void CreateProfile(const std::string &profile_name, const query::UserProfileQuery::limits_t &defined_limits,
                     const std::unordered_set<std::string> &usernames, system::Transaction *system_tx) override;
  void UpdateProfile(const std::string &profile_name, const query::UserProfileQuery::limits_t &updated_limits,
                     system::Transaction *system_tx) override;
  void DropProfile(const std::string &profile_name, system::Transaction *system_tx) override;
  query::UserProfileQuery::limits_t GetProfile(std::string_view name) override;
  std::vector<std::pair<std::string, query::UserProfileQuery::limits_t>> AllProfiles() override;
  void SetProfile(const std::string &profile_name, const std::string &user_or_role,
                  system::Transaction *system_tx) override;
  void RevokeProfile(const std::string &user_or_role, system::Transaction *system_tx) override;
  std::optional<std::string> GetProfileForUser(const std::string &user_or_role) override;
  std::vector<std::string> GetUsernamesForProfile(const std::string &profile_name) override;
  std::optional<std::string> GetProfileForRole(const std::string &user_or_role) override;
  std::vector<std::string> GetRolenamesForProfile(const std::string &profile_name) override;
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
      system::Transaction *system_tx);

#ifdef MG_ENTERPRISE
  void GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                            system::Transaction *system_tx) override;
  void DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                           system::Transaction *system_tx) override;
#endif
};
}  // namespace memgraph::glue
