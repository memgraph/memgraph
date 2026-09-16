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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "auth/models.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/ast/query/user_profile.hpp"
#include "query/typed_value.hpp"
#include "system/system.hpp"
#include "utils/resource_monitoring.hpp"

namespace memgraph::auth {
class AuthTransaction;
}  // namespace memgraph::auth

namespace memgraph::query {

struct CreateUserResult {
  bool created{false};
  bool first_user{false};
  bool builtin_roles_created{false};
};

struct RolenameResult {
  std::string name;
  bool is_builtin{false};
};

class AuthQueryHandler {
 public:
  AuthQueryHandler() = default;
  virtual ~AuthQueryHandler() = default;

  AuthQueryHandler(const AuthQueryHandler &) = delete;
  AuthQueryHandler(AuthQueryHandler &&) = delete;
  AuthQueryHandler &operator=(const AuthQueryHandler &) = delete;
  AuthQueryHandler &operator=(AuthQueryHandler &&) = delete;

  /// Flush an auth transaction, moving its replication actions into `system_tx` for the caller to commit. Returns
  /// false on conflict, leaving durable storage untouched.
  [[nodiscard]] virtual bool CommitTransaction(auth::AuthTransaction &tx, system::Transaction *system_tx) = 0;

  /// Return created=false if the user already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual CreateUserResult CreateUser(const std::string &username, const std::optional<std::string> &password,
                                      auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// Return false if the user does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropUser(const std::string &username, auth::AuthTransaction *auth_tx,
                        system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetPassword(const std::string &username, const std::optional<std::string> &password,
                           auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                              const std::optional<std::string> &newPassword, auth::AuthTransaction *auth_tx,
                              system::Transaction *system_tx) = 0;

#ifdef MG_ENTERPRISE
  /// Return true if access granted successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantDatabase(const std::string &db, const std::string &username, auth::UserOrRoleType type,
                             auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// Return true if access revoked successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyDatabase(const std::string &db, const std::string &username, auth::UserOrRoleType type,
                            auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// Return true if access revoked successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokeDatabase(const std::string &db, const std::string &username, auth::UserOrRoleType type,
                              auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  using DatabasePrivileges = std::vector<std::vector<memgraph::query::TypedValue>>;

  /// Returns database access rights for the user or role.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual DatabasePrivileges GetDatabasePrivileges(const std::string &user, const std::vector<std::string> &roles,
                                                   auth::UserOrRoleType type,
                                                   memgraph::auth::AuthTransaction *auth_tx) = 0;

  DatabasePrivileges GetDatabasePrivileges(const std::string &user, const std::vector<std::string> &roles,
                                           memgraph::auth::AuthTransaction *auth_tx) {
    return GetDatabasePrivileges(user, roles, auth::UserOrRoleType::UNSPECIFIED, auth_tx);
  }

  DatabasePrivileges GetDatabasePrivileges(const std::string &user_or_role, memgraph::auth::AuthTransaction *auth_tx) {
    return GetDatabasePrivileges(user_or_role, {user_or_role}, auth::UserOrRoleType::UNSPECIFIED, auth_tx);
  }

  DatabasePrivileges GetDatabasePrivileges(const std::string &user_or_role, auth::UserOrRoleType type,
                                           memgraph::auth::AuthTransaction *auth_tx) {
    return GetDatabasePrivileges(user_or_role, {user_or_role}, type, auth_tx);
  }

  /// Return true if main database set successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetMainDatabase(std::string_view db, const std::string &username, auth::UserOrRoleType type,
                               auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// Delete database from all users
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DeleteDatabase(std::string_view db, auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// Get the main database for a user or role
  /// @return Optional database access if user/role exists and has a main database set
  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::optional<std::string> GetMainDatabase(const std::string &user_or_role, auth::UserOrRoleType type,
                                                     memgraph::auth::AuthTransaction *auth_tx) = 0;

  std::optional<std::string> GetMainDatabase(const std::string &user_or_role,
                                             memgraph::auth::AuthTransaction *auth_tx) {
    return GetMainDatabase(user_or_role, auth::UserOrRoleType::UNSPECIFIED, auth_tx);
  }
#endif

  /// Return false if the role already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool CreateRole(const std::string &rolename, auth::AuthTransaction *auth_tx,
                          system::Transaction *system_tx) = 0;

  /// Return false if the role does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropRole(const std::string &rolename, auth::AuthTransaction *auth_tx,
                        system::Transaction *system_tx) = 0;

  /// Return true if the role exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool HasRole(const std::string &rolename, memgraph::auth::AuthTransaction *auth_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<memgraph::query::TypedValue> GetUsernames(memgraph::auth::AuthTransaction *auth_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<RolenameResult> GetRolenames(memgraph::auth::AuthTransaction *auth_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<RolenameResult> GetRolenamesForUser(const std::string &username,
                                                          std::optional<std::string> db_name,
                                                          memgraph::auth::AuthTransaction *auth_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename,
                                                                       memgraph::auth::AuthTransaction *auth_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetRoles(const std::string &username, const std::vector<std::string> &roles,
                        const std::unordered_set<std::string> &role_databases, auth::AuthTransaction *auth_tx,
                        system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RemoveRole(const std::string &username, const std::string &rolename, auth::AuthTransaction *auth_tx,
                          system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                          auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void AddRoles(const std::string &username, const std::vector<std::string> &roles,
                        const std::unordered_set<std::string> &role_databases, auth::AuthTransaction *auth_tx,
                        system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokeRoles(const std::string &username, const std::vector<std::string> &roles,
                           const std::unordered_set<std::string> &role_databases, auth::AuthTransaction *auth_tx,
                           system::Transaction *system_tx) = 0;

  virtual std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(
      const std::string &user_or_role, std::optional<std::string> db, auth::UserOrRoleType type,
      memgraph::auth::AuthTransaction *auth_tx) = 0;

  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string &user_or_role,
                                                                      std::optional<std::string> db,
                                                                      memgraph::auth::AuthTransaction *auth_tx) {
    return GetPrivileges(user_or_role, db, auth::UserOrRoleType::UNSPECIFIED, auth_tx);
  }

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantPrivilege(
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
      auth::UserOrRoleType type, auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyPrivilege(
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
      auth::UserOrRoleType type, auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokePrivilege(
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
      auth::UserOrRoleType type, auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;

#ifdef MG_ENTERPRISE
  virtual void GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                    auth::UserOrRoleType type, auth::AuthTransaction *auth_tx,
                                    system::Transaction *system_tx) = 0;
  virtual void DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                   auth::UserOrRoleType type, auth::AuthTransaction *auth_tx,
                                   system::Transaction *system_tx) = 0;

  virtual void GrantPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                                       const std::vector<std::string> &entity_names,
                                       auth::PropertyEntityKind entity_kind, auth::MatchingMode matching_mode,
                                       auth::UserOrRoleType type, auth::PropertyPermissionType perm_type,
                                       auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;
  virtual void DenyPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                                      const std::vector<std::string> &entity_names,
                                      auth::PropertyEntityKind entity_kind, auth::MatchingMode matching_mode,
                                      auth::UserOrRoleType type, auth::PropertyPermissionType perm_type,
                                      auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;
  virtual void RevokePropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                                        const std::vector<std::string> &entity_names,
                                        auth::PropertyEntityKind entity_kind, auth::MatchingMode matching_mode,
                                        auth::UserOrRoleType type, auth::PropertyPermissionType perm_type,
                                        auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;
#endif

// User profiles
#ifdef MG_ENTERPRISE
  virtual void CreateProfile(const std::string &profile_name, const UserProfileQuery::limits_t &defined_limits,
                             const std::unordered_set<std::string> &usernames, auth::AuthTransaction *auth_tx,
                             system::Transaction *system_tx) = 0;
  virtual void UpdateProfile(const std::string &profile_name, const UserProfileQuery::limits_t &updated_limits,
                             auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;
  virtual void DropProfile(const std::string &profile_name, auth::AuthTransaction *auth_tx,
                           system::Transaction *system_tx) = 0;
  virtual UserProfileQuery::limits_t GetProfile(std::string_view name, auth::AuthTransaction *auth_tx) = 0;
  virtual std::vector<std::pair<std::string, UserProfileQuery::limits_t>> AllProfiles(
      memgraph::auth::AuthTransaction *auth_tx) = 0;
  virtual void SetProfile(const std::string &profile_name, const std::string &user_or_role,
                          auth::AuthTransaction *auth_tx, system::Transaction *system_tx) = 0;
  virtual void RevokeProfile(const std::string &user_or_role, auth::AuthTransaction *auth_tx,
                             system::Transaction *system_tx) = 0;
  virtual std::optional<std::string> GetProfileForUser(const std::string &user_or_role,
                                                       memgraph::auth::AuthTransaction *auth_tx) = 0;
  virtual std::vector<std::string> GetUsernamesForProfile(const std::string &profile_name,
                                                          auth::AuthTransaction *auth_tx) = 0;
  // Role-based profile management is no longer supported
  virtual std::optional<std::string> GetProfileForRole(const std::string &user_or_role) = 0;
  virtual std::vector<std::string> GetRolenamesForProfile(const std::string &profile_name) = 0;
#endif
};

}  // namespace memgraph::query
