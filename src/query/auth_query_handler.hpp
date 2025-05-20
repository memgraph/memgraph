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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/ast/query/user_profile.hpp"
#include "query/typed_value.hpp"
#include "system/system.hpp"

namespace memgraph::query {

class AuthQueryHandler {
 public:
  AuthQueryHandler() = default;
  virtual ~AuthQueryHandler() = default;

  AuthQueryHandler(const AuthQueryHandler &) = delete;
  AuthQueryHandler(AuthQueryHandler &&) = delete;
  AuthQueryHandler &operator=(const AuthQueryHandler &) = delete;
  AuthQueryHandler &operator=(AuthQueryHandler &&) = delete;

  /// Return false if the user already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool CreateUser(const std::string &username, const std::optional<std::string> &password,
                          system::Transaction *system_tx) = 0;

  /// Return false if the user does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropUser(const std::string &username, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetPassword(const std::string &username, const std::optional<std::string> &password,
                           system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                              const std::optional<std::string> &newPassword, system::Transaction *system_tx) = 0;

#ifdef MG_ENTERPRISE
  /// Return true if access granted successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantDatabase(const std::string &db, const std::string &username, system::Transaction *system_tx) = 0;

  /// Return true if access revoked successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyDatabase(const std::string &db, const std::string &username, system::Transaction *system_tx) = 0;

  /// Return true if access revoked successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokeDatabase(const std::string &db, const std::string &username, system::Transaction *system_tx) = 0;

  using DatabasePrivileges = std::vector<std::vector<memgraph::query::TypedValue>>;

  /// Returns database access rights for the user
  /// @throw QueryRuntimeException if an error ocurred.
  virtual DatabasePrivileges GetDatabasePrivileges(const std::string &user, const std::vector<std::string> &roles) = 0;
  DatabasePrivileges GetDatabasePrivileges(const std::string &user_or_role) {
    return GetDatabasePrivileges(user_or_role, {user_or_role});
  }

  /// Return true if main database set successfully
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetMainDatabase(std::string_view db, const std::string &username, system::Transaction *system_tx) = 0;

  /// Delete database from all users
  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DeleteDatabase(std::string_view db, system::Transaction *system_tx) = 0;

  /// Get the main database for a user or role
  /// @return Optional database access if user/role exists and has a main database set
  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::optional<std::string> GetMainDatabase(const std::string &user_or_role) = 0;
#endif

  /// Return false if the role already exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool CreateRole(const std::string &rolename, system::Transaction *system_tx) = 0;

  /// Return false if the role does not exist.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool DropRole(const std::string &rolename, system::Transaction *system_tx) = 0;

  /// Return true if the role exists.
  /// @throw QueryRuntimeException if an error ocurred.
  virtual bool HasRole(const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<memgraph::query::TypedValue> GetUsernames() = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<memgraph::query::TypedValue> GetRolenames() = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<std::string> GetRolenamesForUser(const std::string &username,
                                                       std::optional<std::string> db_name) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void SetRoles(const std::string &username, const std::vector<std::string> &roles,
                        const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RemoveRole(const std::string &username, const std::string &rolename, system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                          system::Transaction *system_tx) = 0;

  virtual std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string &user_or_role,
                                                                              std::optional<std::string>) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void GrantPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void DenyPrivilege(const std::string &user_or_role,
                             const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                             system::Transaction *system_tx) = 0;

  /// @throw QueryRuntimeException if an error ocurred.
  virtual void RevokePrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ,
      system::Transaction *system_tx) = 0;

#ifdef MG_ENTERPRISE
  virtual void GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                    system::Transaction *system_tx) = 0;
  virtual void DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                   system::Transaction *system_tx) = 0;
#endif

// User profiles
#ifdef MG_ENTERPRISE
  virtual void CreateProfile(const std::string &profile_name, const UserProfileQuery::limits_t &defined_limits,
                             system::Transaction *system_tx) = 0;
  virtual void UpdateProfile(const std::string &profile_name, const UserProfileQuery::limits_t &updated_limits,
                             system::Transaction *system_tx) = 0;
  virtual void DropProfile(const std::string &profile_name, system::Transaction *system_tx) = 0;
  virtual UserProfileQuery::limits_t GetProfile(std::string_view name) = 0;
  virtual std::vector<std::pair<std::string, UserProfileQuery::limits_t>> AllProfiles() = 0;
  virtual void SetProfile(const std::string &profile_name, const std::string &user_or_role,
                          system::Transaction *system_tx) = 0;
  virtual void RevokeProfile(const std::string &user_or_role, system::Transaction *system_tx) = 0;
  virtual std::optional<std::string> GetProfileForUser(const std::string &user_or_role) = 0;
  virtual std::vector<std::string> GetUsersForProfile(const std::string &profile_name) = 0;
#endif
};

}  // namespace memgraph::query
