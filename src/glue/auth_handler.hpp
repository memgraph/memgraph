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

  // The auth transaction this handler's methods route through, bound for the duration of a single query callback.
  // There is one handler for the process, so this is only safe because an auth query runs on its own session's
  // thread: PrepareAuthQuery produces a PullPlanVector with no plan, so it never reaches the worker pool. If that
  // ever changes, two sessions would clobber this and route each other's writes; the assert in WithTransaction is
  // there to catch it at the point it appears.
  memgraph::auth::AuthTransaction *tx_{nullptr};
  std::thread::id bound_thread_{};

 public:
  explicit AuthQueryHandler(memgraph::auth::SynchedAuth *auth);

  memgraph::auth::AuthTransaction *BindTransaction(memgraph::auth::AuthTransaction *tx) override {
    DMG_ASSERT(tx_ == nullptr || bound_thread_ == std::this_thread::get_id(),
               "Auth transaction bound from two threads: auth queries must run on their own session's thread");
    bound_thread_ = std::this_thread::get_id();
    return std::exchange(tx_, tx);
  }

  query::CreateUserResult CreateUser(const std::string &username, const std::optional<std::string> &password,
                                     system::Transaction *system_tx) override;

  bool DropUser(const std::string &username, system::Transaction *system_tx) override;

  void SetPassword(const std::string &username, const std::optional<std::string> &password,
                   system::Transaction *system_tx) override;

  void ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                      const std::optional<std::string> &newPassword, system::Transaction *system_tx) override;

#ifdef MG_ENTERPRISE
  void GrantDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                     system::Transaction *system_tx) override;

  void DenyDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                    system::Transaction *system_tx) override;

  void RevokeDatabase(const std::string &db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                      system::Transaction *system_tx) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetDatabasePrivileges(const std::string &user,
                                                                              const std::vector<std::string> &roles,
                                                                              auth::UserOrRoleType type) override;

  void SetMainDatabase(std::string_view db_name, const std::string &user_or_role, auth::UserOrRoleType type,
                       system::Transaction *system_tx) override;

  void DeleteDatabase(std::string_view db_name, system::Transaction *system_tx) override;

  std::optional<std::string> GetMainDatabase(const std::string &user_or_role, auth::UserOrRoleType type) override;
#endif

  bool CreateRole(const std::string &rolename, system::Transaction *system_tx) override;

  bool DropRole(const std::string &rolename, system::Transaction *system_tx) override;

  bool HasRole(const std::string &rolename) override;

  std::vector<memgraph::query::TypedValue> GetUsernames() override;

  std::vector<memgraph::query::RolenameResult> GetRolenames() override;

  std::vector<memgraph::query::RolenameResult> GetRolenamesForUser(const std::string &username,
                                                                   std::optional<std::string> db_name) override;

  std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename) override;

  void SetRoles(const std::string &username, const std::vector<std::string> &roles,
                const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) override;

  void RemoveRole(const std::string &username, const std::string &rolename, system::Transaction *system_tx) override;

  void ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                  system::Transaction *system_tx) override;

  void AddRoles(const std::string &username, const std::vector<std::string> &roles,
                const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) override;

  void RevokeRoles(const std::string &username, const std::vector<std::string> &roles,
                   const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) override;

  using query::AuthQueryHandler::GetPrivileges;

  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string &user_or_role,
                                                                      std::optional<std::string>,
                                                                      auth::UserOrRoleType type) override;

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
      auth::UserOrRoleType type, system::Transaction *system_tx) override;

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
      auth::UserOrRoleType type, system::Transaction *system_tx) override;

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
      auth::UserOrRoleType type, system::Transaction *system_tx) override;

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

  void GrantPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                               const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                               auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                               auth::PropertyPermissionType perm_type, system::Transaction *system_tx) override;
  void DenyPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                              const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                              auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                              auth::PropertyPermissionType perm_type, system::Transaction *system_tx) override;
  void RevokePropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                                const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                                auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                                auth::PropertyPermissionType perm_type, system::Transaction *system_tx) override;
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
      auth::UserOrRoleType type, system::Transaction *system_tx);

#ifdef MG_ENTERPRISE
  template <typename EditFn>
  void EditPropertyPermission(const std::string &user_or_role, const std::vector<std::string> &properties,
                              const std::vector<std::string> &entity_names, auth::PropertyEntityKind entity_kind,
                              auth::MatchingMode matching_mode, auth::UserOrRoleType type,
                              system::Transaction *system_tx, EditFn const &edit_fn);
#endif

#ifdef MG_ENTERPRISE
  void GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                            auth::UserOrRoleType type, system::Transaction *system_tx) override;
  void DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                           auth::UserOrRoleType type, system::Transaction *system_tx) override;
#endif

 private:
  /// Locked access, routed through the bound transaction when there is one.
  auto Lock() { return layer_.Lock(tx_); }

  /// Reads inside a transaction take the WRITE lock, not a shared one: installing the overlay mutates Auth's storage
  /// handle, so it cannot be shared with a concurrent reader. The exclusion lasts one statement, not the transaction.
  auto ReadLock() { return layer_.ReadLock(tx_); }
};
}  // namespace memgraph::glue
