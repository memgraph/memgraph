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

#include <regex>

#include "auth/auth.hpp"
#include "glue/auth.hpp"
#include "query/interpreter.hpp"
#include "utils/license.hpp"
#include "utils/string.hpp"

namespace memgraph::glue {

inline constexpr std::string_view kDefaultUserRoleRegex = "[a-zA-Z0-9_.+-@]+";

class AuthQueryHandler final : public memgraph::query::AuthQueryHandler {
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
  std::string name_regex_string_;
  std::regex name_regex_;

 public:
  AuthQueryHandler(memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
                   std::string name_regex_string);

  bool CreateUser(const std::string &username, const std::optional<std::string> &password) override;

  bool DropUser(const std::string &username) override;

  void SetPassword(const std::string &username, const std::optional<std::string> &password) override;

  bool CreateRole(const std::string &rolename) override;

  bool DropRole(const std::string &rolename) override;

  std::vector<memgraph::query::TypedValue> GetUsernames() override;

  std::vector<memgraph::query::TypedValue> GetRolenames() override;

  std::optional<std::string> GetRolenameForUser(const std::string &username) override;

  std::vector<memgraph::query::TypedValue> GetUsernamesForRole(const std::string &rolename) override;

  void SetRole(const std::string &username, const std::string &rolename) override;

  void ClearRole(const std::string &username) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetPrivileges(const std::string &user_or_role) override;

  void GrantPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges) override;

  void DenyPrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges) override;

  void RevokePrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges) override;

 private:
  template <class TEditPermissionsFun, class TEditFineGrainedPermissionsFun>
  void EditPermissions(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges,
      const TEditPermissionsFun &edit_permissions_fun,
      const TEditFineGrainedPermissionsFun &edit_fine_grained_permissions_fun);
};
}  // namespace memgraph::glue
