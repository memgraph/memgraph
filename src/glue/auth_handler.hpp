// Copyright 2023 Memgraph Ltd.
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
#include "auth_global.hpp"
#include "glue/auth.hpp"
#include "license/license.hpp"
#include "query/auth_query_handler.hpp"
#include "utils/string.hpp"

namespace memgraph::glue {

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

#ifdef MG_ENTERPRISE
  bool RevokeDatabaseFromUser(const std::string &db, const std::string &username) override;

  bool GrantDatabaseToUser(const std::string &db, const std::string &username) override;

  std::vector<std::vector<memgraph::query::TypedValue>> GetDatabasePrivileges(const std::string &username) override;

  bool SetMainDatabase(const std::string &db, const std::string &username) override;

  void DeleteDatabase(std::string_view db) override;
#endif

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
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,

      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ) override;

  void DenyPrivilege(const std::string &user_or_role,
                     const std::vector<memgraph::query::AuthQuery::Privilege> &privileges) override;

  void RevokePrivilege(
      const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
      ,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &label_privileges,
      const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
          &edge_type_privileges
#endif
      ) override;

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
  );
};
}  // namespace memgraph::glue
