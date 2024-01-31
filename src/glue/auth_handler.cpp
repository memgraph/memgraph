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

#include "glue/auth_handler.hpp"

#include <sstream>

#include <fmt/format.h>

#include "auth/models.hpp"
#include "dbms/constants.hpp"
#include "glue/auth.hpp"
#include "license/license.hpp"
#include "query/constants.hpp"

namespace {

struct PermissionForPrivilegeResult {
  std::string permission;
  memgraph::auth::PermissionLevel permission_level;
  std::string description;
};

struct FineGrainedPermissionForPrivilegeResult {
  std::string permission;
#ifdef MG_ENTERPRISE
  memgraph::auth::FineGrainedPermission permission_level;
#endif
  std::string description;
};

PermissionForPrivilegeResult GetPermissionForPrivilegeForUserOrRole(
    const memgraph::auth::Permissions &permissions, const memgraph::query::AuthQuery::Privilege &privilege,
    const std::string &user_or_role) {
  PermissionForPrivilegeResult container;

  const auto permission = memgraph::glue::PrivilegeToPermission(privilege);
  container.permission = memgraph::auth::PermissionToString(permission);
  container.permission_level = permissions.Has(permission);

  switch (container.permission_level) {
    case memgraph::auth::PermissionLevel::GRANT:
      container.description = "GRANTED TO " + user_or_role;
      break;
    case memgraph::auth::PermissionLevel::DENY:
      container.description = "DENIED TO " + user_or_role;
      break;
    case memgraph::auth::PermissionLevel::NEUTRAL:
      break;
  }

  return container;
}

std::vector<std::vector<memgraph::query::TypedValue>> ConstructPrivilegesResult(
    const std::vector<PermissionForPrivilegeResult> &privileges) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;

  grants.reserve(privileges.size());
  for (const auto &permission : privileges) {
    grants.push_back({memgraph::query::TypedValue(permission.permission),
                      memgraph::query::TypedValue(memgraph::auth::PermissionLevelToString(permission.permission_level)),
                      memgraph::query::TypedValue(permission.description)});
  }

  return grants;
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowUserPrivileges(
    const std::optional<memgraph::auth::User> &user) {
  std::vector<PermissionForPrivilegeResult> privilege_results;

  const auto &permissions = user->GetPermissions();
  const auto &user_level_permissions = user->permissions();

  for (const auto &privilege : memgraph::query::kPrivilegesAll) {
    auto user_permission_result = GetPermissionForPrivilegeForUserOrRole(permissions, privilege, "USER");
    auto user_only_permissions_result =
        GetPermissionForPrivilegeForUserOrRole(user_level_permissions, privilege, "USER");

    if (user_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
      std::vector<std::string> full_description;
      if (user_only_permissions_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
        full_description.emplace_back(user_only_permissions_result.description);
      }

      if (const auto *role = user->role(); role != nullptr) {
        auto role_permission_result = GetPermissionForPrivilegeForUserOrRole(role->permissions(), privilege, "ROLE");
        if (role_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
          full_description.emplace_back(role_permission_result.description);
        }
      }

      privilege_results.push_back(PermissionForPrivilegeResult{user_permission_result.permission,
                                                               user_permission_result.permission_level,
                                                               memgraph::utils::Join(full_description, ", ")});
    }
  }

  return ConstructPrivilegesResult(privilege_results);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowRolePrivileges(
    const std::optional<memgraph::auth::Role> &role) {
  std::vector<PermissionForPrivilegeResult> privilege_results;
  const auto &permissions = role->permissions();
  for (const auto &privilege : memgraph::query::kPrivilegesAll) {
    auto role_permission_result = GetPermissionForPrivilegeForUserOrRole(permissions, privilege, "ROLE");
    if (role_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
      privilege_results.push_back(role_permission_result);
    }
  }

  return ConstructPrivilegesResult(privilege_results);
}

#ifdef MG_ENTERPRISE
std::vector<std::vector<memgraph::query::TypedValue>> ShowDatabasePrivileges(
    const std::optional<memgraph::auth::User> &user) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast() || !user) {
    return {};
  }

  const auto &db = user->db_access();
  const auto &allows = db.GetAllowAll();
  const auto &grants = db.GetGrants();
  const auto &denies = db.GetDenies();

  std::vector<memgraph::query::TypedValue> res;  // First element is a list of granted databases, second of revoked ones
  if (allows) {
    res.emplace_back("*");
  } else {
    std::vector<memgraph::query::TypedValue> grants_vec(grants.cbegin(), grants.cend());
    res.emplace_back(std::move(grants_vec));
  }
  std::vector<memgraph::query::TypedValue> denies_vec(denies.cbegin(), denies.cend());
  res.emplace_back(std::move(denies_vec));
  return {res};
}

std::vector<FineGrainedPermissionForPrivilegeResult> GetFineGrainedPermissionForPrivilegeForUserOrRole(
    const memgraph::auth::FineGrainedAccessPermissions &permissions, const std::string &permission_type,
    const std::string &user_or_role) {
  std::vector<FineGrainedPermissionForPrivilegeResult> fine_grained_permissions;
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return fine_grained_permissions;
  }
  const auto global_permission = permissions.GetGlobalPermission();
  if (global_permission.has_value()) {
    const auto &permission_level = memgraph::auth::PermissionToFineGrainedPermission(global_permission.value());

    std::stringstream permission_representation;
    permission_representation << "ALL " << permission_type << "S";
    const auto &permission_level_representation =
        permission_level == memgraph::auth::FineGrainedPermission::NOTHING ? "DENIED" : "GRANTED";

    const auto permission_description =
        fmt::format("GLOBAL {0} PERMISSION {1} TO {2}", permission_type, permission_level_representation, user_or_role);

    fine_grained_permissions.push_back(FineGrainedPermissionForPrivilegeResult{
        permission_representation.str(), permission_level, permission_description});
  }

  for (const auto &[label, permission] : permissions.GetPermissions()) {
    auto permission_level = memgraph::auth::PermissionToFineGrainedPermission(permission);

    std::stringstream permission_representation;
    permission_representation << permission_type << " :" << label;

    const auto &permission_level_representation =
        permission_level == memgraph::auth::FineGrainedPermission::NOTHING ? "DENIED" : "GRANTED";

    const auto permission_description =
        fmt::format("{0} PERMISSION {1} TO {2}", permission_type, permission_level_representation, user_or_role);

    fine_grained_permissions.push_back(FineGrainedPermissionForPrivilegeResult{
        permission_representation.str(), permission_level, permission_description});
  }

  return fine_grained_permissions;
}

std::vector<std::vector<memgraph::query::TypedValue>> ConstructFineGrainedPrivilegesResult(
    const std::vector<FineGrainedPermissionForPrivilegeResult> &privileges) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  grants.reserve(privileges.size());
  for (const auto &permission : privileges) {
    grants.push_back(
        {memgraph::query::TypedValue(permission.permission),
         memgraph::query::TypedValue(memgraph::auth::FineGrainedPermissionToString(permission.permission_level)),
         memgraph::query::TypedValue(permission.description)});
  }

  return grants;
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedUserPrivileges(
    const std::optional<memgraph::auth::User> &user) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  const auto &label_permissions = user->GetFineGrainedAccessLabelPermissions();
  const auto &edge_type_permissions = user->GetFineGrainedAccessEdgeTypePermissions();

  auto all_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(label_permissions, "LABEL", "USER");
  auto edge_type_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(edge_type_permissions, "EDGE_TYPE", "USER");

  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(), edge_type_fine_grained_permissions.begin(),
                                      edge_type_fine_grained_permissions.end());

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedRolePrivileges(
    const std::optional<memgraph::auth::Role> &role) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  const auto &label_permissions = role->GetFineGrainedAccessLabelPermissions();
  const auto &edge_type_permissions = role->GetFineGrainedAccessEdgeTypePermissions();

  auto all_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(label_permissions, "LABEL", "USER");
  auto edge_type_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(edge_type_permissions, "EDGE_TYPE", "USER");

  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(), edge_type_fine_grained_permissions.begin(),
                                      edge_type_fine_grained_permissions.end());

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}
#endif

}  // namespace

namespace memgraph::glue {

AuthQueryHandler::AuthQueryHandler(
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth)
    : auth_(auth) {}

bool AuthQueryHandler::CreateUser(const std::string &username, const std::optional<std::string> &password) {
  try {
    const auto [first_user, user_added] = std::invoke([&, this] {
      auto locked_auth = auth_->Lock();
      const auto first_user = !locked_auth->HasUsers();
      const auto user_added = locked_auth->AddUser(username, password).has_value();
      return std::make_pair(first_user, user_added);
    });

    if (first_user) {
      spdlog::info("{} is first created user. Granting all privileges.", username);
      GrantPrivilege(
          username, memgraph::query::kPrivilegesAll
#ifdef MG_ENTERPRISE
          ,
          {{{memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {memgraph::query::kAsterisk}}}},
          {
            {
              {
                memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, { memgraph::query::kAsterisk }
              }
            }
          }
#endif
      );
#ifdef MG_ENTERPRISE
      GrantDatabaseToUser(auth::kAllDatabases, username);
      SetMainDatabase(dbms::kDefaultDB, username);
#endif
    }

    return user_added;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::DropUser(const std::string &username) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) return false;
    return locked_auth->RemoveUser(username);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::SetPassword(const std::string &username, const std::optional<std::string> &password) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }
    locked_auth->UpdatePassword(*user, password);
    locked_auth->SaveUser(*user);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::CreateRole(const std::string &rolename) {
  try {
    auto locked_auth = auth_->Lock();
    return locked_auth->AddRole(rolename).has_value();
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

#ifdef MG_ENTERPRISE
bool AuthQueryHandler::RevokeDatabaseFromUser(const std::string &db, const std::string &username) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) return false;
    return locked_auth->RevokeDatabaseFromUser(db, username);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::GrantDatabaseToUser(const std::string &db, const std::string &username) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) return false;
    return locked_auth->GrantDatabaseToUser(db, username);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<std::vector<memgraph::query::TypedValue>> AuthQueryHandler::GetDatabasePrivileges(
    const std::string &username) {
  try {
    auto locked_auth = auth_->ReadLock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }
    return ShowDatabasePrivileges(user);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::SetMainDatabase(std::string_view db, const std::string &username) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) return false;
    return locked_auth->SetMainDatabase(db, username);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::DeleteDatabase(std::string_view db) {
  try {
    auth_->Lock()->DeleteDatabase(std::string(db));
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}
#endif

bool AuthQueryHandler::DropRole(const std::string &rolename) {
  try {
    auto locked_auth = auth_->Lock();
    auto role = locked_auth->GetRole(rolename);

    if (!role) {
      return false;
    };

    return locked_auth->RemoveRole(rolename);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<memgraph::query::TypedValue> AuthQueryHandler::GetUsernames() {
  try {
    auto locked_auth = auth_->ReadLock();
    std::vector<memgraph::query::TypedValue> usernames;
    const auto &users = locked_auth->AllUsers();
    usernames.reserve(users.size());
    for (const auto &user : users) {
      usernames.emplace_back(user.username());
    }
    return usernames;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<memgraph::query::TypedValue> AuthQueryHandler::GetRolenames() {
  try {
    auto locked_auth = auth_->ReadLock();
    std::vector<memgraph::query::TypedValue> rolenames;
    const auto &roles = locked_auth->AllRoles();
    rolenames.reserve(roles.size());
    for (const auto &role : roles) {
      rolenames.emplace_back(role.rolename());
    }
    return rolenames;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::optional<std::string> AuthQueryHandler::GetRolenameForUser(const std::string &username) {
  try {
    auto locked_auth = auth_->ReadLock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", username);
    }

    if (const auto *role = user->role(); role != nullptr) {
      return role->rolename();
    }
    return std::nullopt;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<memgraph::query::TypedValue> AuthQueryHandler::GetUsernamesForRole(const std::string &rolename) {
  try {
    auto locked_auth = auth_->ReadLock();
    auto role = locked_auth->GetRole(rolename);
    if (!role) {
      throw memgraph::query::QueryRuntimeException("Role '{}' doesn't exist.", rolename);
    }
    std::vector<memgraph::query::TypedValue> usernames;
    const auto &users = locked_auth->AllUsersForRole(rolename);
    usernames.reserve(users.size());
    for (const auto &user : users) {
      usernames.emplace_back(user.username());
    }
    return usernames;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::SetRole(const std::string &username, const std::string &rolename) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", username);
    }
    auto role = locked_auth->GetRole(rolename);
    if (!role) {
      throw memgraph::query::QueryRuntimeException("Role '{}' doesn't exist .", rolename);
    }
    if (const auto *current_role = user->role(); current_role != nullptr) {
      throw memgraph::query::QueryRuntimeException("User '{}' is already a member of role '{}'.", username,
                                                   current_role->rolename());
    }
    user->SetRole(*role);
    locked_auth->SaveUser(*user);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::ClearRole(const std::string &username) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", username);
    }
    user->ClearRole();
    locked_auth->SaveUser(*user);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<std::vector<memgraph::query::TypedValue>> AuthQueryHandler::GetPrivileges(const std::string &user_or_role) {
  try {
    auto locked_auth = auth_->ReadLock();
    std::vector<std::vector<memgraph::query::TypedValue>> grants;
#ifdef MG_ENTERPRISE
    std::vector<std::vector<memgraph::query::TypedValue>> fine_grained_grants;
#endif
    auto user = locked_auth->GetUser(user_or_role);
    auto role = locked_auth->GetRole(user_or_role);
    if (!user && !role) {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }

    if (user) {
      grants = ShowUserPrivileges(user);
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        fine_grained_grants = ShowFineGrainedUserPrivileges(user);
      }
#endif
    } else {
      grants = ShowRolePrivileges(role);
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        fine_grained_grants = ShowFineGrainedRolePrivileges(role);
      }
#endif
    }
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      grants.insert(grants.end(), fine_grained_grants.begin(), fine_grained_grants.end());
    }
#endif
    return grants;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::GrantPrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
    ,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges
#endif
) {
  EditPermissions(
      user_or_role, privileges,
#ifdef MG_ENTERPRISE
      label_privileges, edge_type_privileges,
#endif
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Grant(permission);
      }
#ifdef MG_ENTERPRISE
      ,
      [](auto &fine_grained_permissions, const auto &privilege_collection) {
        for (const auto &[privilege, entities] : privilege_collection) {
          const auto &permission = memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(privilege);
          for (const auto &entity : entities) {
            fine_grained_permissions.Grant(entity, permission);
          }
        }
      }
#endif
  );
}  // namespace memgraph::glue

void AuthQueryHandler::DenyPrivilege(const std::string &user_or_role,
                                     const std::vector<memgraph::query::AuthQuery::Privilege> &privileges) {
  EditPermissions(
      user_or_role, privileges,
#ifdef MG_ENTERPRISE
      {}, {},
#endif
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Deny(permission);
      }
#ifdef MG_ENTERPRISE
      ,
      [](auto &fine_grained_permissions, const auto &privilege_collection) {}
#endif
  );
}

void AuthQueryHandler::RevokePrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges
#ifdef MG_ENTERPRISE
    ,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges
#endif
) {
  EditPermissions(
      user_or_role, privileges,
#ifdef MG_ENTERPRISE
      label_privileges, edge_type_privileges,
#endif
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Revoke(permission);
      }
#ifdef MG_ENTERPRISE
      ,
      [](auto &fine_grained_permissions, const auto &privilege_collection) {
        for ([[maybe_unused]] const auto &[privilege, entities] : privilege_collection) {
          for (const auto &entity : entities) {
            fine_grained_permissions.Revoke(entity);
          }
        }
      }
#endif
  );
}  // namespace memgraph::glue

template <class TEditPermissionsFun
#ifdef MG_ENTERPRISE
          ,
          class TEditFineGrainedPermissionsFun
#endif
          >
void AuthQueryHandler::EditPermissions(
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
) {
  try {
    std::vector<memgraph::auth::Permission> permissions;
    permissions.reserve(privileges.size());
    for (const auto &privilege : privileges) {
      permissions.push_back(memgraph::glue::PrivilegeToPermission(privilege));
    }
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(user_or_role);
    auto role = locked_auth->GetRole(user_or_role);
    if (!user && !role) {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }
    if (user) {
      for (const auto &permission : permissions) {
        edit_permissions_fun(user->permissions(), permission);
      }
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        for (const auto &label_privilege_collection : label_privileges) {
          edit_fine_grained_permissions_fun(user->fine_grained_access_handler().label_permissions(),
                                            label_privilege_collection);
        }
        for (const auto &edge_type_privilege_collection : edge_type_privileges) {
          edit_fine_grained_permissions_fun(user->fine_grained_access_handler().edge_type_permissions(),
                                            edge_type_privilege_collection);
        }
      }
#endif
      locked_auth->SaveUser(*user);
    } else {
      for (const auto &permission : permissions) {
        edit_permissions_fun(role->permissions(), permission);
      }
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        for (const auto &label_privilege : label_privileges) {
          edit_fine_grained_permissions_fun(role->fine_grained_access_handler().label_permissions(), label_privilege);
        }
        for (const auto &edge_type_privilege : edge_type_privileges) {
          edit_fine_grained_permissions_fun(role->fine_grained_access_handler().edge_type_permissions(),
                                            edge_type_privilege);
        }
      }
#endif
      locked_auth->SaveRole(*role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

}  // namespace memgraph::glue
