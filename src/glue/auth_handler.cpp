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

#include "glue/auth_handler.hpp"

#include "auth/models.hpp"
#include "glue/auth.hpp"

namespace {

struct PermissionForPrivilegeResult {
  std::string permission;
  memgraph::auth::PermissionLevel permission_level;
  std::string description;
};

struct FineGrainedPermissionForPrivilegeResult {
  std::string permission;
  memgraph::auth::FineGrainedPermission permission_level;
  std::string description;
};

PermissionForPrivilegeResult GetPermissionForPrivilegeForActor(const memgraph::auth::Permissions &permissions,
                                                               const memgraph::query::AuthQuery::Privilege &privilege,
                                                               const std::string &actor) {
  PermissionForPrivilegeResult container;

  const auto permission = memgraph::glue::PrivilegeToPermission(privilege);
  container.permission = memgraph::auth::PermissionToString(permission);
  container.permission_level = permissions.Has(permission);

  switch (container.permission_level) {
    case memgraph::auth::PermissionLevel::GRANT:
      container.description = "GRANTED TO " + actor;
      break;
    case memgraph::auth::PermissionLevel::DENY:
      container.description = "DENIED TO " + actor;
      break;
    case memgraph::auth::PermissionLevel::NEUTRAL:
      container.description = "";
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
    auto user_permission_result = GetPermissionForPrivilegeForActor(permissions, privilege, "USER");
    auto user_only_permissions_result = GetPermissionForPrivilegeForActor(user_level_permissions, privilege, "USER");

    if (user_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
      std::vector<std::string> full_description;
      if (user_only_permissions_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
        full_description.emplace_back(user_only_permissions_result.description);
      }

      if (const auto *role = user->role(); role != nullptr) {
        auto role_permission_result = GetPermissionForPrivilegeForActor(role->permissions(), privilege, "ROLE");
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
    auto role_permission_result = GetPermissionForPrivilegeForActor(permissions, privilege, "ROLE");
    if (role_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
      privilege_results.push_back(role_permission_result);
    }
  }

  return ConstructPrivilegesResult(privilege_results);
}

std::vector<FineGrainedPermissionForPrivilegeResult> GetFineGrainedPermissionForPrivilegeForActor(
    const memgraph::auth::FineGrainedAccessPermissions &permissions, const std::string &permission_type,
    const std::string &actor) {
  std::vector<FineGrainedPermissionForPrivilegeResult> fine_grained_permissions;
  const auto global_permission = permissions.GetGlobalPermission();
  if (global_permission.has_value()) {
    const auto &permission_level = memgraph::auth::PermissionToFineGrainedPermission(global_permission.value());
    const auto &permission_representation = "ALL " + permission_type + "S";
    const auto &permission_level_representation =
        permission_level == memgraph::auth::FineGrainedPermission::NO_PERMISSION ? "DENIED" : "GRANTED";

    std::string permission_description;
    permission_description.append("GLOBAL ");
    permission_description.append(permission_type);
    permission_description.append(" PERMISSION ");
    permission_description.append(permission_level_representation);
    permission_description.append(" TO ");
    permission_description.append(actor);

    fine_grained_permissions.push_back(
        FineGrainedPermissionForPrivilegeResult{permission_representation, permission_level, permission_description});
  }

  for (const auto &permission : permissions.GetPermissions()) {
    const auto label = permission.first;
    auto permission_level = memgraph::auth::PermissionToFineGrainedPermission(permission.second);

    const auto &permission_representation = permission_type + " :" + permission.first;
    const auto &permission_level_representation =
        permission_level == memgraph::auth::FineGrainedPermission::NO_PERMISSION ? "DENIED" : "GRANTED";

    std::string permission_description;
    permission_description.append(permission_type);
    permission_description.append(" PERMISSION ");
    permission_description.append(permission_level_representation);
    permission_description.append(" TO ");
    permission_description.append(actor);

    fine_grained_permissions.push_back(
        FineGrainedPermissionForPrivilegeResult{permission_representation, permission_level, permission_description});
  }

  return fine_grained_permissions;
}

std::vector<std::vector<memgraph::query::TypedValue>> ConstructFineGrainedPrivilegesResult(
    const std::vector<FineGrainedPermissionForPrivilegeResult> &privileges) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;

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
  const auto &label_permissions = user->GetFineGrainedAccessLabelPermissions();
  const auto &edge_type_permissions = user->GetFineGrainedAccessEdgeTypePermissions();

  auto all_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForActor(label_permissions, "LABEL", "USER");
  auto edge_type_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForActor(edge_type_permissions, "EDGE_TYPE", "USER");

  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(), edge_type_fine_grained_permissions.begin(),
                                      edge_type_fine_grained_permissions.end());

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedRolePrivileges(
    const std::optional<memgraph::auth::Role> &role) {
  const auto &label_permissions = role->GetFineGrainedAccessLabelPermissions();
  const auto &edge_type_permissions = role->GetFineGrainedAccessEdgeTypePermissions();

  auto all_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForActor(label_permissions, "LABEL", "USER");
  auto edge_type_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForActor(edge_type_permissions, "EDGE_TYPE", "USER");

  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(), edge_type_fine_grained_permissions.begin(),
                                      edge_type_fine_grained_permissions.end());

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}
}  // namespace

namespace memgraph::glue {

AuthQueryHandler::AuthQueryHandler(
    memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
    std::string name_regex_string)
    : auth_(auth), name_regex_string_(std::move(name_regex_string)), name_regex_(name_regex_string_) {}

bool AuthQueryHandler::CreateUser(const std::string &username, const std::optional<std::string> &password) {
  if (name_regex_string_ != default_user_role_regex) {
    if (const auto license_check_result =
            memgraph::utils::license::global_license_checker.IsValidLicense(memgraph::utils::global_settings);
        license_check_result.HasError()) {
      throw memgraph::auth::AuthException(
          "Custom user/role regex is a Memgraph Enterprise feature. Please set the config "
          "(\"--auth-user-or-role-name-regex\") to its default value (\"{}\") or remove the flag.\n{}",
          default_user_role_regex,
          memgraph::utils::license::LicenseCheckErrorToString(license_check_result.GetError(), "user/role regex"));
    }
  }
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
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
          username, memgraph::query::kPrivilegesAll,
          {{{memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {memgraph::auth::kAsterisk}}}},
          {{{memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {memgraph::auth::kAsterisk}}}});
    }

    return user_added;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::DropUser(const std::string &username) {
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
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
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }
    user->UpdatePassword(password);
    locked_auth->SaveUser(*user);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::CreateRole(const std::string &rolename) {
  if (!std::regex_match(rolename, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid role name.");
  }
  try {
    auto locked_auth = auth_->Lock();
    return locked_auth->AddRole(rolename).has_value();
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::DropRole(const std::string &rolename) {
  if (!std::regex_match(rolename, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid role name.");
  }
  try {
    auto locked_auth = auth_->Lock();
    auto role = locked_auth->GetRole(rolename);
    if (!role) return false;
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
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
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
  if (!std::regex_match(rolename, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid role name.");
  }
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
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
  if (!std::regex_match(rolename, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid role name.");
  }
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
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
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
  if (!std::regex_match(user_or_role, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user or role name.");
  }
  try {
    auto locked_auth = auth_->ReadLock();
    std::vector<std::vector<memgraph::query::TypedValue>> grants;
    std::vector<std::vector<memgraph::query::TypedValue>> fine_grained_grants;
    auto user = locked_auth->GetUser(user_or_role);
    auto role = locked_auth->GetRole(user_or_role);
    if (!user && !role) {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }

    if (user) {
      grants = ShowUserPrivileges(user);
      fine_grained_grants = ShowFineGrainedUserPrivileges(user);
    } else {
      grants = ShowRolePrivileges(role);
      fine_grained_grants = ShowFineGrainedRolePrivileges(role);
    }

    grants.insert(grants.end(), fine_grained_grants.begin(), fine_grained_grants.end());

    return grants;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::GrantPrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges, edge_type_privileges,
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Grant(permission);
      },
      [](auto &fine_grained_permissions, const auto &privilege_collection) {
        for (const auto &[privilege, entities] : privilege_collection) {
          const auto &permission = memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(privilege);
          for (const auto &entity : entities) {
            fine_grained_permissions.Grant(entity, permission);
          }
        }
      });
}

void AuthQueryHandler::DenyPrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges, edge_type_privileges,
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Deny(permission);
      },
      [](auto &fine_grained_permissions, const auto &privilege_collection) {
        for (const auto &[privilege, entities] : privilege_collection) {
          const auto &permission = memgraph::glue::FineGrainedPrivilegeToFineGrainedPermission(privilege);
          for (const auto &entity : entities) {
            fine_grained_permissions.Deny(entity, permission);
          }
        }
      });
}

void AuthQueryHandler::RevokePrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges, edge_type_privileges,
      [](auto &permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions.Revoke(permission);
      },
      [](auto &fine_grained_permissions, const auto &privilege_collection) {
        for ([[maybe_unused]] const auto &[privilege, entities] : privilege_collection) {
          for (const auto &entity : entities) {
            fine_grained_permissions.Revoke(entity);
          }
        }
      });
}

template <class TEditPermissionsFun, class TEditFineGrainedPermissionsFun>
void AuthQueryHandler::EditPermissions(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &label_privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
        &edge_type_privileges,
    const TEditPermissionsFun &edit_permissions_fun,
    const TEditFineGrainedPermissionsFun &edit_fine_grained_permissions_fun) {
  if (!std::regex_match(user_or_role, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user or role name.");
  }
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
      for (const auto &label_privilege_collection : label_privileges) {
        edit_fine_grained_permissions_fun(user->fine_grained_access_handler().label_permissions(),
                                          label_privilege_collection);
      }
      for (const auto &edge_type_privilege_collection : edge_type_privileges) {
        edit_fine_grained_permissions_fun(user->fine_grained_access_handler().edge_type_permissions(),
                                          edge_type_privilege_collection);
      }

      locked_auth->SaveUser(*user);
    } else {
      for (const auto &permission : permissions) {
        edit_permissions_fun(role->permissions(), permission);
      }
      for (const auto &label_privilege : label_privileges) {
        edit_fine_grained_permissions_fun(user->fine_grained_access_handler().label_permissions(), label_privilege);
      }
      for (const auto &edge_type_privilege : edge_type_privileges) {
        edit_fine_grained_permissions_fun(role->fine_grained_access_handler().edge_type_permissions(),
                                          edge_type_privilege);
      }

      locked_auth->SaveRole(*role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

}  // namespace memgraph::glue
