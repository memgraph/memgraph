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

#include "glue/auth.hpp"

namespace {
static std::vector<std::vector<memgraph::query::TypedValue>> ShowUserPrivileges(
    std::optional<memgraph::auth::User> user) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;
  const auto &permissions = user->GetPermissions();

  for (const auto &privilege : memgraph::query::kPrivilegesAll) {
    auto permission = memgraph::glue::PrivilegeToPermission(privilege);
    auto effective = permissions.Has(permission);
    if (permissions.Has(permission) != memgraph::auth::PermissionLevel::NEUTRAL) {
      std::vector<std::string> description;
      auto user_level = user->permissions().Has(permission);
      if (user_level == memgraph::auth::PermissionLevel::GRANT) {
        description.emplace_back("GRANTED TO USER");
      } else if (user_level == memgraph::auth::PermissionLevel::DENY) {
        description.emplace_back("DENIED TO USER");
      }

      if (const auto *role = user->role(); role != nullptr) {
        auto role_level = role->permissions().Has(permission);
        if (role_level == memgraph::auth::PermissionLevel::GRANT) {
          description.emplace_back("GRANTED TO ROLE");
        } else if (role_level == memgraph::auth::PermissionLevel::DENY) {
          description.emplace_back("DENIED TO ROLE");
        }
      }

      grants.push_back({memgraph::query::TypedValue(memgraph::auth::PermissionToString(permission)),
                        memgraph::query::TypedValue(memgraph::auth::PermissionLevelToString(effective)),
                        memgraph::query::TypedValue(memgraph::utils::Join(description, ", "))});
    }
  }
  return grants;
}

static std::vector<std::vector<memgraph::query::TypedValue>> ShowRolePrivileges(
    std::optional<memgraph::auth::Role> role) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;
  const auto &permissions = role->permissions();
  for (const auto &privilege : memgraph::query::kPrivilegesAll) {
    auto permission = memgraph::glue::PrivilegeToPermission(privilege);
    auto effective = permissions.Has(permission);
    if (effective != memgraph::auth::PermissionLevel::NEUTRAL) {
      std::string description;
      if (effective == memgraph::auth::PermissionLevel::GRANT) {
        description = "GRANTED TO ROLE";
      } else if (effective == memgraph::auth::PermissionLevel::DENY) {
        description = "DENIED TO ROLE";
      }
      grants.push_back({memgraph::query::TypedValue(memgraph::auth::PermissionToString(permission)),
                        memgraph::query::TypedValue(memgraph::auth::PermissionLevelToString(effective)),
                        memgraph::query::TypedValue(description)});
    }
  }

  return grants;
}

static std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedUserPrivileges(
    std::optional<memgraph::auth::User> user) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;
  const auto &permissions = user->GetFineGrainedAccessPermissions();

  const auto global_permission = permissions.global_permission();
  if (global_permission.has_value()) {
    auto permission_level = memgraph::auth::PermissionToLabelPermission(global_permission.value());
    grants.push_back({memgraph::query::TypedValue("ALL LABELS"),
                      memgraph::query::TypedValue(memgraph::auth::LabelPermissionToString(permission_level)),
                      memgraph::query::TypedValue("GLOBAL LABEL PERMISSION GRANTED TO USER")});
  }

  for (const auto &permission : permissions.permissions()) {
    const auto label = permission.first;
    auto permission_level = memgraph::auth::PermissionToLabelPermission(permission.second);

    grants.push_back({memgraph::query::TypedValue("LABEL :" + permission.first),
                      memgraph::query::TypedValue(memgraph::auth::LabelPermissionToString(permission_level)),
                      memgraph::query::TypedValue("LABEL PERMISSION GRANTED TO USER")});
  }

  return grants;
}

static std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedRolePrivileges(
    std::optional<memgraph::auth::Role> role) {
  std::vector<std::vector<memgraph::query::TypedValue>> grants;
  const auto &permissions = role->fine_grained_access_permissions();

  const auto global_permission = permissions.global_permission();
  if (global_permission.has_value()) {
    auto permission_level = memgraph::auth::PermissionToLabelPermission(global_permission.value());
    grants.push_back({memgraph::query::TypedValue("ALL LABELS"),
                      memgraph::query::TypedValue(memgraph::auth::LabelPermissionToString(permission_level)),
                      memgraph::query::TypedValue("GLOBAL LABEL PERMISSION GRANTED TO ROLE")});
  }

  for (const auto &permission : permissions.permissions()) {
    const auto label = permission.first;
    auto permission_level = memgraph::auth::PermissionToLabelPermission(permission.second);

    grants.push_back({memgraph::query::TypedValue("LABEL :" + permission.first),
                      memgraph::query::TypedValue(memgraph::auth::LabelPermissionToString(permission_level)),
                      memgraph::query::TypedValue("LABEL PERMISSION GRANTED TO ROLE")});
  }

  return grants;
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
      GrantPrivilege(username, memgraph::query::kPrivilegesAll,
                     {{{memgraph::query::AuthQuery::LabelPrivilege::CREATE_DELETE, {memgraph::auth::kAsterisk}}}});
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

memgraph::auth::User *AuthQueryHandler::GetUser(const std::string &username) {
  if (!std::regex_match(username, name_regex_)) {
    throw memgraph::query::QueryRuntimeException("Invalid user name.");
  }
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist .", username);
    }

    return new memgraph::auth::User(*user);

  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::GrantPrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::LabelPrivilege, std::vector<std::string>>>
        &label_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges,
      [](auto *permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions->Grant(permission);
      },
      [](auto *label_permissions, const auto &label_permission_pair) {
        for (const auto &it : label_permission_pair.second) {
          label_permissions->Grant(it, memgraph::glue::LabelPrivilegeToLabelPermission(label_permission_pair.first));
        }
      });
}

void AuthQueryHandler::DenyPrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::LabelPrivilege, std::vector<std::string>>>
        &label_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges,
      [](auto *permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions->Deny(permission);
      },
      [](auto *label_permissions, const auto &label_permission_pair) {
        for (const auto &it : label_permission_pair.second) {
          label_permissions->Deny(it, memgraph::glue::LabelPrivilegeToLabelPermission(label_permission_pair.first));
        }
      });
}

void AuthQueryHandler::RevokePrivilege(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::LabelPrivilege, std::vector<std::string>>>
        &label_privileges) {
  EditPermissions(
      user_or_role, privileges, label_privileges,
      [](auto *permissions, const auto &permission) {
        // TODO (mferencevic): should we first check that the
        // privilege is granted/denied/revoked before
        // unconditionally granting/denying/revoking it?
        permissions->Revoke(permission);
      },
      [](auto *label_permissions, const auto &label_permission_pair) {
        for (const auto &it : label_permission_pair.second) {
          label_permissions->Revoke(it);
        }
      });
}

template <class TEditFun, class TEditLabelPermisionsFun>
void AuthQueryHandler::EditPermissions(
    const std::string &user_or_role, const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
    const std::vector<std::unordered_map<memgraph::query::AuthQuery::LabelPrivilege, std::vector<std::string>>>
        &label_privileges,
    const TEditFun &edit_fun, const TEditLabelPermisionsFun &edit_label_permisions_fun) {
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
        edit_fun(&user->permissions(), permission);
      }
      for (const auto &label_privilege : label_privileges) {
        for (const auto &it : label_privilege) {
          edit_label_permisions_fun(&user->fine_grained_access_permissions(), it);
        }
      }
      locked_auth->SaveUser(*user);
    } else {
      for (const auto &permission : permissions) {
        edit_fun(&role->permissions(), permission);
      }
      for (const auto &label_privilege : label_privileges) {
        for (const auto &it : label_privilege) {
          edit_label_permisions_fun(&role->fine_grained_access_permissions(), it);
        }
      }

      locked_auth->SaveRole(*role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

}  // namespace memgraph::glue
