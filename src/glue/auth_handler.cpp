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

#include "glue/auth_handler.hpp"

#include <optional>
#include <sstream>

#include <fmt/format.h>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "dbms/constants.hpp"
#include "frontend/ast/ast_visitor.hpp"
#include "glue/auth.hpp"
#include "license/license.hpp"
#include "query/constants.hpp"
#include "query/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/variant_helpers.hpp"

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
    const std::optional<memgraph::auth::User> &user, std::optional<std::string_view> db_name) {
  std::vector<PermissionForPrivilegeResult> privilege_results;

  const auto &permissions = user->GetPermissions(db_name);
  memgraph::auth::Permissions user_level_permissions =
#ifdef MG_ENTERPRISE
      (!db_name || user->has_access(db_name.value())) ? user->permissions() : memgraph::auth::Permissions{};
#else
      user->permissions();
#endif
  const auto &roles_permissions = user->roles().GetPermissions(db_name);

  for (const auto &privilege : memgraph::query::kPrivilegesAll) {
    auto user_permission_result = GetPermissionForPrivilegeForUserOrRole(permissions, privilege, "USER");
    auto user_only_permissions_result =
        GetPermissionForPrivilegeForUserOrRole(user_level_permissions, privilege, "USER");
    auto role_permission_result = GetPermissionForPrivilegeForUserOrRole(roles_permissions, privilege, "ROLE");

    if (user_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
      std::vector<std::string> full_description;
      if (user_only_permissions_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
        full_description.emplace_back(user_only_permissions_result.description);
      }
      if (role_permission_result.permission_level != memgraph::auth::PermissionLevel::NEUTRAL) {
        full_description.emplace_back(role_permission_result.description);
      }
      privilege_results.push_back(PermissionForPrivilegeResult{user_permission_result.permission,
                                                               user_permission_result.permission_level,
                                                               memgraph::utils::Join(full_description, ", ")});
    }
  }

  return ConstructPrivilegesResult(privilege_results);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowRolePrivileges(
    const std::optional<memgraph::auth::Role> &role, std::optional<std::string_view> db_name = std::nullopt) {
  std::vector<PermissionForPrivilegeResult> privilege_results;
#ifdef MG_ENTERPRISE
  const auto &permissions = role->GetPermissions(db_name);
#else
  const auto &permissions = role->permissions();
#endif
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
    const std::optional<memgraph::auth::User> &user, const std::optional<memgraph::auth::Roles> &roles) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast() || (!user && !roles)) {
    return {};
  }

  bool allows = false;
  std::set<std::string> grants;
  std::set<std::string> denies;

  const memgraph::auth::Roles *roles_obj = roles ? &*roles : nullptr;

  if (user) {
    const auto &db = user->db_access();
    allows |= db.GetAllowAll();
    grants.insert(db.GetGrants().begin(), db.GetGrants().end());
    denies.insert(db.GetDenies().begin(), db.GetDenies().end());
    roles_obj = &user->roles();
  }
  if (roles_obj) {
    for (const auto &role : *roles_obj) {
      const auto &role_db = role.db_access();
      allows |= role_db.GetAllowAll();
      grants.insert(role_db.GetGrants().begin(), role_db.GetGrants().end());
      denies.insert(role_db.GetDenies().begin(), role_db.GetDenies().end());
    }
  }

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

std::vector<std::vector<memgraph::query::TypedValue>> ShowDatabasePrivileges(
    const std::optional<memgraph::auth::User> &user) {
  return ShowDatabasePrivileges(user, std::nullopt);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowDatabasePrivileges(
    const std::optional<memgraph::auth::Roles> &roles) {
  return ShowDatabasePrivileges(std::nullopt, roles);
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

    std::string permission_description =
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

    std::string permission_description =
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
    const std::optional<memgraph::auth::User> &user, std::optional<std::string_view> db_name = std::nullopt) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }

  auto all_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForUserOrRole(
      user->GetUserFineGrainedAccessLabelPermissions(db_name), "LABEL", "USER");
  auto all_role_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForUserOrRole(
      user->GetRoleFineGrainedAccessLabelPermissions(db_name), "LABEL", "ROLE");
  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(),
                                      std::make_move_iterator(all_role_fine_grained_permissions.begin()),
                                      std::make_move_iterator(all_role_fine_grained_permissions.end()));

  auto edge_type_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForUserOrRole(
      user->GetUserFineGrainedAccessEdgeTypePermissions(db_name), "EDGE_TYPE", "USER");
  auto role_edge_type_fine_grained_permissions = GetFineGrainedPermissionForPrivilegeForUserOrRole(
      user->GetRoleFineGrainedAccessEdgeTypePermissions(db_name), "EDGE_TYPE", "ROLE");
  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(),
                                      std::make_move_iterator(edge_type_fine_grained_permissions.begin()),
                                      std::make_move_iterator(edge_type_fine_grained_permissions.end()));
  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(),
                                      std::make_move_iterator(role_edge_type_fine_grained_permissions.begin()),
                                      std::make_move_iterator(role_edge_type_fine_grained_permissions.end()));

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}

std::vector<std::vector<memgraph::query::TypedValue>> ShowFineGrainedRolePrivileges(
    const std::optional<memgraph::auth::Role> &role, std::optional<std::string_view> db_name = std::nullopt) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  const auto &label_permissions = role->GetFineGrainedAccessLabelPermissions(db_name);
  const auto &edge_type_permissions = role->GetFineGrainedAccessEdgeTypePermissions(db_name);

  auto all_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(label_permissions, "LABEL", "ROLE");
  auto edge_type_fine_grained_permissions =
      GetFineGrainedPermissionForPrivilegeForUserOrRole(edge_type_permissions, "EDGE_TYPE", "ROLE");

  all_fine_grained_permissions.insert(all_fine_grained_permissions.end(), edge_type_fine_grained_permissions.begin(),
                                      edge_type_fine_grained_permissions.end());

  return ConstructFineGrainedPrivilegesResult(all_fine_grained_permissions);
}

// Converting values from query to user profile framework
memgraph::auth::UserProfiles::Limits name_to_limit(const auto &name) {
  uint8_t enum_i = 0;
  for (const auto &limit : memgraph::auth::UserProfiles::kLimits) {
    if (name == limit) break;
    ++enum_i;
  }
  if (enum_i == memgraph::auth::UserProfiles::kLimits.size()) {
    throw memgraph::query::QueryRuntimeException("Unknown limit '{}'. Currently implemented limits: {}", name,
                                                 memgraph::auth::UserProfiles::AllLimits());
  }
  return memgraph::auth::UserProfiles::Limits{enum_i};
}

void is_limit_supported(memgraph::query::UserProfileQuery::LimitValueResult::Type value_type,
                        memgraph::auth::UserProfiles::Limits limit_type) {
  // Unlimited is always supported
  if (value_type == memgraph::query::UserProfileQuery::LimitValueResult::Type::UNLIMITED) return;
  // Different limits support different values
  switch (limit_type) {
    case memgraph::auth::UserProfiles::Limits::kSessions:
      if (value_type != memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY) {
        throw memgraph::query::QueryRuntimeException("Limit 'sessions' only supports integer values.");
      }
      break;
    case memgraph::auth::UserProfiles::Limits::kTransactionsMemory:
      if (value_type != memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT) {
        throw memgraph::query::QueryRuntimeException(
            "Limit 'sessions' only supports memory limit values. Example: 100MB");
      }
      break;
  }
}

auto convert_limit_value(const memgraph::auth::UserProfiles::Profile &profile) {
  memgraph::query::UserProfileQuery::limits_t query_profile;
  for (const auto &[limit_type, limit_value] : profile.limits) {
    memgraph::query::UserProfileQuery::LimitValueResult limit_value_result;
    if (std::holds_alternative<memgraph::auth::UserProfiles::unlimitted_t>(limit_value)) {
      limit_value_result.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::UNLIMITED;
    } else if (limit_type == memgraph::auth::UserProfiles::Limits::kSessions) {
      limit_value_result.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::QUANTITY;
      limit_value_result.quantity.value = std::get<uint64_t>(limit_value);
    } else if (limit_type == memgraph::auth::UserProfiles::Limits::kTransactionsMemory) {
      limit_value_result.type = memgraph::query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
      if (std::get<uint64_t>(limit_value) >= 1024UL * 1024UL) {  // Convert to MB
        limit_value_result.mem_limit.value = std::get<uint64_t>(limit_value) / 1024 / 1024;
        limit_value_result.mem_limit.scale = 1024UL * 1024UL;
      } else {  // Convert to KB
        limit_value_result.mem_limit.value = std::get<uint64_t>(limit_value) / 1024;
        limit_value_result.mem_limit.scale = 1024;
      }
    }
    query_profile.emplace_back(memgraph::auth::UserProfiles::kLimits[(int)limit_type], limit_value_result);
  }
  return query_profile;
}
#endif
}  // namespace

namespace memgraph::glue {

AuthQueryHandler::AuthQueryHandler(memgraph::auth::SynchedAuth *auth) : auth_(auth) {}

bool AuthQueryHandler::CreateUser(const std::string &username, const std::optional<std::string> &password,
                                  system::Transaction *system_tx) {
  try {
    const auto [first_user, user_added] = std::invoke([&, this] {
      auto locked_auth = auth_->Lock();
      const auto first_user = !locked_auth->HasUsers();
      const auto user_added = locked_auth->AddUser(username, password, system_tx).has_value();
      return std::make_pair(first_user, user_added);
    });

    if (first_user) {
      spdlog::info("{} is the first created user. Granting all privileges.", username);
      GrantPrivilege(
          username, memgraph::query::kPrivilegesAll
#ifdef MG_ENTERPRISE
          ,
          {{{memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {memgraph::query::kAsterisk}}}},
          {{{memgraph::query::AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {memgraph::query::kAsterisk}}}}
#endif
          ,
          system_tx);
#ifdef MG_ENTERPRISE
      GrantDatabase(auth::kAllDatabases, username, system_tx);
      SetMainDatabase(dbms::kDefaultDB, username, system_tx);
#endif
    }

    return user_added;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::DropUser(const std::string &username, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) return false;
    const auto res = locked_auth->RemoveUser(username, system_tx);
    return res;
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::SetPassword(const std::string &username, const std::optional<std::string> &password,
                                   system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }
    locked_auth->UpdatePassword(*user, password);
    locked_auth->SaveUser(*user, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::ChangePassword(const std::string &username, const std::optional<std::string> &oldPassword,
                                      const std::optional<std::string> &newPassword, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }
    if (user->CheckPasswordExplicit(*oldPassword)) {
      locked_auth->UpdatePassword(*user, newPassword);
      locked_auth->SaveUser(*user, system_tx);
    } else {
      throw memgraph::query::QueryRuntimeException("Old password is not correct.");
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::CreateRole(const std::string &rolename, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    return locked_auth->AddRole(rolename, system_tx).has_value();
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

#ifdef MG_ENTERPRISE
void AuthQueryHandler::GrantDatabase(const std::string &db_name, const std::string &user_or_role,
                                     system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    const auto res = locked_auth->GrantDatabase(db_name, user_or_role, system_tx);
    switch (res) {
      using enum auth::Auth::Result;
      case SUCCESS:
        return;
      case NO_USER_ROLE:
        throw query::QueryRuntimeException("No user nor role '{}' found.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::DenyDatabase(const std::string &db_name, const std::string &user_or_role,
                                    system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    const auto res = locked_auth->DenyDatabase(db_name, user_or_role, system_tx);
    switch (res) {
      using enum auth::Auth::Result;
      case SUCCESS:
        return;
      case NO_USER_ROLE:
        throw query::QueryRuntimeException("No user nor role '{}' found.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::RevokeDatabase(const std::string &db_name, const std::string &user_or_role,
                                      system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    const auto res = locked_auth->RevokeDatabase(db_name, user_or_role, system_tx);
    switch (res) {
      using enum auth::Auth::Result;
      case SUCCESS:
        return;
      case NO_USER_ROLE:
        throw query::QueryRuntimeException("No user nor role '{}' found.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<std::vector<memgraph::query::TypedValue>> AuthQueryHandler::GetDatabasePrivileges(
    const std::string &user, const std::vector<std::string> &roles) {
  try {
    auto locked_auth = auth_->ReadLock();
    if (auto local_user = locked_auth->GetUser(user)) {
      return ShowDatabasePrivileges(local_user);
    }
    // User doesn't exist, check if any of the roles exist (this can happen when auth module is used)
    std::optional<memgraph::auth::Roles> roles_obj;
    for (const auto &role : roles) {
      if (auto role_obj = locked_auth->GetRole(role)) {
        if (!roles_obj) roles_obj.emplace();
        roles_obj->AddRole(std::move(*role_obj));
      }
    }
    if (roles_obj) {
      return ShowDatabasePrivileges(roles_obj);
    }
    throw memgraph::query::QueryRuntimeException("Missing user '{}' or one of role: {}.", user, fmt::join(roles, ", "));
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::SetMainDatabase(std::string_view db_name, const std::string &user_or_role,
                                       system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    const auto res = locked_auth->SetMainDatabase(db_name, user_or_role, system_tx);
    switch (res) {
      using enum auth::Auth::Result;
      case SUCCESS:
        return;
      case NO_USER_ROLE:
        throw query::QueryRuntimeException("No user nor role '{}' found.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::DeleteDatabase(std::string_view db_name, system::Transaction *system_tx) {
  try {
    auth_->Lock()->DeleteDatabase(std::string(db_name), system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::optional<std::string> AuthQueryHandler::GetMainDatabase(const std::string &user_or_role) {
  try {
    auto locked_auth = auth_->ReadLock();
    if (auto user = locked_auth->GetUser(user_or_role)) {
      try {
        return user->GetMain();
      } catch (const memgraph::auth::AuthException &) {
        return std::nullopt;  // User has no main database set
      }
    }

    if (auto role = locked_auth->GetRole(user_or_role)) {
      try {
        return role->GetMain();
      } catch (const memgraph::auth::AuthException &) {
        return std::nullopt;  // Role has no main database set
      }
    }

    return std::nullopt;  // User/role doesn't exist
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}
#endif

bool AuthQueryHandler::DropRole(const std::string &rolename, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto role = locked_auth->GetRole(rolename);

    if (!role) {
      return false;
    };

    return locked_auth->RemoveRole(rolename, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

bool AuthQueryHandler::HasRole(const std::string &rolename) {
  try {
    auto locked_auth = auth_->ReadLock();
    return locked_auth->GetRole(rolename).has_value();
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

std::vector<std::string> AuthQueryHandler::GetRolenamesForUser(const std::string &username,
                                                               std::optional<std::string> db_name) {
  try {
    auto locked_auth = auth_->ReadLock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }

    std::vector<std::string> rolenames;
    auto roles = user->roles().GetRoles();
#ifdef MG_ENTERPRISE
    if (db_name.has_value()) {
      // Get roles filtered by database
      roles = user->GetMultiTenantRoles(db_name.value());
    }
#endif
    if (!roles.empty()) {
      rolenames.reserve(roles.size());
      for (const auto &role : roles) {
        rolenames.emplace_back(role.rolename());
      }
    }
    return rolenames;
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

void AuthQueryHandler::SetRoles(const std::string &username, const std::vector<std::string> &roles,
                                const std::unordered_set<std::string> &role_databases, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }

#ifdef MG_ENTERPRISE
    // Multi-tenant support
    if (!role_databases.empty()) {
      for (const auto &db_name : role_databases) {
        user->ClearMultiTenantRoles(db_name);
        for (const auto &role : roles) {
          auto role_obj = locked_auth->GetRole(role);
          if (!role_obj) {
            throw memgraph::query::QueryRuntimeException("Role '{}' doesn't exist.", role);
          }
          user->AddMultiTenantRole(*role_obj, db_name);
        }
      }
      locked_auth->SaveUser(*user, system_tx);
      return;
    }
#endif

    // Clear existing roles first
    user->ClearAllRoles();

    // Add each role
    for (const auto &rolename : roles) {
      auto role = locked_auth->GetRole(rolename);
      if (!role) {
        throw memgraph::query::QueryRuntimeException("Role '{}' doesn't exist.", rolename);
      }
      user->AddRole(*role);
    }

    locked_auth->SaveUser(*user, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::ClearRoles(const std::string &username, const std::unordered_set<std::string> &role_databases,
                                  system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }

#ifdef MG_ENTERPRISE
    // Multi-tenant support
    if (!role_databases.empty()) {
      for (const auto &db_name : role_databases) {
        user->ClearMultiTenantRoles(db_name);
      }
      locked_auth->SaveUser(*user, system_tx);
      return;
    }
#endif

    // Clear all roles (default behavior)
    user->ClearAllRoles();
    locked_auth->SaveUser(*user, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::RemoveRole(const std::string &username, const std::string &rolename,
                                  system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    auto user = locked_auth->GetUser(username);
    if (!user) {
      throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", username);
    }

    // Check if user has this role
    bool has_role = false;
    for (const auto &role : user->roles()) {
      if (role.rolename() == rolename) {
        has_role = true;
        break;
      }
    }

    if (!has_role) {
      throw memgraph::query::QueryRuntimeException("User '{}' is not a member of role '{}'.", username, rolename);
    }

    user->RemoveRole(rolename);
    locked_auth->SaveUser(*user, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::vector<std::vector<memgraph::query::TypedValue>> AuthQueryHandler::GetPrivileges(
    const std::string &user_or_role, std::optional<std::string> db_name) {
  try {
    auto locked_auth = auth_->ReadLock();
    std::vector<std::vector<memgraph::query::TypedValue>> grants;
#ifdef MG_ENTERPRISE
    std::vector<std::vector<memgraph::query::TypedValue>> fine_grained_grants;
#endif
    if (auto user = locked_auth->GetUser(user_or_role)) {
      grants = ShowUserPrivileges(user, db_name);
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        fine_grained_grants = ShowFineGrainedUserPrivileges(user, db_name);
      }
#endif
    } else if (auto role = locked_auth->GetRole(user_or_role)) {
      grants = ShowRolePrivileges(role, db_name);
#ifdef MG_ENTERPRISE
      if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        fine_grained_grants = ShowFineGrainedRolePrivileges(role, db_name);
      }
#endif
    } else {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
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
    ,
    system::Transaction *system_tx) {
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
      ,
      system_tx);
}  // namespace memgraph::glue

void AuthQueryHandler::DenyPrivilege(const std::string &user_or_role,
                                     const std::vector<memgraph::query::AuthQuery::Privilege> &privileges,
                                     system::Transaction *system_tx) {
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
      ,
      system_tx);
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
    ,
    system::Transaction *system_tx) {
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
      ,
      system_tx);
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
    ,
    system::Transaction *system_tx) {
  try {
    std::vector<memgraph::auth::Permission> permissions;
    permissions.reserve(privileges.size());
    for (const auto &privilege : privileges) {
      permissions.push_back(memgraph::glue::PrivilegeToPermission(privilege));
    }
    auto locked_auth = auth_->Lock();

    if (auto user = locked_auth->GetUser(user_or_role)) {
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
      locked_auth->SaveUser(*user, system_tx);
    } else if (auto role = locked_auth->GetRole(user_or_role)) {
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
      locked_auth->SaveRole(*role, system_tx);
    } else {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

#ifdef MG_ENTERPRISE
void AuthQueryHandler::GrantImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                            system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();

    const bool all = targets.size() == 1 && targets[0] == "*";
    std::vector<auth::User> target_users;  // TODO User or UserId?
    if (!all) {
      for (const auto &target : targets) {
        auto user = locked_auth->GetUser(target);
        if (!user) {
          throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", target);
        }
        target_users.emplace_back(std::move(*user));
      }
    }

    if (auto user = locked_auth->GetUser(user_or_role)) {
      user->permissions().Grant(auth::Permission::IMPERSONATE_USER);
      if (all) {
        user->GrantUserImp();
      } else {
        user->GrantUserImp(target_users);
      }
      locked_auth->SaveUser(*user, system_tx);
    } else if (auto role = locked_auth->GetRole(user_or_role)) {
      role->permissions().Grant(auth::Permission::IMPERSONATE_USER);
      if (all) {
        role->GrantUserImp();
      } else {
        role->GrantUserImp(target_users);
      }
      locked_auth->SaveRole(*role, system_tx);
    } else {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::DenyImpersonateUser(const std::string &user_or_role, const std::vector<std::string> &targets,
                                           system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();

    const bool all = targets.size() == 1 && targets[0] == "*";
    if (all) {
      throw memgraph::query::QueryRuntimeException(
          "Cannot deny all users. Instead try to revoke the IMPERSONATE_USER privilege.");
    }
    std::vector<auth::User> target_users;  // TODO User or UserId?
    for (const auto &target : targets) {
      auto user = locked_auth->GetUser(target);
      if (!user) {
        throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", target);
      }
      target_users.emplace_back(std::move(*user));
    }

    if (auto user = locked_auth->GetUser(user_or_role)) {
      user->DenyUserImp(target_users);
      locked_auth->SaveUser(*user, system_tx);
    } else if (auto role = locked_auth->GetRole(user_or_role)) {
      role->DenyUserImp(target_users);
      locked_auth->SaveRole(*role, system_tx);
    } else {
      throw memgraph::query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
    }
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::CreateProfile(const std::string &profile_name,
                                     const query::UserProfileQuery::limits_t &defined_limits,
                                     system::Transaction *system_tx) {
  auth::UserProfiles::limits_t limits;
  for (const auto &[limit_name, limit_value] : defined_limits) {
    const auto limit_type = name_to_limit(limit_name);
    is_limit_supported(limit_value.type, limit_type);  // throw on failure
    switch (limit_value.type) {
      case query::UserProfileQuery::LimitValueResult::Type::UNLIMITED:
        limits.emplace(limit_type, auth::UserProfiles::unlimitted_t{});
        break;
      case query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT: {
        limits.emplace(limit_type, limit_value.mem_limit.value * limit_value.mem_limit.scale);
      } break;
      case query::UserProfileQuery::LimitValueResult::Type::QUANTITY: {
        limits.emplace(limit_type, limit_value.quantity.value);
      } break;
    }
  }
  auto locked_auth = auth_->Lock();
  if (!locked_auth->CreateProfile(profile_name, std::move(limits), system_tx)) {
    throw memgraph::query::QueryRuntimeException("Profile '{}' already exists.", profile_name);
  }
}

void AuthQueryHandler::UpdateProfile(const std::string &profile_name,
                                     const query::UserProfileQuery::limits_t &updated_limits,
                                     system::Transaction *system_tx) {
  auth::UserProfiles::limits_t limits;
  for (const auto &[limit_name, limit_value] : updated_limits) {
    const auto limit_type = name_to_limit(limit_name);
    is_limit_supported(limit_value.type, limit_type);  // throw on failure
    switch (limit_value.type) {
      case query::UserProfileQuery::LimitValueResult::Type::UNLIMITED:
        limits.emplace(limit_type, auth::UserProfiles::unlimitted_t{});
        break;
      case query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT: {
        limits.emplace(limit_type, limit_value.mem_limit.value * limit_value.mem_limit.scale);
      } break;
      case query::UserProfileQuery::LimitValueResult::Type::QUANTITY: {
        limits.emplace(limit_type, limit_value.quantity.value);
      } break;
    }
  }
  auto locked_auth = auth_->Lock();
  const auto &profile = locked_auth->UpdateProfile(profile_name, limits, system_tx);
  if (!profile) {
    throw memgraph::query::QueryRuntimeException("Profile '{}' does not exist.", profile_name);
  }
}

void AuthQueryHandler::DropProfile(const std::string &profile_name, system::Transaction *system_tx) {
  auto locked_auth = auth_->Lock();
  if (!locked_auth->DropProfile(profile_name, system_tx)) {
    throw memgraph::query::QueryRuntimeException("Profile '{}' does not exist.", profile_name);
  }
}

query::UserProfileQuery::limits_t AuthQueryHandler::GetProfile(std::string_view profile_name) {
  auto locked_auth = auth_->Lock();
  auto profile = locked_auth->GetProfile(profile_name);
  if (!profile) {
    throw query::QueryRuntimeException("Profile '{}' does not exist.", profile_name);
  }
  // Fill missing/unlimited limits
  for (size_t e_id = 0; e_id < auth::UserProfiles::kLimits.size(); ++e_id) {
    const auto limit = static_cast<auth::UserProfiles::Limits>(e_id);
    if (profile->limits.find(limit) == profile->limits.end()) {
      profile->limits.emplace(limit, auth::UserProfiles::unlimitted_t{});
    }
  }
  return convert_limit_value(*profile);
}

std::vector<std::pair<std::string, query::UserProfileQuery::limits_t>> AuthQueryHandler::AllProfiles() {
  std::vector<std::pair<std::string, query::UserProfileQuery::limits_t>> res;
  auto locked_auth = auth_->Lock();
  for (const auto &profile : locked_auth->AllProfiles()) {
    // Fill missing/unlimited limits
    for (size_t e_id = 0; e_id < auth::UserProfiles::kLimits.size(); ++e_id) {
      const auto limit = static_cast<auth::UserProfiles::Limits>(e_id);
      if (profile.limits.find(limit) == profile.limits.end()) {
        profile.limits.emplace(limit, auth::UserProfiles::unlimitted_t{});
      }
    }
    auto limits = convert_limit_value(profile);
    res.emplace_back(profile.name, limits);
  }
  return res;
}

void AuthQueryHandler::SetProfile(const std::string &profile_name, const std::string &user_or_role,
                                  system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    const auto profile = locked_auth->SetProfile(profile_name, user_or_role, system_tx);
    DMG_ASSERT(profile, "Missing profile");
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

void AuthQueryHandler::RevokeProfile(const std::string &user_or_role, system::Transaction *system_tx) {
  try {
    auto locked_auth = auth_->Lock();
    locked_auth->RevokeProfile(user_or_role, system_tx);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::optional<std::string> AuthQueryHandler::GetProfileForUser(const std::string &user_or_role) {
  auto locked_auth = auth_->Lock();
  auto user = locked_auth->GetUser(user_or_role);
  if (!user) {
    throw memgraph::query::QueryRuntimeException("User '{}' doesn't exist.", user_or_role);
  }
  if (const auto profile = user->profile(); profile) {
    return profile->name;
  }
  return std::nullopt;
}

std::vector<std::string> AuthQueryHandler::GetUsernamesForProfile(const std::string &profile_name) {
  try {
    auto locked_auth = auth_->Lock();
    return locked_auth->GetUsernamesForProfile(profile_name);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}

std::optional<std::string> AuthQueryHandler::GetProfileForRole(const std::string &user_or_role) {
  auto locked_auth = auth_->Lock();
  auto role = locked_auth->GetRole(user_or_role);
  if (!role) {
    throw memgraph::query::QueryRuntimeException("Role '{}' doesn't exist.", user_or_role);
  }
  if (const auto profile = role->profile(); profile) {
    return profile->name;
  }
  return std::nullopt;
}

std::vector<std::string> AuthQueryHandler::GetRolenamesForProfile(const std::string &profile_name) {
  try {
    auto locked_auth = auth_->Lock();
    return locked_auth->GetRolenamesForProfile(profile_name);
  } catch (const memgraph::auth::AuthException &e) {
    throw memgraph::query::QueryRuntimeException(e.what());
  }
}
#endif

}  // namespace memgraph::glue
