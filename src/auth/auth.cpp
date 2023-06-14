// Copyright 2022 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/auth.hpp"

#include <cstring>
#include <iostream>
#include <limits>
#include <utility>

#include <fmt/format.h>

#include "auth/exceptions.hpp"
#include "license/license.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

DEFINE_VALIDATED_string(auth_module_executable, "", "Absolute path to the auth module executable that should be used.",
                        {
                          if (value.empty()) return true;
                          // Check the file status, following symlinks.
                          auto status = std::filesystem::status(value);
                          if (!std::filesystem::is_regular_file(status)) {
                            std::cerr << "The auth module path doesn't exist or isn't a file!" << std::endl;
                            return false;
                          }
                          return true;
                        });
DEFINE_bool(auth_module_create_missing_user, true, "Set to false to disable creation of missing users.");
DEFINE_bool(auth_module_create_missing_role, true, "Set to false to disable creation of missing roles.");
DEFINE_bool(auth_module_manage_roles, true, "Set to false to disable management of roles through the auth module.");
DEFINE_VALIDATED_int32(auth_module_timeout_ms, 10000,
                       "Timeout (in milliseconds) used when waiting for a "
                       "response from the auth module.",
                       FLAG_IN_RANGE(100, 1800000));

namespace memgraph::auth {
const std::string kUserPrefix = "user:";
const std::string kRolePrefix = "role:";
const std::string kLinkPrefix = "link:";

/**
 * All data stored in the `Auth` storage is stored in an underlying
 * `kvstore::KVStore`. Because we are using a key-value store to store the data,
 * the data has to be encoded. The encoding used is as follows:
 *
 * User: key="user:<username>", value="<json_encoded_members_of_user>"
 * Role: key="role:<rolename>", value="<json_encoded_members_of_role>"
 *
 * The User->Role relationship isn't stored in the `User` encoded data because
 * we want to be able to delete/modify a Role and have it automatically be
 * removed/modified in all linked users. Because of that we store the links to
 * the role as a foreign-key like mapping in the KVStore. It is saved as
 * follows:
 *
 * key="link:<username>", value="<rolename>"
 */

Auth::Auth(const std::string &storage_directory) : storage_(storage_directory), module_(FLAGS_auth_module_executable) {}

std::optional<User> Auth::Authenticate(const std::string &username, const std::string &password) {
  if (module_.IsUsed()) {
    const auto license_check_result = license::global_license_checker.IsEnterpriseValid(utils::global_settings);
    if (license_check_result.HasError()) {
      spdlog::warn(license::LicenseCheckErrorToString(license_check_result.GetError(), "authentication modules"));
      return std::nullopt;
    }

    nlohmann::json params = nlohmann::json::object();
    params["username"] = username;
    params["password"] = password;

    auto ret = module_.Call(params, FLAGS_auth_module_timeout_ms);

    // Verify response integrity.
    if (!ret.is_object() || ret.find("authenticated") == ret.end() || ret.find("role") == ret.end()) {
      return std::nullopt;
    }
    const auto &ret_authenticated = ret.at("authenticated");
    const auto &ret_role = ret.at("role");
    if (!ret_authenticated.is_boolean() || !ret_role.is_string()) {
      return std::nullopt;
    }
    auto is_authenticated = ret_authenticated.get<bool>();
    const auto &rolename = ret_role.get<std::string>();

    // Authenticate the user.
    if (!is_authenticated) return std::nullopt;

    // Find or create the user and return it.
    auto user = GetUser(username);
    if (!user) {
      if (FLAGS_auth_module_create_missing_user) {
        user = AddUser(username, password);
        if (!user) {
          spdlog::warn(utils::MessageWithLink(
              "Couldn't create the missing user '{}' using the auth module because the user already exists as a role.",
              username, "https://memgr.ph/auth"));
          return std::nullopt;
        }
      } else {
        spdlog::warn(utils::MessageWithLink(
            "Couldn't authenticate user '{}' using the auth module because the user doesn't exist.", username,
            "https://memgr.ph/auth"));
        return std::nullopt;
      }
    } else {
      user->UpdatePassword(password);
    }
    if (FLAGS_auth_module_manage_roles) {
      if (!rolename.empty()) {
        auto role = GetRole(rolename);
        if (!role) {
          if (FLAGS_auth_module_create_missing_role) {
            role = AddRole(rolename);
            if (!role) {
              spdlog::warn(
                  utils::MessageWithLink("Couldn't authenticate user '{}' using the auth module because the user's "
                                         "role '{}' already exists as a user.",
                                         username, rolename, "https://memgr.ph/auth"));
              return std::nullopt;
            }
            SaveRole(*role);
          } else {
            spdlog::warn(utils::MessageWithLink(
                "Couldn't authenticate user '{}' using the auth module because the user's role '{}' doesn't exist.",
                username, rolename, "https://memgr.ph/auth"));
            return std::nullopt;
          }
        }
        user->SetRole(*role);
      } else {
        user->ClearRole();
      }
    }
    SaveUser(*user);
    return user;
  } else {
    auto user = GetUser(username);
    if (!user) {
      spdlog::warn(utils::MessageWithLink("Couldn't authenticate user '{}' because the user doesn't exist.", username,
                                          "https://memgr.ph/auth"));
      return std::nullopt;
    }
    if (!user->CheckPassword(password)) {
      spdlog::warn(utils::MessageWithLink("Couldn't authenticate user '{}' because the password is not correct.",
                                          username, "https://memgr.ph/auth"));
      return std::nullopt;
    }
    return user;
  }
}

std::optional<User> Auth::GetUser(const std::string &username_orig) const {
  auto username = utils::ToLowerCase(username_orig);
  auto existing_user = storage_.Get(kUserPrefix + username);
  if (!existing_user) return std::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_user);
  } catch (const nlohmann::json::parse_error &e) {
    throw AuthException("Couldn't load user data!");
  }

  auto user = User::Deserialize(data);
  auto link = storage_.Get(kLinkPrefix + username);

  if (link) {
    auto role = GetRole(*link);
    if (role) {
      user.SetRole(*role);
    }
  }
  return user;
}

void Auth::SaveUser(const User &user) {
  bool success = false;
  if (const auto *role = user.role(); role != nullptr) {
    success = storage_.PutMultiple(
        {{kUserPrefix + user.username(), user.Serialize().dump()}, {kLinkPrefix + user.username(), role->rolename()}});
  } else {
    success = storage_.PutAndDeleteMultiple({{kUserPrefix + user.username(), user.Serialize().dump()}},
                                            {kLinkPrefix + user.username()});
  }
  if (!success) {
    throw AuthException("Couldn't save user '{}'!", user.username());
  }
}

std::optional<User> Auth::AddUser(const std::string &username, const std::optional<std::string> &password) {
  auto existing_user = GetUser(username);
  if (existing_user) return std::nullopt;
  auto existing_role = GetRole(username);
  if (existing_role) return std::nullopt;
  auto new_user = User(username);
  new_user.UpdatePassword(password);
  SaveUser(new_user);
  return new_user;
}

bool Auth::RemoveUser(const std::string &username_orig) {
  auto username = utils::ToLowerCase(username_orig);
  if (!storage_.Get(kUserPrefix + username)) return false;
  std::vector<std::string> keys({kLinkPrefix + username, kUserPrefix + username});
  if (!storage_.DeleteMultiple(keys)) {
    throw AuthException("Couldn't remove user '{}'!", username);
  }
  return true;
}

std::vector<auth::User> Auth::AllUsers() const {
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kUserPrefix); it != storage_.end(kUserPrefix); ++it) {
    auto username = it->first.substr(kUserPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    auto user = GetUser(username);
    if (user) {
      ret.push_back(std::move(*user));
    }
  }
  return ret;
}

bool Auth::HasUsers() const { return storage_.begin(kUserPrefix) != storage_.end(kUserPrefix); }

std::optional<Role> Auth::GetRole(const std::string &rolename_orig) const {
  auto rolename = utils::ToLowerCase(rolename_orig);
  auto existing_role = storage_.Get(kRolePrefix + rolename);
  if (!existing_role) return std::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_role);
  } catch (const nlohmann::json::parse_error &e) {
    throw AuthException("Couldn't load role data!");
  }

  return Role::Deserialize(data);
}

void Auth::SaveRole(const Role &role) {
  if (!storage_.Put(kRolePrefix + role.rolename(), role.Serialize().dump())) {
    throw AuthException("Couldn't save role '{}'!", role.rolename());
  }
}

std::optional<Role> Auth::AddRole(const std::string &rolename) {
  auto existing_role = GetRole(rolename);
  if (existing_role) return std::nullopt;
  auto existing_user = GetUser(rolename);
  if (existing_user) return std::nullopt;
  auto new_role = Role(rolename);
  SaveRole(new_role);
  return new_role;
}

bool Auth::RemoveRole(const std::string &rolename_orig) {
  auto rolename = utils::ToLowerCase(rolename_orig);
  if (!storage_.Get(kRolePrefix + rolename)) return false;
  std::vector<std::string> keys;
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix); ++it) {
    if (utils::ToLowerCase(it->second) == rolename) {
      keys.push_back(it->first);
    }
  }
  keys.push_back(kRolePrefix + rolename);
  if (!storage_.DeleteMultiple(keys)) {
    throw AuthException("Couldn't remove role '{}'!", rolename);
  }
  return true;
}

std::vector<auth::Role> Auth::AllRoles() const {
  std::vector<auth::Role> ret;
  for (auto it = storage_.begin(kRolePrefix); it != storage_.end(kRolePrefix); ++it) {
    auto rolename = it->first.substr(kRolePrefix.size());
    if (rolename != utils::ToLowerCase(rolename)) continue;
    auto role = GetRole(rolename);
    if (role) {
      ret.push_back(*role);
    } else {
      throw AuthException("Couldn't load role '{}'!", rolename);
    }
  }
  return ret;
}

std::vector<auth::User> Auth::AllUsersForRole(const std::string &rolename_orig) const {
  auto rolename = utils::ToLowerCase(rolename_orig);
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix); ++it) {
    auto username = it->first.substr(kLinkPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    if (it->second != utils::ToLowerCase(it->second)) continue;
    if (it->second == rolename) {
      auto user = GetUser(username);
      if (user) {
        ret.push_back(std::move(*user));
      } else {
        throw AuthException("Couldn't load user '{}'!", username);
      }
    }
  }
  return ret;
}

}  // namespace memgraph::auth
