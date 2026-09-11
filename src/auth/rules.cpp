// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/rules.hpp"

#include <string>
#include <unordered_set>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "auth/exceptions.hpp"
#include "utils/string.hpp"

namespace memgraph::auth::rules {

namespace {
nlohmann::json ParseJson(std::string_view str) {
  try {
    return nlohmann::json::parse(str);
  } catch (nlohmann::json::parse_error const &) {
    throw AuthException("Couldn't load auth data!");
  }
}

nlohmann::json ParseAndMigrateJson(std::string_view str) {
  auto data = ParseJson(str);
  MigrateAuthJson(data);
  return data;
}
}  // namespace

void LinkUser(Repository const &repo, User &user) {
  // User set roles on particular databases
  // NOTE Has to be done in this order, otherwise the global roles will overwrite the multi-tenant roles
  [[maybe_unused]] std::unordered_set<std::string> failed_mt_roles;
#ifdef MG_ENTERPRISE
  auto mt_link = repo.Get(Repository::MtLinkKey(user.username()));
  if (mt_link) {
    try {
      auto json_data = ParseJson(*mt_link);
      if (!json_data.is_object()) {
        spdlog::warn("Found invalid JSON in mtlink format for user '{}'", user.username());
        return;
      }
      for (const auto &[db, roles_array] : json_data.items()) {
        if (!roles_array.is_array()) {
          spdlog::warn(
              "Invalid mtlink entry for user '{}': expected array of rolenames for db '{}'", user.username(), db);
          continue;
        }
        for (const auto &rolename_json : roles_array) {
          if (!rolename_json.is_string()) {
            spdlog::warn(
                "Invalid mtlink entry for user '{}': expected string rolename for db '{}'", user.username(), db);
            continue;
          }
          const auto &rolename = rolename_json.get<std::string>();
          auto role = GetRole(repo, rolename);
          if (!role) {
            spdlog::warn("Role '{}' doesn't exist for user '{}'", rolename, user.username());
            continue;
          }
          try {
            user.AddMultiTenantRole(*role, db);
          } catch (const AuthException &e) {
            spdlog::warn("Couldn't add multi-tenant role '{}' to user '{}' on database '{}': {}",
                         rolename,
                         user.username(),
                         db,
                         e.what());
            failed_mt_roles.insert(rolename);
          }
        }
      }
    } catch (const nlohmann::detail::exception &) {
      // This shouldn't happen after V2 migration, but handle gracefully
      spdlog::warn("Found invalid JSON in mtlink format for user '{}'", user.username());
      return;
    }
  }
#endif

  // User set these roles on all databases
  auto link = repo.Get(Repository::RoleLinkKey(user.username()));
  if (link) {
    try {
      // Parse as JSON array (V2 format)
      auto json_data = ParseJson(*link);
      if (!json_data.is_array()) {
        spdlog::warn("Found invalid JSON in link format for user '{}'", user.username());
        return;
      }
      // V2 format: array of role names
      for (const auto &role_name : json_data) {
        if (role_name.is_string()) {
          // Check that the role is not already added (via the multi-tenant role) or failed to add
          if (failed_mt_roles.contains(role_name.get<std::string>()) ||
              user.roles().GetRole(role_name.get<std::string>())) {
            continue;
          }
          auto role = GetRole(repo, role_name.get<std::string>());
          if (role) {
            user.AddRole(*role);
          }
        }
      }

    } catch (const nlohmann::detail::exception &) {
      // This shouldn't happen after V2 migration, but handle gracefully
      spdlog::warn("Found invalid JSON in link format for user '{}'", user.username());
      return;
    }
  }

#ifdef MG_ENTERPRISE
  // Profile linking moved to UserProfiles class
#endif
}

std::optional<User> GetUser(Repository const &repo, std::string_view username_raw) {
  auto username = utils::ToLowerCase(username_raw);
  auto existing_user = repo.Get(Repository::UserKey(username));
  if (!existing_user) return std::nullopt;

  auto user = User::Deserialize(ParseAndMigrateJson(*existing_user));
  LinkUser(repo, user);
  return user;
}

std::optional<Role> GetRole(Repository const &repo, std::string_view rolename_raw) {
  auto rolename = utils::ToLowerCase(rolename_raw);
  auto existing_role = repo.Get(Repository::RoleKey(rolename));
  if (!existing_role) return std::nullopt;

  auto role = Role::Deserialize(ParseAndMigrateJson(*existing_role));
  return role;
}

std::vector<User> AllUsers(Repository const &repo) {
  std::vector<User> ret;
  repo.ForEachUser([&](auto username, auto const &value) {
    if (username != utils::ToLowerCase(username)) return;
    try {
      User user = User::Deserialize(ParseAndMigrateJson(value));
      LinkUser(repo, user);
      ret.emplace_back(std::move(user));
    } catch (AuthException &) {
    }
  });
  return ret;
}

std::vector<std::string> AllUsernames(Repository const &repo) {
  std::vector<std::string> ret;
  repo.ForEachUser([&](auto username, auto const &value) {
    if (username != utils::ToLowerCase(username)) return;
    try {
      User::Deserialize(ParseAndMigrateJson(value));
      ret.emplace_back(username);
    } catch (AuthException &) {
    }
  });
  return ret;
}

std::vector<Role> AllRoles(Repository const &repo) {
  std::vector<Role> ret;
  repo.ForEachRole([&](auto rolename, auto const &value) {
    if (rolename != utils::ToLowerCase(rolename)) return;
    Role role = Role::Deserialize(ParseAndMigrateJson(value));
    ret.emplace_back(std::move(role));
  });
  return ret;
}

std::vector<std::string> AllRolenames(Repository const &repo) {
  std::vector<std::string> ret;
  repo.ForEachRole([&](auto rolename, auto const &value) {
    if (rolename != utils::ToLowerCase(rolename)) return;
    try {
      Role::Deserialize(ParseAndMigrateJson(value));
      ret.emplace_back(rolename);
    } catch (AuthException &) {
    }
  });
  return ret;
}

std::vector<User> AllUsersForRole(Repository const &repo, std::string_view rolename_raw) {
  const auto rolename = utils::ToLowerCase(rolename_raw);
  std::vector<User> ret;
  repo.ForEachRoleLink([&](auto username, auto const &value) {
    if (username != utils::ToLowerCase(username)) return;

    bool has_role = false;
    try {
      auto json_data = ParseJson(value);
      if (!json_data.is_array()) {
        spdlog::warn("Found non-array link format for user '{}'", username);
        return;
      }
      for (auto const &role_name : json_data) {
        if (role_name.is_string() && utils::ToLowerCase(role_name.template get<std::string>()) == rolename) {
          has_role = true;
          break;
        }
      }
    } catch (nlohmann::detail::exception const &) {
      spdlog::warn("Found invalid JSON in link format for user '{}', treating as single role", username);
      return;
    }

    if (has_role) {
      if (auto user = GetUser(repo, username)) {
        ret.push_back(std::move(*user));
      } else {
        throw AuthException("Couldn't load user '{}'!", username);
      }
    }
  });
  return ret;
}

std::vector<std::string> AllUsernamesForRole(Repository const &repo, std::string_view rolename_raw) {
  const auto rolename = utils::ToLowerCase(rolename_raw);
  std::vector<std::string> ret;
  repo.ForEachRoleLink([&](auto username, auto const &value) {
    if (username != utils::ToLowerCase(username)) return;
    bool has_role = false;
    try {
      auto json_data = ParseJson(value);
      if (!json_data.is_array()) {
        spdlog::warn("Found non-array link format for user '{}'", username);
        return;
      }
      for (auto const &role_name : json_data) {
        if (role_name.is_string() && utils::ToLowerCase(role_name.template get<std::string>()) == rolename) {
          has_role = true;
          break;
        }
      }
    } catch (nlohmann::detail::exception const &) {
      spdlog::warn("Found invalid JSON in link format for user '{}', treating as single role", username);
      return;
    }
    if (has_role) ret.push_back(std::move(username));
  });
  return ret;
}

bool HasUser(Repository const &repo, std::string_view name) {
  auto username = utils::ToLowerCase(name);
  return repo.Get(Repository::UserKey(username)).has_value();
}

bool HasRole(Repository const &repo, std::string_view name) {
  auto rolename = utils::ToLowerCase(name);
  return repo.Get(Repository::RoleKey(rolename)).has_value();
}

bool HasUsers(Repository const &repo) { return repo.HasAnyUser(); }

}  // namespace memgraph::auth::rules
