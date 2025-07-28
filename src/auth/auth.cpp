// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/auth.hpp"

#include <optional>
#include <utility>

#include <fmt/format.h>

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "auth/rpc.hpp"
#include "flags/auth.hpp"
#include "license/license.hpp"
#include "system/transaction.hpp"
#include "utils/flag_validation.hpp"
#include "utils/message.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

namespace memgraph {
std::unordered_map<std::string, std::string> ModuleMappingsToMap(std::string_view module_mappings) {
  std::unordered_map<std::string, std::string> module_per_scheme;
  if (module_mappings.empty()) {
    return module_per_scheme;
  }

  for (const auto &mapping : utils::Split(module_mappings, ";")) {
    const auto module_and_scheme = utils::Split(mapping, ":");
    if (module_and_scheme.empty()) {
      throw memgraph::utils::BasicException(
          "Empty auth module mapping: each entry should follow the \"auth_scheme:module_path\" syntax, e.g. "
          "\"saml-entra-id:usr/lib/saml.py\"!");
    }
    const auto scheme_name = std::string{utils::Trim(module_and_scheme[0])};

    const auto n_values_provided = module_and_scheme.size();
    const auto use_default = n_values_provided == 1 && DEFAULT_SSO_MAPPINGS.contains(scheme_name);
    if (n_values_provided != 2 && !use_default) {
      throw auth::AuthException(
          "Entries in the auth module mapping follow the \"auth_scheme:module_path\" syntax, e.g. "
          "\"saml-entra-id:usr/lib/saml.py\"!");
    }
    const auto module_path =
        std::string{use_default ? DEFAULT_SSO_MAPPINGS.at(scheme_name) : utils::Trim(module_and_scheme[1])};
    module_per_scheme.emplace(scheme_name, module_path);
  }
  return module_per_scheme;
}
}  // namespace memgraph

// DEPRECATED FLAGS
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_HIDDEN_string(
    auth_module_executable, "", "Absolute path to the auth module executable that should be used.", {
      spdlog::warn(
          "The auth-module-executable flag is deprecated and superseded by auth-module-mappings. "
          "To switch to the up-to-date flag, start Memgraph with auth-module-mappings=basic:{your module's path}.");
      if (value.empty()) return true;
      // Check the file status, following symlinks.
      auto status = std::filesystem::status(value);
      if (!std::filesystem::is_regular_file(status)) {
        std::cerr << "The auth module path doesn't exist or isn't a file!\n";
        return false;
      }
      return true;
    });
namespace memgraph::auth {

const Auth::Epoch Auth::kStartEpoch = 1;

namespace {
#ifdef MG_ENTERPRISE
/**
 * REPLICATION SYSTEM ACTION IMPLEMENTATIONS
 */
struct UpdateAuthData : memgraph::system::ISystemAction {
  explicit UpdateAuthData(User user) : user_{std::move(user)}, role_{std::nullopt} {}
  explicit UpdateAuthData(Role role) : user_{std::nullopt}, role_{std::move(role)} {}

  void DoDurability() override { /* Done during Auth execution */
  }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const replication::UpdateAuthDataRes &response) { return response.success; };
    if (user_) {
      return client.StreamAndFinalizeDelta<replication::UpdateAuthDataRpc>(
          check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), *user_);
    }
    if (role_) {
      return client.StreamAndFinalizeDelta<replication::UpdateAuthDataRpc>(
          check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), *role_);
    }
    // Should never get here
    MG_ASSERT(false, "Trying to update auth data that is not a user nor a role");
    return {};
  }

  void PostReplication(replication::RoleMainData &mainData) const override {}

 private:
  std::optional<User> user_;
  std::optional<Role> role_;
};

struct DropAuthData : memgraph::system::ISystemAction {
  enum class AuthDataType { USER, ROLE };

  explicit DropAuthData(AuthDataType type, std::string_view name) : type_{type}, name_{name} {}

  void DoDurability() override { /* Done during Auth execution */
  }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check_response = [](const replication::DropAuthDataRes &response) { return response.success; };

    memgraph::replication::DropAuthDataReq::DataType type{};
    switch (type_) {
      case AuthDataType::USER:
        type = memgraph::replication::DropAuthDataReq::DataType::USER;
        break;
      case AuthDataType::ROLE:
        type = memgraph::replication::DropAuthDataReq::DataType::ROLE;
        break;
    }
    return client.StreamAndFinalizeDelta<replication::DropAuthDataRpc>(
        check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), type, name_);
  }
  void PostReplication(replication::RoleMainData &mainData) const override {}

 private:
  AuthDataType type_;
  std::string name_;
};
#endif

/**
 * CONSTANTS
 */
const std::string kUserPrefix = "user:";
const std::string kRolePrefix = "role:";
const std::string kLinkPrefix = "link:";
const std::string kMtLinkPrefix = "mtlink:";
const std::string kVersion = "version";

static constexpr auto kVersionV1 = "V1";
static constexpr auto kVersionV2 = "V2";
}  // namespace

/**
 * All data stored in the `Auth` storage is stored in an underlying
 * `kvstore::KVStore`. Because we are using a key-value store to store the data,
 * the data has to be encoded. The encoding used is as follows:
 *
 * User: key="user:<username>", value="<json_encoded_members_of_user>"
 * Role: key="role:<rolename>", value="<json_endoded_members_of_role>"
 *
 * The User->Role relationship isn't stored in the `User` encoded data because
 * we want to be able to delete/modify a Role and have it automatically be
 * removed/modified in all linked users. Because of that we store the links to
 * the role as a foreign-key like mapping in the KVStore. It is saved as
 * follows:
 *
 * key="link:<username>", value="<rolename>"
 */

namespace {
void MigrateVersions(kvstore::KVStore &store) {
  static constexpr auto kPasswordHashV0V1 = "password_hash";
  auto version_str = store.Get(kVersion);

  if (!version_str) {
    using namespace std::string_literals;

    // pre versioning, add version to the store
    auto puts = std::map<std::string, std::string>{{kVersion, kVersionV1}};

    // also add hash kind into durability

    auto it = store.begin(kUserPrefix);
    auto const e = store.end(kUserPrefix);

    if (it != e) {
      const auto hash_algo = CurrentHashAlgorithm();
      spdlog::info("Updating auth durability, assuming previously stored as {}", AsString(hash_algo));

      for (; it != e; ++it) {
        auto const &[key, value] = *it;
        try {
          auto user_data = nlohmann::json::parse(value);

          auto password_hash = user_data[kPasswordHashV0V1];
          if (!password_hash.is_string()) {
            throw AuthException("Couldn't load user data!");
          }
          // upgrade the password_hash to include the hash algortihm
          if (password_hash.empty()) {
            user_data[kPasswordHashV0V1] = nullptr;
          } else {
            user_data[kPasswordHashV0V1] = HashedPassword{hash_algo, password_hash};
          }
          puts.emplace(key, user_data.dump());
        } catch (const nlohmann::json::parse_error &e) {
          throw AuthException("Couldn't load user data!");
        }
      }
    }

    // Perform migration to V1
    store.PutMultiple(puts);
    version_str = kVersionV1;
  }

  // Migrate from V1 to V2: convert single role links to JSON arrays
  if (version_str == kVersionV1) {
    spdlog::info("Migrating auth storage from V1 to V2: converting single role links to JSON arrays");

    auto puts = std::map<std::string, std::string>{{kVersion, kVersionV2}};
    auto deletes = std::vector<std::string>{};

    // Migrate all link entries from single role format to JSON array format
    for (auto it = store.begin(kLinkPrefix); it != store.end(kLinkPrefix); ++it) {
      auto const &[key, value] = *it;

      // Convert old format (single role name) to new format (JSON array)
      if (!value.empty()) {
        nlohmann::json roles_array = nlohmann::json::array();
        roles_array.push_back(value);
        puts.emplace(key, roles_array.dump());
      } else {
        // Empty value, delete the link
        deletes.push_back(key);
      }
    }

    // Apply the migration
    if (!puts.empty()) {
      store.PutMultiple(puts);
    }
    if (!deletes.empty()) {
      store.DeleteMultiple(deletes);
    }
    version_str = kVersionV2;

    spdlog::info("Auth storage migration to V2 completed successfully");
  }
}

std::unordered_map<std::string, auth::Module> PopulateModules(std::string_view module_mappings) {
  std::unordered_map<std::string, auth::Module> module_per_scheme;
  if (!FLAGS_auth_module_executable.empty()) {
    module_per_scheme.emplace("basic", FLAGS_auth_module_executable);
  }

  if (module_mappings.empty()) {
    return module_per_scheme;
  }

  for (const auto &[scheme, module_path] : ModuleMappingsToMap(module_mappings)) {
    module_per_scheme.emplace(scheme, module_path);
  }
  return module_per_scheme;
}

auto ParseJson(std::string_view str) {
  nlohmann::json data;
  try {
    data = nlohmann::json::parse(str);
  } catch (const nlohmann::json::parse_error &e) {
    throw AuthException("Couldn't load auth data!");
  }
  return data;
}

};  // namespace

Auth::Auth(std::string storage_directory, Config config)
    : storage_(std::move(storage_directory)), config_{std::move(config)} {
  modules_ = PopulateModules(FLAGS_auth_module_mappings);
  if (storage_.Size() > 0) {
    MigrateVersions(storage_);
  } else {
    // Clean storage; put the version
    storage_.Put(kVersion, kVersionV2);
  }
}

std::optional<UserOrRole> Auth::CallExternalModule(const std::string &scheme, const nlohmann::json &module_params,
                                                   std::optional<std::string> provided_username) {
  spdlog::trace("Calling external auth module for scheme '{}'.", scheme);
  auto ret = modules_.at(scheme).Call(module_params, FLAGS_auth_module_timeout_ms);

  auto get_errors = [&ret]() -> std::string {
    std::string default_error = "Couldn't authenticate user: check stderr for auth module error messages.";
    if (ret.find("errors") == ret.end()) {
      return default_error;
    }
    const auto &ret_errors = ret.at("errors");
    if (!ret_errors.is_string()) {
      return "Couldn't authenticate user: the error message returned by the auth module needs to be a string value.";
    }
    const auto &errors = ret_errors.get<std::string>();
    return errors;
  };

  auto get_string_field = [&ret](const auto &name) -> std::optional<std::string> {
    if (ret.find(name) == ret.end()) {
      spdlog::warn(utils::MessageWithLink(
          "Couldn't authenticate user: the field \"{}\" was not returned by the external auth module.", name,
          "https://memgr.ph/sso"));
      return std::nullopt;
    }

    const auto &ret_field = ret.at(name);
    if (!ret_field.is_string()) {
      spdlog::warn(
          utils::MessageWithLink("Couldn't authenticate user: the field \"{}\" returned by the external auth module "
                                 "needs to have a string value.",
                                 name, "https://memgr.ph/sso"));
      return std::nullopt;
    }

    return ret_field.template get<std::string>();
  };

  if (!ret.is_object() || ret.find("authenticated") == ret.end()) {
    spdlog::warn(
        utils::MessageWithLink("Couldn't authenticate user: the message returned by the external auth module needs to "
                               "be an object with the success status in the \"authenticated\" field.",
                               "https://memgr.ph/sso"));
    return std::nullopt;
  }
  const auto &ret_authenticated = ret.at("authenticated");
  if (!ret_authenticated.is_boolean()) {
    spdlog::warn(utils::MessageWithLink(
        "Couldn't authenticate user: the authentication status returned by the external auth module "
        "needs to be a boolean value.",
        "https://memgr.ph/sso"));
    return std::nullopt;
  }
  const auto is_authenticated = ret_authenticated.get<bool>();

  if (!is_authenticated) {
    const auto error = get_errors();
    spdlog::warn(utils::MessageWithLink("Couldn't authenticate user:", error, "https://memgr.ph/sso"));
    return std::nullopt;
  }

  // Handle both single role and multiple roles
  std::vector<std::string> role_names;

  // Check for "roles" field first (multiple roles)
  if (ret.find("roles") != ret.end()) {
    const auto &roles_field = ret.at("roles");
    if (roles_field.is_array()) {
      for (const auto &role_name : roles_field) {
        if (role_name.is_string()) {
          role_names.push_back(role_name.get<std::string>());
        }
      }
    } else if (roles_field.is_string()) {
      role_names.push_back(roles_field.get<std::string>());
    }
  }

  // Fall back to single "role" field for backward compatibility
  if (role_names.empty()) {
    const auto rolename = get_string_field("role");
    if (!rolename) {
      spdlog::warn(utils::MessageWithLink(
          "Couldn't authenticate external user because the role was not returned by the auth module.",
          "https://memgr.ph/auth"));
      return std::nullopt;
    }
    role_names.push_back(*rolename);
  }

  if (role_names.empty()) {
    spdlog::warn(utils::MessageWithLink(
        "Couldn't authenticate user: no valid role(s) returned by the external auth module.", "https://memgr.ph/sso"));
    return std::nullopt;
  }

  // Get all the roles
  auth::Roles roles;
  for (const auto &role_name : role_names) {
    auto role = GetRole(role_name);
    if (!role) {
      spdlog::warn(utils::MessageWithLink("Couldn't authenticate external user because the role {} doesn't exist.",
                                          role_name, "https://memgr.ph/auth"));
      return std::nullopt;
    }
    roles.AddRole(*role);
  }

  auto username = provided_username.has_value() ? provided_username : get_string_field("username");
  if (!username) {
    spdlog::warn(utils::MessageWithLink(
        "Couldn't authenticate external user because the username was not returned by the auth module.",
        "https://memgr.ph/auth"));
    return std::nullopt;
  }

  auto already_existing_user = GetUser(*username);
  if (already_existing_user) {
    spdlog::warn(utils::MessageWithLink(
        "Couldn't authenticate external user because a local user {} with the same name already exists.", *username,
        "https://memgr.ph/auth"));
    return std::nullopt;
  }

  spdlog::trace("Authenticated user '{}' with roles: {}.", *username, fmt::join(roles.rolenames(), ", "));
  return UserOrRole(auth::RoleWUsername{*username, roles});
}

std::optional<UserOrRole> Auth::Authenticate(const std::string &username, const std::string &password) {
  if (!modules_.contains("basic")) {
    /*
     * LOCAL AUTH STORAGE
     */
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
    if (user->UpgradeHash(password)) {
      SaveUser(*user);
    }

    return user;
  }

  if (!HasAuthModulePrerequisites("basic")) {
    return std::nullopt;
  }

  nlohmann::json params = nlohmann::json::object();
  params["username"] = username;
  params["password"] = password;

  return CallExternalModule("basic", params, username);
}

std::optional<UserOrRole> Auth::SSOAuthenticate(const std::string &scheme,
                                                const std::string &identity_provider_response) {
  if (!HasAuthModulePrerequisites(scheme)) {
    return std::nullopt;
  }

  nlohmann::json params = nlohmann::json::object();
  params["scheme"] = scheme;
  params["response"] = identity_provider_response;

  return CallExternalModule(scheme, params);
}

void Auth::LinkUser(User &user) const {
  // User set roles on particular databases
  // NOTE Has to be done in this order, otherwise the global roles will overwrite the multi-tenant roles
  [[maybe_unused]] std::unordered_set<std::string> failed_mt_roles;
#ifdef MG_ENTERPRISE
  auto mt_link = storage_.Get(kMtLinkPrefix + user.username());
  if (mt_link) {
    try {
      auto json_data = ParseJson(*mt_link);
      if (!json_data.is_object()) {
        spdlog::warn("Found invalid JSON in mtlink format for user '{}'", user.username());
        return;
      }
      for (const auto &[db, roles_array] : json_data.items()) {
        if (!roles_array.is_array()) {
          spdlog::warn("Invalid mtlink entry for user '{}': expected array of rolenames for db '{}'", user.username(),
                       db);
          continue;
        }
        for (const auto &rolename_json : roles_array) {
          if (!rolename_json.is_string()) {
            spdlog::warn("Invalid mtlink entry for user '{}': expected string rolename for db '{}'", user.username(),
                         db);
            continue;
          }
          const auto &rolename = rolename_json.get<std::string>();
          auto role = GetRole(rolename);
          if (!role) {
            spdlog::warn("Role '{}' doesn't exist for user '{}'", rolename, user.username());
            continue;
          }
          try {
            user.AddMultiTenantRole(*role, db);
          } catch (const AuthException &e) {
            spdlog::warn("Couldn't add multi-tenant role '{}' to user '{}' on database '{}': {}", rolename,
                         user.username(), db, e.what());
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
  auto link = storage_.Get(kLinkPrefix + user.username());
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
          auto role = GetRole(role_name.get<std::string>());
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
}

std::optional<User> Auth::GetUser(const std::string &username_orig) const {
  auto username = utils::ToLowerCase(username_orig);
  auto existing_user = storage_.Get(kUserPrefix + username);
  if (!existing_user) return std::nullopt;

  auto user = User::Deserialize(ParseJson(*existing_user));
  LinkUser(user);
  return user;
}

void Auth::SaveUser(const User &user, system::Transaction *system_tx) {
  bool success = false;
  std::map<std::string, std::string> puts;
  std::vector<std::string> deletes;

  // Always store the user data
  puts.emplace(kUserPrefix + user.username(), user.Serialize().dump());

  // Store regular roles
  if (!user.roles().empty()) {
    // Store all roles as a JSON array
    nlohmann::json roles_array = nlohmann::json::array();
    for (const auto &role : user.roles()) {
      roles_array.push_back(role.rolename());
    }
    puts.emplace(kLinkPrefix + user.username(), roles_array.dump());
  } else {
    deletes.push_back(kLinkPrefix + user.username());
  }

#ifdef MG_ENTERPRISE
  // Store multi-tenant roles
  const auto &mt_mappings = user.GetMultiTenantRoleMappings();
  if (!mt_mappings.empty()) {
    nlohmann::json mt_links;
    for (const auto &[db_name, role_names] : mt_mappings) {
      mt_links[db_name] = nlohmann::json::array();
      for (const auto &role_name : role_names) {
        mt_links[db_name].push_back(role_name);
      }
    }
    puts.emplace(kMtLinkPrefix + user.username(), mt_links.dump());
  } else {
    deletes.push_back(kMtLinkPrefix + user.username());
  }
#endif

  // Perform the storage operations
  success = storage_.PutAndDeleteMultiple(puts, deletes);
  if (!success) {
    throw AuthException("Couldn't save user '{}'!", user.username());
  }

  // Durability updated -> new epoch
  UpdateEpoch();

  // All changes to the user end up calling this function, so no need to add a delta anywhere else
  if (system_tx) {
#ifdef MG_ENTERPRISE
    system_tx->AddAction<UpdateAuthData>(user);
#endif
  }
}

void Auth::UpdatePassword(auth::User &user, const std::optional<std::string> &password) {
  // Check if user passed in an already hashed string
  if (password) {
    const auto already_hashed = UserDefinedHash(*password);
    if (already_hashed) {
      // TODO: Do this?
      // if (config_.custom_password_regex || config_.password_permit_null) {
      //   throw AuthException("Passing in a hash is not allowed when password format restrictions are defined.");
      // }
      user.UpdateHash(*already_hashed);
      return;
    }
  }

  // Check if null
  if (!password) {
    if (!config_.password_permit_null) {
      throw AuthException("Null passwords aren't permitted!");
    }
  } else {
    // Check if compliant with our filter
    if (config_.custom_password_regex) {
      if (const auto license_check_result = license::global_license_checker.IsEnterpriseValid(utils::global_settings);
          license_check_result.HasError()) {
        throw AuthException(
            "Custom password regex is a Memgraph Enterprise feature. Please set the config "
            "(\"--auth-password-strength-regex\") to its default value (\"{}\") or remove the flag.\n{}",
            glue::kDefaultPasswordRegex,
            license::LicenseCheckErrorToString(license_check_result.GetError(), "password regex"));
      }
    }
    if (!std::regex_match(*password, config_.password_regex)) {
      throw AuthException(
          "The user password doesn't conform to the required strength! Regex: "
          "\"{}\"",
          config_.password_regex_str);
    }
  }

  // All checks passed; update
  user.UpdatePassword(password);
}

std::optional<User> Auth::AddUser(const std::string &username, const std::optional<std::string> &password,
                                  system::Transaction *system_tx) {
  if (!NameRegexMatch(username)) {
    throw AuthException("Invalid user name.");
  }
  auto existing_user = GetUser(username);
  if (existing_user) return std::nullopt;
  auto existing_role = GetRole(username);
  if (existing_role) return std::nullopt;
  auto new_user = User(username);
  UpdatePassword(new_user, password);
  SaveUser(new_user, system_tx);
  return new_user;
}

bool Auth::RemoveUser(const std::string &username_orig, system::Transaction *system_tx) {
  auto username = utils::ToLowerCase(username_orig);
  if (!storage_.Get(kUserPrefix + username)) return false;
  std::vector<std::string> keys({kMtLinkPrefix + username, kLinkPrefix + username, kUserPrefix + username});
  if (!storage_.DeleteMultiple(keys)) {
    throw AuthException("Couldn't remove user '{}'!", username);
  }

  // Durability updated -> new epoch
  UpdateEpoch();

  // Handling drop user delta
  if (system_tx) {
#ifdef MG_ENTERPRISE
    system_tx->AddAction<DropAuthData>(DropAuthData::AuthDataType::USER, username);
#endif
  }
  return true;
}

std::vector<auth::User> Auth::AllUsers() const {
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kUserPrefix); it != storage_.end(kUserPrefix); ++it) {
    auto username = it->first.substr(kUserPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    try {
      User user = auth::User::Deserialize(ParseJson(it->second));  // Will throw on failure
      LinkUser(user);
      ret.emplace_back(std::move(user));
    } catch (AuthException &) {
      continue;
    }
  }
  return ret;
}

std::vector<std::string> Auth::AllUsernames() const {
  std::vector<std::string> ret;
  for (auto it = storage_.begin(kUserPrefix); it != storage_.end(kUserPrefix); ++it) {
    auto username = it->first.substr(kUserPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    try {
      // Check if serialized correctly
      memgraph::auth::User::Deserialize(ParseJson(it->second));  // Will throw on failure
      ret.emplace_back(std::move(username));
    } catch (AuthException &) {
      continue;
    }
  }
  return ret;
}

bool Auth::HasUsers() const { return storage_.begin(kUserPrefix) != storage_.end(kUserPrefix); }

bool Auth::AccessControlled() const { return HasUsers() || UsingAuthModule(); }

std::optional<Role> Auth::GetRole(const std::string &rolename_orig) const {
  auto rolename = utils::ToLowerCase(rolename_orig);
  auto existing_role = storage_.Get(kRolePrefix + rolename);
  if (!existing_role) return std::nullopt;

  return Role::Deserialize(ParseJson(*existing_role));
}

void Auth::SaveRole(const Role &role, system::Transaction *system_tx) {
  if (!storage_.Put(kRolePrefix + role.rolename(), role.Serialize().dump())) {
    throw AuthException("Couldn't save role '{}'!", role.rolename());
  }

  // Durability updated -> new epoch
  UpdateEpoch();

  // All changes to the role end up calling this function, so no need to add a delta anywhere else
  if (system_tx) {
#ifdef MG_ENTERPRISE
    system_tx->AddAction<UpdateAuthData>(role);
#endif
  }
}

std::optional<Role> Auth::AddRole(const std::string &rolename, system::Transaction *system_tx) {
  if (!NameRegexMatch(rolename)) {
    throw AuthException("Invalid role name.");
  }
  if (auto existing_role = GetRole(rolename)) return std::nullopt;
  if (auto existing_user = GetUser(rolename)) return std::nullopt;
  auto new_role = Role(rolename);
  SaveRole(new_role, system_tx);
  return new_role;
}

bool Auth::RemoveRole(const std::string &rolename_orig, system::Transaction *system_tx) {
  auto rolename = utils::ToLowerCase(rolename_orig);
  if (!storage_.Get(kRolePrefix + rolename)) return false;

  // First, remove the role from all users who have it
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix); ++it) {
    auto username = it->first.substr(kLinkPrefix.size());
    bool needs_update = false;
    nlohmann::json updated_roles;

    try {
      // Parse as JSON array (V2 format)
      auto json_data = ParseJson(it->second);
      if (!json_data.is_array()) {
        spdlog::warn("Found invalid JSON in link format for user '{}'", username);
        continue;
      }
      // V2 format: remove the role from the array
      updated_roles = nlohmann::json::array();
      for (const auto &role_name : json_data) {
        if (role_name.is_string() && utils::ToLowerCase(role_name.get<std::string>()) != rolename) {
          updated_roles.push_back(role_name);
        } else if (role_name.is_string() && utils::ToLowerCase(role_name.get<std::string>()) == rolename) {
          needs_update = true;
        }
      }
    } catch (const nlohmann::detail::exception &) {
      // This shouldn't happen after V2 migration, but handle gracefully
      spdlog::warn("Found invalid JSON in link format for user '{}', treating as single role", username);
      continue;
    }

    if (needs_update) {
      if (updated_roles.is_null()) {
        // Remove the link entirely (old format or empty array)
        storage_.Delete(it->first);
      } else if (updated_roles.empty()) {
        // Remove the link entirely (empty array)
        storage_.Delete(it->first);
      } else {
        // Update with the remaining roles
        storage_.Put(it->first, updated_roles.dump());
      }
    }
  }

  for (auto it = storage_.begin(kMtLinkPrefix); it != storage_.end(kMtLinkPrefix); ++it) {
    auto username = it->first.substr(kMtLinkPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    auto json_data = ParseJson(it->second);
    bool update = false;
    for (auto &[db_name, role_names] : json_data.items()) {
      if (role_names.is_array()) {
        // Find and remove the role by index
        for (size_t i = 0; i < role_names.size(); ++i) {
          if (role_names[i].is_string() && utils::ToLowerCase(role_names[i].get<std::string>()) == rolename) {
            role_names.erase(i);
            update = true;
            break;  // Remove only the first occurrence
          }
        }
      }
    }
    if (update) {
      if (json_data.empty()) {
        storage_.Delete(it->first);
      } else {
        storage_.Put(it->first, json_data.dump());
      }
    }
  }

  // Then remove the role itself
  if (!storage_.Delete(kRolePrefix + rolename)) {
    throw AuthException("Couldn't remove role '{}'!", rolename);
  }

  // Durability updated -> new epoch
  UpdateEpoch();

  // Handling drop role delta
  if (system_tx) {
#ifdef MG_ENTERPRISE
    system_tx->AddAction<DropAuthData>(DropAuthData::AuthDataType::ROLE, rolename);
#endif
  }
  return true;
}

std::vector<auth::Role> Auth::AllRoles() const {
  std::vector<auth::Role> ret;
  for (auto it = storage_.begin(kRolePrefix); it != storage_.end(kRolePrefix); ++it) {
    auto rolename = it->first.substr(kRolePrefix.size());
    if (rolename != utils::ToLowerCase(rolename)) continue;
    Role role = memgraph::auth::Role::Deserialize(ParseJson(it->second));  // Will throw on failure
    ret.emplace_back(std::move(role));
  }
  return ret;
}

std::vector<std::string> Auth::AllRolenames() const {
  std::vector<std::string> ret;
  for (auto it = storage_.begin(kRolePrefix); it != storage_.end(kRolePrefix); ++it) {
    auto rolename = it->first.substr(kRolePrefix.size());
    if (rolename != utils::ToLowerCase(rolename)) continue;
    try {
      // Check that the data is serialized correctly
      memgraph::auth::Role::Deserialize(ParseJson(it->second));
      ret.emplace_back(std::move(rolename));
    } catch (AuthException &) {
      continue;
    }
  }
  return ret;
}

std::vector<auth::User> Auth::AllUsersForRole(const std::string &rolename_orig) const {
  const auto rolename = utils::ToLowerCase(rolename_orig);
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix); ++it) {
    auto username = it->first.substr(kLinkPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;

    bool has_role = false;
    try {
      // Parse as JSON array (V2 format)
      auto json_data = ParseJson(it->second);
      if (!json_data.is_array()) {
        spdlog::warn("Found non-array link format for user '{}'", username);
        continue;
      }
      // V2 format: check if role is in the array
      for (const auto &role_name : json_data) {
        if (role_name.is_string() && utils::ToLowerCase(role_name.get<std::string>()) == rolename) {
          has_role = true;
          break;
        }
      }
    } catch (const nlohmann::detail::exception &) {
      // This shouldn't happen after V2 migration, but handle gracefully
      spdlog::warn("Found invalid JSON in link format for user '{}', treating as single role", username);
      continue;
    }

    if (has_role) {
      if (auto user = GetUser(username)) {
        ret.push_back(std::move(*user));
      } else {
        throw AuthException("Couldn't load user '{}'!", username);
      }
    }
  }
  return ret;
}

#ifdef MG_ENTERPRISE
Auth::Result Auth::GrantDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx) {
  using enum Auth::Result;
  if (auto user = GetUser(name)) {
    GrantDatabase(db, *user, system_tx);
    return SUCCESS;
  }
  if (auto role = GetRole(name)) {
    GrantDatabase(db, *role, system_tx);
    return SUCCESS;
  }
  return NO_USER_ROLE;
}

void Auth::GrantDatabase(const std::string &db, User &user, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    user.db_access().GrantAll();
  } else {
    user.db_access().Grant(db);
  }
  SaveUser(user, system_tx);
}

void Auth::GrantDatabase(const std::string &db, Role &role, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    role.db_access().GrantAll();
  } else {
    role.db_access().Grant(db);
  }
  SaveRole(role, system_tx);
}

Auth::Result Auth::DenyDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx) {
  using enum Auth::Result;
  if (auto user = GetUser(name)) {
    DenyDatabase(db, *user, system_tx);
    return SUCCESS;
  }
  if (auto role = GetRole(name)) {
    DenyDatabase(db, *role, system_tx);
    return SUCCESS;
  }
  return NO_USER_ROLE;
}

void Auth::DenyDatabase(const std::string &db, User &user, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    user.db_access().DenyAll();
  } else {
    user.db_access().Deny(db);
  }
  SaveUser(user, system_tx);
}

void Auth::DenyDatabase(const std::string &db, Role &role, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    role.db_access().DenyAll();
  } else {
    role.db_access().Deny(db);
  }
  SaveRole(role, system_tx);
}

Auth::Result Auth::RevokeDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx) {
  using enum Auth::Result;
  if (auto user = GetUser(name)) {
    RevokeDatabase(db, *user, system_tx);
    return SUCCESS;
  }
  if (auto role = GetRole(name)) {
    RevokeDatabase(db, *role, system_tx);
    return SUCCESS;
  }
  return NO_USER_ROLE;
}

void Auth::RevokeDatabase(const std::string &db, User &user, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    user.db_access().RevokeAll();
  } else {
    user.db_access().Revoke(db);
  }
  SaveUser(user, system_tx);
}

void Auth::RevokeDatabase(const std::string &db, Role &role, system::Transaction *system_tx) {
  if (db == kAllDatabases) {
    role.db_access().RevokeAll();
  } else {
    role.db_access().Revoke(db);
  }
  SaveRole(role, system_tx);
}

void Auth::DeleteDatabase(const std::string &db, system::Transaction *system_tx) {
  for (auto it = storage_.begin(kUserPrefix); it != storage_.end(kUserPrefix); ++it) {
    auto username = it->first.substr(kUserPrefix.size());
    try {
      User user = auth::User::Deserialize(ParseJson(it->second));
      LinkUser(user);
      user.db_access().Revoke(db);
      SaveUser(user, system_tx);
    } catch (AuthException &) {
      continue;
    }
  }
  for (auto it = storage_.begin(kRolePrefix); it != storage_.end(kRolePrefix); ++it) {
    auto rolename = it->first.substr(kRolePrefix.size());
    try {
      auto role = memgraph::auth::Role::Deserialize(ParseJson(it->second));
      role.db_access().Revoke(db);
      SaveRole(role, system_tx);
    } catch (AuthException &) {
      continue;
    }
  }
}

Auth::Result Auth::SetMainDatabase(std::string_view db, const std::string &name, system::Transaction *system_tx) {
  using enum Auth::Result;
  if (auto user = GetUser(name)) {
    SetMainDatabase(db, *user, system_tx);
    return SUCCESS;
  }
  if (auto role = GetRole(name)) {
    SetMainDatabase(db, *role, system_tx);
    return SUCCESS;
  }
  return NO_USER_ROLE;
}

void Auth::SetMainDatabase(std::string_view db, User &user, system::Transaction *system_tx) {
  if (!user.db_access().SetMain(db)) {
    throw AuthException("Couldn't set default database '{}' for '{}'!", db, user.username());
  }
  SaveUser(user, system_tx);
}

void Auth::SetMainDatabase(std::string_view db, Role &role, system::Transaction *system_tx) {
  if (!role.db_access().SetMain(db)) {
    throw AuthException("Couldn't set default database '{}' for '{}'!", db, role.rolename());
  }
  SaveRole(role, system_tx);
}
#endif

bool Auth::NameRegexMatch(const std::string &user_or_role) const {
  if (config_.custom_name_regex) {
    if (const auto license_check_result =
            memgraph::license::global_license_checker.IsEnterpriseValid(memgraph::utils::global_settings);
        license_check_result.HasError()) {
      throw memgraph::auth::AuthException(
          "Custom user/role regex is a Memgraph Enterprise feature. Please set the config "
          "(\"--auth-user-or-role-name-regex\") to its default value (\"{}\") or remove the flag.\n{}",
          glue::kDefaultUserRoleRegex,
          memgraph::license::LicenseCheckErrorToString(license_check_result.GetError(), "user/role regex"));
    }
  }
  return std::regex_match(user_or_role, config_.name_regex);
}

}  // namespace memgraph::auth
