// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/models.hpp"

#include <cstdint>
#include <regex>

#include <gflags/gflags.h>

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "dbms/constants.hpp"
#include "license/license.hpp"
#include "query/constants.hpp"
#include "spdlog/spdlog.h"
#include "utils/cast.hpp"
#include "utils/logging.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(auth_password_permit_null, true, "Set to false to disable null passwords.");

inline constexpr std::string_view default_password_regex = ".+";
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(auth_password_strength_regex, default_password_regex.data(),
              "The regular expression that should be used to match the entire "
              "entered password to ensure its strength.");

namespace memgraph::auth {
namespace {

// Constant list of all available permissions.
const std::vector<Permission> kPermissionsAll = {Permission::MATCH,
                                                 Permission::CREATE,
                                                 Permission::MERGE,
                                                 Permission::DELETE,
                                                 Permission::SET,
                                                 Permission::REMOVE,
                                                 Permission::INDEX,
                                                 Permission::STATS,
                                                 Permission::CONSTRAINT,
                                                 Permission::DUMP,
                                                 Permission::AUTH,
                                                 Permission::REPLICATION,
                                                 Permission::DURABILITY,
                                                 Permission::READ_FILE,
                                                 Permission::FREE_MEMORY,
                                                 Permission::TRIGGER,
                                                 Permission::CONFIG,
                                                 Permission::STREAM,
                                                 Permission::MODULE_READ,
                                                 Permission::MODULE_WRITE,
                                                 Permission::WEBSOCKET,
                                                 Permission::TRANSACTION_MANAGEMENT,
                                                 Permission::STORAGE_MODE,
                                                 Permission::MULTI_DATABASE_EDIT,
                                                 Permission::MULTI_DATABASE_USE};

}  // namespace

std::string PermissionToString(Permission permission) {
  switch (permission) {
    case Permission::MATCH:
      return "MATCH";
    case Permission::CREATE:
      return "CREATE";
    case Permission::MERGE:
      return "MERGE";
    case Permission::DELETE:
      return "DELETE";
    case Permission::SET:
      return "SET";
    case Permission::REMOVE:
      return "REMOVE";
    case Permission::INDEX:
      return "INDEX";
    case Permission::STATS:
      return "STATS";
    case Permission::CONSTRAINT:
      return "CONSTRAINT";
    case Permission::DUMP:
      return "DUMP";
    case Permission::REPLICATION:
      return "REPLICATION";
    case Permission::DURABILITY:
      return "DURABILITY";
    case Permission::READ_FILE:
      return "READ_FILE";
    case Permission::FREE_MEMORY:
      return "FREE_MEMORY";
    case Permission::TRIGGER:
      return "TRIGGER";
    case Permission::CONFIG:
      return "CONFIG";
    case Permission::AUTH:
      return "AUTH";
    case Permission::STREAM:
      return "STREAM";
    case Permission::MODULE_READ:
      return "MODULE_READ";
    case Permission::MODULE_WRITE:
      return "MODULE_WRITE";
    case Permission::WEBSOCKET:
      return "WEBSOCKET";
    case Permission::TRANSACTION_MANAGEMENT:
      return "TRANSACTION_MANAGEMENT";
    case Permission::STORAGE_MODE:
      return "STORAGE_MODE";
    case Permission::MULTI_DATABASE_EDIT:
      return "MULTI_DATABASE_EDIT";
    case Permission::MULTI_DATABASE_USE:
      return "MULTI_DATABASE_USE";
  }
}

std::string PermissionLevelToString(PermissionLevel level) {
  switch (level) {
    case PermissionLevel::GRANT:
      return "GRANT";
    case PermissionLevel::NEUTRAL:
      return "NEUTRAL";
    case PermissionLevel::DENY:
      return "DENY";
  }
}

#ifdef MG_ENTERPRISE
FineGrainedPermission PermissionToFineGrainedPermission(const uint64_t permission) {
  if (permission & FineGrainedPermission::CREATE_DELETE) {
    return FineGrainedPermission::CREATE_DELETE;
  }

  if (permission & FineGrainedPermission::UPDATE) {
    return FineGrainedPermission::UPDATE;
  }

  if (permission & FineGrainedPermission::READ) {
    return FineGrainedPermission::READ;
  }

  return FineGrainedPermission::NOTHING;
}

std::string FineGrainedPermissionToString(const FineGrainedPermission level) {
  switch (level) {
    case FineGrainedPermission::CREATE_DELETE:
      return "CREATE_DELETE";
    case FineGrainedPermission::UPDATE:
      return "UPDATE";
    case FineGrainedPermission::READ:
      return "READ";
    case FineGrainedPermission::NOTHING:
      return "NOTHING";
  }
}

FineGrainedAccessPermissions Merge(const FineGrainedAccessPermissions &first,
                                   const FineGrainedAccessPermissions &second) {
  std::unordered_map<std::string, uint64_t> permissions{first.GetPermissions()};
  std::optional<uint64_t> global_permission;

  if (second.GetGlobalPermission().has_value()) {
    global_permission = *second.GetGlobalPermission();
  } else if (first.GetGlobalPermission().has_value()) {
    global_permission = *first.GetGlobalPermission();
  }

  for (const auto &[label_name, permission] : second.GetPermissions()) {
    permissions[label_name] = permission;
  }

  return FineGrainedAccessPermissions(permissions, global_permission);
}
#endif

Permissions::Permissions(uint64_t grants, uint64_t denies) {
  // The deny bitmask has higher priority than the grant bitmask.
  denies_ = denies;
  // Mask out the grant bitmask to make sure that it is correct.
  grants_ = grants & (~denies);
}

PermissionLevel Permissions::Has(Permission permission) const {
  // Check for the deny first because it has greater priority than a grant.
  if (denies_ & utils::UnderlyingCast(permission)) {
    return PermissionLevel::DENY;
  }
  if (grants_ & utils::UnderlyingCast(permission)) {
    return PermissionLevel::GRANT;
  }
  return PermissionLevel::NEUTRAL;
}

void Permissions::Grant(Permission permission) {
  // Remove the possible deny.
  denies_ &= ~utils::UnderlyingCast(permission);
  // Now we grant the permission.
  grants_ |= utils::UnderlyingCast(permission);
}

void Permissions::Revoke(Permission permission) {
  // Remove the possible grant.
  grants_ &= ~utils::UnderlyingCast(permission);
  // Remove the possible deny.
  denies_ &= ~utils::UnderlyingCast(permission);
}

void Permissions::Deny(Permission permission) {
  // First deny the permission.
  denies_ |= utils::UnderlyingCast(permission);
  // Remove the possible grant.
  grants_ &= ~utils::UnderlyingCast(permission);
}

std::vector<Permission> Permissions::GetGrants() const {
  std::vector<Permission> ret;
  for (const auto &permission : kPermissionsAll) {
    if (Has(permission) == PermissionLevel::GRANT) {
      ret.push_back(permission);
    }
  }
  return ret;
}

std::vector<Permission> Permissions::GetDenies() const {
  std::vector<Permission> ret;
  for (const auto &permission : kPermissionsAll) {
    if (Has(permission) == PermissionLevel::DENY) {
      ret.push_back(permission);
    }
  }
  return ret;
}

nlohmann::json Permissions::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["grants"] = grants_;
  data["denies"] = denies_;
  return data;
}

Permissions Permissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
  }
  if (!data["grants"].is_number_unsigned() || !data["denies"].is_number_unsigned()) {
    throw AuthException("Couldn't load permissions data!");
  }
  return Permissions{data["grants"], data["denies"]};
}

uint64_t Permissions::grants() const { return grants_; }
uint64_t Permissions::denies() const { return denies_; }

bool operator==(const Permissions &first, const Permissions &second) {
  return first.grants() == second.grants() && first.denies() == second.denies();
}

bool operator!=(const Permissions &first, const Permissions &second) { return !(first == second); }

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions::FineGrainedAccessPermissions(const std::unordered_map<std::string, uint64_t> &permissions,
                                                           const std::optional<uint64_t> &global_permission)
    : permissions_(permissions), global_permission_(global_permission) {}

PermissionLevel FineGrainedAccessPermissions::Has(const std::string &permission,
                                                  const FineGrainedPermission fine_grained_permission) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return PermissionLevel::GRANT;
  }
  const auto concrete_permission = std::invoke([&]() -> uint64_t {
    if (permissions_.contains(permission)) {
      return permissions_.at(permission);
    }

    if (global_permission_.has_value()) {
      return global_permission_.value();
    }

    return 0;
  });

  const auto temp_permission = concrete_permission & fine_grained_permission;

  return temp_permission > 0 ? PermissionLevel::GRANT : PermissionLevel::DENY;
}

void FineGrainedAccessPermissions::Grant(const std::string &permission,
                                         const FineGrainedPermission fine_grained_permission) {
  if (permission == query::kAsterisk) {
    global_permission_ = CalculateGrant(fine_grained_permission);
  } else {
    permissions_[permission] = CalculateGrant(fine_grained_permission);
  }
}

void FineGrainedAccessPermissions::Revoke(const std::string &permission) {
  if (permission == query::kAsterisk) {
    permissions_.clear();
    global_permission_ = std::nullopt;
  } else {
    permissions_.erase(permission);
  }
}

nlohmann::json FineGrainedAccessPermissions::Serialize() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  nlohmann::json data = nlohmann::json::object();
  data["permissions"] = permissions_;
  data["global_permission"] = global_permission_.has_value() ? global_permission_.value() : -1;
  return data;
}

FineGrainedAccessPermissions FineGrainedAccessPermissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
  }
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }
  std::optional<uint64_t> global_permission;

  if (data["global_permission"].empty() || data["global_permission"] == -1) {
    global_permission = std::nullopt;
  } else {
    global_permission = data["global_permission"];
  }

  return FineGrainedAccessPermissions(data["permissions"], global_permission);
}

const std::unordered_map<std::string, uint64_t> &FineGrainedAccessPermissions::GetPermissions() const {
  return permissions_;
}
const std::optional<uint64_t> &FineGrainedAccessPermissions::GetGlobalPermission() const { return global_permission_; };

uint64_t FineGrainedAccessPermissions::CalculateGrant(FineGrainedPermission fine_grained_permission) {
  uint64_t shift{1};
  uint64_t result{0};
  auto uint_fine_grained_permission = static_cast<uint64_t>(fine_grained_permission);
  while (uint_fine_grained_permission > 0) {
    result |= uint_fine_grained_permission;
    uint_fine_grained_permission >>= shift;
  }

  return result;
}

bool operator==(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return first.GetPermissions() == second.GetPermissions() &&
         first.GetGlobalPermission() == second.GetGlobalPermission();
}

bool operator!=(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return !(first == second);
}

FineGrainedAccessHandler::FineGrainedAccessHandler(FineGrainedAccessPermissions labelPermissions,
                                                   FineGrainedAccessPermissions edgeTypePermissions)
    : label_permissions_(std::move(labelPermissions)), edge_type_permissions_(std::move(edgeTypePermissions)) {}

const FineGrainedAccessPermissions &FineGrainedAccessHandler::label_permissions() const { return label_permissions_; }
FineGrainedAccessPermissions &FineGrainedAccessHandler::label_permissions() { return label_permissions_; }

const FineGrainedAccessPermissions &FineGrainedAccessHandler::edge_type_permissions() const {
  return edge_type_permissions_;
}
FineGrainedAccessPermissions &FineGrainedAccessHandler::edge_type_permissions() { return edge_type_permissions_; }

nlohmann::json FineGrainedAccessHandler::Serialize() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  nlohmann::json data = nlohmann::json::object();
  data["label_permissions"] = label_permissions_.Serialize();
  data["edge_type_permissions"] = edge_type_permissions_.Serialize();
  return data;
}

FineGrainedAccessHandler FineGrainedAccessHandler::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  if (!data["label_permissions"].is_object() || !data["edge_type_permissions"].is_object()) {
    throw AuthException("Couldn't load label_permissions or edge_type_permissions data!");
  }
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessHandler{};
  }
  auto label_permissions = FineGrainedAccessPermissions::Deserialize(data["label_permissions"]);
  auto edge_type_permissions = FineGrainedAccessPermissions::Deserialize(data["edge_type_permissions"]);

  return FineGrainedAccessHandler(std::move(label_permissions), std::move(edge_type_permissions));
}

bool operator==(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second) {
  return first.label_permissions_ == second.label_permissions_ &&
         first.edge_type_permissions_ == second.edge_type_permissions_;
}

bool operator!=(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second) {
  return !(first == second);
}
#endif

Role::Role(const std::string &rolename) : rolename_(utils::ToLowerCase(rolename)) {}
Role::Role(const std::string &rolename, const Permissions &permissions)
    : rolename_(utils::ToLowerCase(rolename)), permissions_(permissions) {}
#ifdef MG_ENTERPRISE
Role::Role(const std::string &rolename, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler)
    : rolename_(utils::ToLowerCase(rolename)),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)) {}
#endif

const std::string &Role::rolename() const { return rolename_; }
const Permissions &Role::permissions() const { return permissions_; }
Permissions &Role::permissions() { return permissions_; }
#ifdef MG_ENTERPRISE
const FineGrainedAccessHandler &Role::fine_grained_access_handler() const { return fine_grained_access_handler_; }
FineGrainedAccessHandler &Role::fine_grained_access_handler() { return fine_grained_access_handler_; }

const FineGrainedAccessPermissions &Role::GetFineGrainedAccessLabelPermissions() const {
  return fine_grained_access_handler_.label_permissions();
}

const FineGrainedAccessPermissions &Role::GetFineGrainedAccessEdgeTypePermissions() const {
  return fine_grained_access_handler_.edge_type_permissions();
}
#endif

nlohmann::json Role::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["rolename"] = rolename_;
  data["permissions"] = permissions_.Serialize();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    data["fine_grained_access_handler"] = fine_grained_access_handler_.Serialize();
  } else {
    data["fine_grained_access_handler"] = {};
  }
#endif
  return data;
}

Role Role::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  if (!data["rolename"].is_string() || !data["permissions"].is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    FineGrainedAccessHandler fine_grained_access_handler;
    // We can have an empty fine_grained if the user was created without a valid license
    if (data["fine_grained_access_handler"].is_object()) {
      fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(data["fine_grained_access_handler"]);
    }
    return {data["rolename"], permissions, std::move(fine_grained_access_handler)};
  }
#endif
  return {data["rolename"], permissions};
}

bool operator==(const Role &first, const Role &second) {
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_ &&
           first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
  }
#endif
  return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_;
}

#ifdef MG_ENTERPRISE
void Databases::Add(const std::string &db) {
  if (allow_all_) {
    grants_dbs_.clear();
    allow_all_ = false;
  }
  grants_dbs_.emplace(db);
  denies_dbs_.erase(db);
}

void Databases::Remove(const std::string &db) {
  denies_dbs_.emplace(db);
  grants_dbs_.erase(db);
}

void Databases::Delete(const std::string &db) {
  denies_dbs_.erase(db);
  if (!allow_all_) {
    grants_dbs_.erase(db);
  }
  // Reset if default deleted
  if (default_db_ == db) {
    default_db_ = "";
  }
}

void Databases::GrantAll() {
  allow_all_ = true;
  grants_dbs_.clear();
  denies_dbs_.clear();
}

void Databases::DenyAll() {
  allow_all_ = false;
  grants_dbs_.clear();
  denies_dbs_.clear();
}

bool Databases::SetDefault(const std::string &db) {
  if (!Contains(db)) return false;
  default_db_ = db;
  return true;
}

[[nodiscard]] bool Databases::Contains(const std::string &db) const {
  return !denies_dbs_.contains(db) && (allow_all_ || grants_dbs_.contains(db));
}

const std::string &Databases::GetDefault() const {
  if (!Contains(default_db_)) {
    throw AuthException("No access to the set default database \"{}\".", default_db_);
  }
  return default_db_;
}

nlohmann::json Databases::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["grants"] = grants_dbs_;
  data["denies"] = denies_dbs_;
  data["allow_all"] = allow_all_;
  data["default"] = default_db_;
  return data;
}

Databases Databases::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load database data!");
  }
  if (!data["grants"].is_structured() || !data["denies"].is_structured() || !data["allow_all"].is_boolean() ||
      !data["default"].is_string()) {
    throw AuthException("Couldn't load database data!");
  }
  return {data["allow_all"], data["grants"], data["denies"], data["default"]};
}
#endif

User::User() {}

User::User(const std::string &username) : username_(utils::ToLowerCase(username)) {}
User::User(const std::string &username, const std::string &password_hash, const Permissions &permissions)
    : username_(utils::ToLowerCase(username)), password_hash_(password_hash), permissions_(permissions) {}

#ifdef MG_ENTERPRISE
User::User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler, Databases db_access)
    : username_(utils::ToLowerCase(username)),
      password_hash_(password_hash),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)),
      database_access_(db_access) {}
#endif

bool User::CheckPassword(const std::string &password) {
  if (password_hash_.empty()) return true;
  return VerifyPassword(password, password_hash_);
}

void User::UpdatePassword(const std::optional<std::string> &password) {
  if (!password) {
    if (!FLAGS_auth_password_permit_null) {
      throw AuthException("Null passwords aren't permitted!");
    }
    password_hash_ = "";
    return;
  }

  if (FLAGS_auth_password_strength_regex != default_password_regex) {
    if (const auto license_check_result = license::global_license_checker.IsEnterpriseValid(utils::global_settings);
        license_check_result.HasError()) {
      throw AuthException(
          "Custom password regex is a Memgraph Enterprise feature. Please set the config "
          "(\"--auth-password-strength-regex\") to its default value (\"{}\") or remove the flag.\n{}",
          default_password_regex,
          license::LicenseCheckErrorToString(license_check_result.GetError(), "password regex"));
    }
  }
  std::regex re(FLAGS_auth_password_strength_regex);
  if (!std::regex_match(*password, re)) {
    throw AuthException(
        "The user password doesn't conform to the required strength! Regex: "
        "\"{}\"",
        FLAGS_auth_password_strength_regex);
  }

  password_hash_ = EncryptPassword(*password);
}

void User::SetRole(const Role &role) { role_.emplace(role); }

void User::ClearRole() { role_ = std::nullopt; }

Permissions User::GetPermissions() const {
  if (role_) {
    return Permissions{permissions_.grants() | role_->permissions().grants(),
                       permissions_.denies() | role_->permissions().denies()};
  }
  return permissions_;
}

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions User::GetFineGrainedAccessLabelPermissions() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  if (role_) {
    return Merge(role()->fine_grained_access_handler().label_permissions(),
                 fine_grained_access_handler_.label_permissions());
  }

  return fine_grained_access_handler_.label_permissions();
}

FineGrainedAccessPermissions User::GetFineGrainedAccessEdgeTypePermissions() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }
  if (role_) {
    return Merge(role()->fine_grained_access_handler().edge_type_permissions(),
                 fine_grained_access_handler_.edge_type_permissions());
  }
  return fine_grained_access_handler_.edge_type_permissions();
}
#endif

const std::string &User::username() const { return username_; }

const Permissions &User::permissions() const { return permissions_; }
Permissions &User::permissions() { return permissions_; }
#ifdef MG_ENTERPRISE
const FineGrainedAccessHandler &User::fine_grained_access_handler() const { return fine_grained_access_handler_; }

FineGrainedAccessHandler &User::fine_grained_access_handler() { return fine_grained_access_handler_; }
#endif
const Role *User::role() const {
  if (role_.has_value()) {
    return &role_.value();
  }
  return nullptr;
}

nlohmann::json User::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["username"] = username_;
  data["password_hash"] = password_hash_;
  data["permissions"] = permissions_.Serialize();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    data["fine_grained_access_handler"] = fine_grained_access_handler_.Serialize();
    data["databases"] = database_access_.Serialize();
  } else {
    data["fine_grained_access_handler"] = {};
    data["databases"] = {};
  }
#endif
  // The role shouldn't be serialized here, it is stored as a foreign key.
  return data;
}

User User::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load user data!");
  }
  if (!data["username"].is_string() || !data["password_hash"].is_string() || !data["permissions"].is_object()) {
    throw AuthException("Couldn't load user data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    Databases db_access;
    if (data["databases"].is_structured()) {
      db_access = Databases::Deserialize(data["databases"]);
    } else {
      // Back-compatibility
      spdlog::warn("User without specified database access. Given access to the default database.");
      db_access.Add(dbms::kDefaultDB);
      db_access.SetDefault(dbms::kDefaultDB);
    }
    FineGrainedAccessHandler fine_grained_access_handler;
    // We can have an empty fine_grained if the user was created without a valid license
    if (data["fine_grained_access_handler"].is_object()) {
      fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(data["fine_grained_access_handler"]);
    }
    return {data["username"], data["password_hash"], permissions, std::move(fine_grained_access_handler), db_access};
  }
#endif
  return {data["username"], data["password_hash"], permissions};
}

bool operator==(const User &first, const User &second) {
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
           first.permissions_ == second.permissions_ && first.role_ == second.role_ &&
           first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
  }
#endif
  return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
         first.permissions_ == second.permissions_ && first.role_ == second.role_;
}

}  // namespace memgraph::auth
