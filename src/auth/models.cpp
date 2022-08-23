// Copyright 2022 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/models.hpp"
#include <cstdint>
#include <regex>
#include <unordered_set>

#include <gflags/gflags.h>

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "utils/cast.hpp"
#include "utils/license.hpp"
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
const std::vector<Permission> kPermissionsAll = {
    Permission::MATCH,      Permission::CREATE,    Permission::MERGE,       Permission::DELETE,
    Permission::SET,        Permission::REMOVE,    Permission::INDEX,       Permission::STATS,
    Permission::CONSTRAINT, Permission::DUMP,      Permission::AUTH,        Permission::REPLICATION,
    Permission::DURABILITY, Permission::READ_FILE, Permission::FREE_MEMORY, Permission::TRIGGER,
    Permission::CONFIG,     Permission::STREAM,    Permission::MODULE_READ, Permission::MODULE_WRITE,
    Permission::WEBSOCKET};
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

FineGrainedAccessPermissions Merge(const FineGrainedAccessPermissions &first,
                                   const FineGrainedAccessPermissions &second) {
  std::unordered_map<std::string, uint64_t> permissions{first.GetPermissions()};
  std::optional<uint64_t> global_permission;

  if (second.GetGlobalPermission().has_value()) {
    global_permission = second.GetGlobalPermission().value();
  } else if (first.GetGlobalPermission().has_value()) {
    global_permission = first.GetGlobalPermission().value();
  }

  for (const auto &[label_name, permission] : second.GetPermissions()) {
    permissions[label_name] = permission;
  }

  return FineGrainedAccessPermissions(permissions, global_permission);
}

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

FineGrainedAccessPermissions::FineGrainedAccessPermissions(const std::unordered_map<std::string, uint64_t> &permissions,
                                                           const std::optional<uint64_t> &global_permission)
    : permissions_(permissions), global_permission_(global_permission) {}

PermissionLevel FineGrainedAccessPermissions::Has(const std::string &permission,
                                                  const FineGrainedPermission fine_grained_permission) const {
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
  if (permission == kAsterisk) {
    global_permission_ = CalculateGrant(fine_grained_permission);
  } else {
    permissions_[permission] |= CalculateGrant(fine_grained_permission);
  }
}

void FineGrainedAccessPermissions::Revoke(const std::string &permission) {
  if (permission == kAsterisk) {
    permissions_.clear();
    global_permission_ = std::nullopt;
  } else {
    permissions_.erase(permission);
  }
}

void FineGrainedAccessPermissions::Deny(const std::string &permission,
                                        const FineGrainedPermission fine_grained_permission) {
  if (permission == kAsterisk) {
    global_permission_ = CalculateDeny(fine_grained_permission);
  } else {
    permissions_[permission] = CalculateDeny(fine_grained_permission);
  }
}

nlohmann::json FineGrainedAccessPermissions::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["permissions"] = permissions_;
  data["global_permission"] = global_permission_.has_value() ? global_permission_.value() : -1;
  return data;
}

FineGrainedAccessPermissions FineGrainedAccessPermissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
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

uint64_t FineGrainedAccessPermissions::CalculateGrant(FineGrainedPermission label_permission) {
  uint64_t shift{1};
  uint64_t result{0};
  auto uint_label_permission = static_cast<uint64_t>(label_permission);

  while (uint_label_permission > 0) {
    result |= uint_label_permission;
    uint_label_permission >>= shift;
  }

  return result;
}

uint64_t FineGrainedAccessPermissions::CalculateDeny(FineGrainedPermission label_permission) {
  uint64_t shift{1};
  uint64_t result{0};
  auto uint_label_permission = static_cast<uint64_t>(label_permission);

  while (uint_label_permission <= kLabelPermissionMax) {
    result |= uint_label_permission;
    uint_label_permission <<= shift;
  }

  return kLabelPermissionAll - result;
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

Role::Role(const std::string &rolename) : rolename_(utils::ToLowerCase(rolename)) {}

Role::Role(const std::string &rolename, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler)
    : rolename_(utils::ToLowerCase(rolename)),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)) {}

const std::string &Role::rolename() const { return rolename_; }
const Permissions &Role::permissions() const { return permissions_; }
Permissions &Role::permissions() { return permissions_; }
const FineGrainedAccessHandler &Role::fine_grained_access_handler() const { return fine_grained_access_handler_; }
FineGrainedAccessHandler &Role::fine_grained_access_handler() { return fine_grained_access_handler_; }

nlohmann::json Role::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["rolename"] = rolename_;
  data["permissions"] = permissions_.Serialize();
  data["fine_grained_access_handler"] = fine_grained_access_handler_.Serialize();
  return data;
}

Role Role::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  if (!data["rolename"].is_string() || !data["permissions"].is_object() ||
      !data["fine_grained_access_handler"].is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
  auto fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(data["fine_grained_access_handler"]);
  return {data["rolename"], permissions, std::move(fine_grained_access_handler)};
}

bool operator==(const Role &first, const Role &second) {
  return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_ &&
         first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
}

User::User() {}

User::User(const std::string &username) : username_(utils::ToLowerCase(username)) {}

User::User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler)
    : username_(utils::ToLowerCase(username)),
      password_hash_(password_hash),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)) {}

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
    if (const auto license_check_result = utils::license::global_license_checker.IsValidLicense(utils::global_settings);
        license_check_result.HasError()) {
      throw AuthException(
          "Custom password regex is a Memgraph Enterprise feature. Please set the config "
          "(\"--auth-password-strength-regex\") to its default value (\"{}\") or remove the flag.\n{}",
          default_password_regex,
          utils::license::LicenseCheckErrorToString(license_check_result.GetError(), "password regex"));
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

FineGrainedAccessPermissions User::GetFineGrainedAccessLabelPermissions() const {
  if (role_) {
    return Merge(role()->fine_grained_access_handler().label_permissions(),
                 fine_grained_access_handler_.label_permissions());
  }
  return fine_grained_access_handler_.label_permissions();
}

FineGrainedAccessPermissions User::GetFineGrainedAccessEdgeTypePermissions() const {
  if (role_) {
    return Merge(role()->fine_grained_access_handler().edge_type_permissions(),
                 fine_grained_access_handler_.edge_type_permissions());
  }
  return fine_grained_access_handler_.edge_type_permissions();
}

const std::string &User::username() const { return username_; }

const Permissions &User::permissions() const { return permissions_; }
Permissions &User::permissions() { return permissions_; }
const FineGrainedAccessHandler &User::fine_grained_access_handler() const { return fine_grained_access_handler_; }
FineGrainedAccessHandler &User::fine_grained_access_handler() { return fine_grained_access_handler_; }

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
  data["fine_grained_access_handler"] = fine_grained_access_handler_.Serialize();
  // The role shouldn't be serialized here, it is stored as a foreign key.
  return data;
}

User User::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load user data!");
  }
  if (!data["username"].is_string() || !data["password_hash"].is_string() || !data["permissions"].is_object() ||
      !data["fine_grained_access_handler"].is_object()) {
    throw AuthException("Couldn't load user data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
  auto fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(data["fine_grained_access_handler"]);
  return {data["username"], data["password_hash"], permissions, fine_grained_access_handler};
}

bool operator==(const User &first, const User &second) {
  return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
         first.permissions_ == second.permissions_ && first.role_ == second.role_ &&
         first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
}

}  // namespace memgraph::auth
