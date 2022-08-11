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
const std::string ASTERISK = "*";
const uint64_t LabelPermissionMax = static_cast<uint64_t>(memgraph::auth::LabelPermission::CREATE_DELETE);
}  // namespace
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

std::unordered_map<std::string, uint64_t> Merge(const std::unordered_map<std::string, uint64_t> &first,
                                                const std::unordered_map<std::string, uint64_t> &second) {
  std::unordered_map<std::string, uint64_t> result{first};

  for (const auto &it : second) {
    if (result.find(it.first) != result.end()) {
      result[it.first] |= it.second;
    }
  }

  return result;
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
  return {data["grants"], data["denies"]};
}

uint64_t Permissions::grants() const { return grants_; }
uint64_t Permissions::denies() const { return denies_; }

bool operator==(const Permissions &first, const Permissions &second) {
  return first.grants() == second.grants() && first.denies() == second.denies();
}

bool operator!=(const Permissions &first, const Permissions &second) { return !(first == second); }

FineGrainedAccessPermissions::FineGrainedAccessPermissions(const std::unordered_map<std::string, uint64_t> &grants,
                                                           const std::unordered_map<std::string, uint64_t> &denies)
    : grants_(grants), denies_(denies) {}

PermissionLevel FineGrainedAccessPermissions::Has(const std::string &permission, LabelPermission label_permission) {
  if (denies_.size() == 1 && denies_.find(ASTERISK) != denies_.end()) {
    return PermissionLevel::DENY;
  }

  if ((grants_.size() == 1 && grants_.find(ASTERISK) != grants_.end())) {
    return PermissionLevel::GRANT;
  }

  auto grants_permission = PermissionLevel::DENY;
  auto denies_permission = PermissionLevel::GRANT;

  if (denies_.find(permission) != denies_.end() && denies_[permission] & label_permission) {
    denies_permission = PermissionLevel::DENY;
  }

  if (grants_.find(permission) != grants_.end() && grants_[permission] & label_permission) {
    grants_permission = PermissionLevel::GRANT;
  }

  return denies_permission >= grants_permission ? denies_permission : grants_permission;
}

void FineGrainedAccessPermissions::Grant(const std::string &permission, const LabelPermission label_permission) {
  if (permission == ASTERISK) {
    MG_ASSERT(label_permission == LabelPermission::CREATE_DELETE);
    grants_.clear();
    grants_[permission] = LabelPermission::CREATE_DELETE | LabelPermission::EDIT | LabelPermission::READ;

    return;
  }

  auto deniedPermissionIter = denies_.find(permission);

  if (deniedPermissionIter != denies_.end()) {
    denies_.erase(deniedPermissionIter);
  }

  if (grants_.size() == 1 && grants_.find(ASTERISK) != grants_.end()) {
    grants_.erase(ASTERISK);
  }

  uint64_t shift = 1;

  if (grants_.find(permission) == grants_.end()) {
    uint64_t perm = 0;
    auto u_label_perm = static_cast<uint64_t>(label_permission);

    while (u_label_perm != 0) {
      perm |= u_label_perm;
      u_label_perm >>= shift;
    }

    grants_[permission] = perm;
  }
}

void FineGrainedAccessPermissions::Revoke(const std::string &permission) {
  if (permission == ASTERISK) {
    grants_.clear();
    denies_.clear();

    return;
  }

  auto deniedPermissionIter = denies_.find(permission);
  auto grantedPermissionIter = grants_.find(permission);

  if (deniedPermissionIter != denies_.end()) {
    denies_.erase(deniedPermissionIter);
  }

  if (grantedPermissionIter != grants_.end()) {
    grants_.erase(grantedPermissionIter);
  }
}

void FineGrainedAccessPermissions::Deny(const std::string &permission, const LabelPermission label_permission) {
  if (permission == ASTERISK) {
    MG_ASSERT(label_permission == LabelPermission::READ);
    denies_.clear();
    denies_[permission] = LabelPermission::CREATE_DELETE | LabelPermission::EDIT | LabelPermission::READ;

    return;
  }

  auto grantedPermissionIter = grants_.find(permission);

  if (grantedPermissionIter != grants_.end()) {
    grants_.erase(grantedPermissionIter);
  }

  if (denies_.size() == 1 && denies_.find(ASTERISK) != denies_.end()) {
    denies_.erase(ASTERISK);
  }

  uint64_t shift = 1;

  if (denies_.find(permission) == denies_.end()) {
    uint64_t perm = 0;
    auto u_label_perm = static_cast<uint64_t>(label_permission);

    while (u_label_perm <= LabelPermissionMax) {
      perm |= u_label_perm;
      u_label_perm <<= shift;
    }

    denies_[permission] = perm;
  }
}

nlohmann::json FineGrainedAccessPermissions::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["grants"] = grants_;
  data["denies"] = denies_;
  return data;
}

FineGrainedAccessPermissions FineGrainedAccessPermissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
  }

  return FineGrainedAccessPermissions(data["grants"], data["denies"]);
}

const std::unordered_map<std::string, uint64_t> &FineGrainedAccessPermissions::grants() const { return grants_; }
const std::unordered_map<std::string, uint64_t> &FineGrainedAccessPermissions::denies() const { return denies_; }

bool operator==(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return first.grants() == second.grants() && first.denies() == second.denies();
}

bool operator!=(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return !(first == second);
}

Role::Role(const std::string &rolename) : rolename_(utils::ToLowerCase(rolename)) {}

Role::Role(const std::string &rolename, const Permissions &permissions,
           const FineGrainedAccessPermissions &fine_grained_access_permissions)
    : rolename_(utils::ToLowerCase(rolename)),
      permissions_(permissions),
      fine_grained_access_permissions_(fine_grained_access_permissions) {}

const std::string &Role::rolename() const { return rolename_; }
const Permissions &Role::permissions() const { return permissions_; }
Permissions &Role::permissions() { return permissions_; }
const FineGrainedAccessPermissions &Role::fine_grained_access_permissions() const {
  return fine_grained_access_permissions_;
}
FineGrainedAccessPermissions &Role::fine_grained_access_permissions() { return fine_grained_access_permissions_; }

nlohmann::json Role::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["rolename"] = rolename_;
  data["permissions"] = permissions_.Serialize();
  data["fine_grained_access_permissions"] = fine_grained_access_permissions_.Serialize();
  return data;
}

Role Role::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  if (!data["rolename"].is_string() || !data["permissions"].is_object() ||
      !data["fine_grained_access_permissions"].is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
  auto fine_grained_access_permissions =
      FineGrainedAccessPermissions::Deserialize(data["fine_grained_access_permissions"]);
  return {data["rolename"], permissions, fine_grained_access_permissions};
}

bool operator==(const Role &first, const Role &second) {
  return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_;
}

User::User(const std::string &username) : username_(utils::ToLowerCase(username)) {}

User::User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
           const FineGrainedAccessPermissions &fine_grained_access_permissions)
    : username_(utils::ToLowerCase(username)),
      password_hash_(password_hash),
      permissions_(permissions),
      fine_grained_access_permissions_(fine_grained_access_permissions) {}

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
    return Permissions(permissions_.grants() | role_->permissions().grants(),
                       permissions_.denies() | role_->permissions().denies());
  }
  return permissions_;
}

FineGrainedAccessPermissions User::GetFineGrainedAccessPermissions() const {
  if (role_) {
    return FineGrainedAccessPermissions(
        Merge(fine_grained_access_permissions_.grants(), role_->fine_grained_access_permissions().grants()),
        Merge(fine_grained_access_permissions_.denies(), role_->fine_grained_access_permissions().denies()));
  }
  return fine_grained_access_permissions_;
}

const std::string &User::username() const { return username_; }

const Permissions &User::permissions() const { return permissions_; }
Permissions &User::permissions() { return permissions_; }
const FineGrainedAccessPermissions &User::fine_grained_access_permissions() const {
  return fine_grained_access_permissions_;
}
FineGrainedAccessPermissions &User::fine_grained_access_permissions() { return fine_grained_access_permissions_; }

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
  data["fine_grained_access_permissions"] = fine_grained_access_permissions_.Serialize();
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
  auto fine_grained_access_permissions =
      FineGrainedAccessPermissions::Deserialize(data["fine_grained_access_permissions"]);
  return {data["username"], data["password_hash"], permissions, fine_grained_access_permissions};
}

bool operator==(const User &first, const User &second) {
  return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
         first.permissions_ == second.permissions_ && first.role_ == second.role_;
}
}  // namespace memgraph::auth
