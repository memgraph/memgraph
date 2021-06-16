#pragma once

#include <optional>
#include <string>

#include <json/json.hpp>

namespace auth {

// These permissions must have values that are applicable for usage in a
// bitmask.
// clang-format off
enum class Permission : uint64_t {
  MATCH       = 1,
  CREATE      = 1U << 1U,
  MERGE       = 1U << 2U,
  DELETE      = 1U << 3U,
  SET         = 1U << 4U,
  REMOVE      = 1U << 5U,
  INDEX       = 1U << 6U,
  STATS       = 1U << 7U,
  CONSTRAINT  = 1U << 8U,
  DUMP        = 1U << 9U,
  REPLICATION = 1U << 10U,
  LOCK_PATH   = 1U << 11U,
  READ_FILE   = 1U << 12U,
  FREE_MEMORY = 1U << 13U,
  TRIGGER     = 1U << 14U,
  CONFIG      = 1U << 15U,
  AUTH        = 1U << 16U
};
// clang-format on

// Constant list of all available permissions.
const std::vector<Permission> kPermissionsAll = {Permission::MATCH,     Permission::CREATE,    Permission::MERGE,
                                                 Permission::DELETE,    Permission::SET,       Permission::REMOVE,
                                                 Permission::INDEX,     Permission::STATS,     Permission::CONSTRAINT,
                                                 Permission::DUMP,      Permission::AUTH,      Permission::REPLICATION,
                                                 Permission::LOCK_PATH, Permission::READ_FILE, Permission::FREE_MEMORY,
                                                 Permission::TRIGGER,   Permission::CONFIG};

// Function that converts a permission to its string representation.
std::string PermissionToString(Permission permission);

// Class that indicates what permission level the user/role has.
enum class PermissionLevel {
  GRANT,
  NEUTRAL,
  DENY,
};

// Function that converts a permission level to its string representation.
std::string PermissionLevelToString(PermissionLevel level);

class Permissions final {
 public:
  Permissions(uint64_t grants = 0, uint64_t denies = 0);

  PermissionLevel Has(Permission permission) const;

  void Grant(Permission permission);

  void Revoke(Permission permission);

  void Deny(Permission permission);

  std::vector<Permission> GetGrants() const;

  std::vector<Permission> GetDenies() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Permissions Deserialize(const nlohmann::json &data);

  uint64_t grants() const;
  uint64_t denies() const;

 private:
  uint64_t grants_{0};
  uint64_t denies_{0};
};

bool operator==(const Permissions &first, const Permissions &second);

bool operator!=(const Permissions &first, const Permissions &second);

class Role final {
 public:
  Role(const std::string &rolename);

  Role(const std::string &rolename, const Permissions &permissions);

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Role Deserialize(const nlohmann::json &data);

  friend bool operator==(const Role &first, const Role &second);

 private:
  std::string rolename_;
  Permissions permissions_;
};

bool operator==(const Role &first, const Role &second);

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User(const std::string &username);

  User(const std::string &username, const std::string &password_hash, const Permissions &permissions);

  /// @throw AuthException if unable to verify the password.
  bool CheckPassword(const std::string &password);

  /// @throw AuthException if unable to set the password.
  void UpdatePassword(const std::optional<std::string> &password = std::nullopt);

  void SetRole(const Role &role);

  void ClearRole();

  const Permissions GetPermissions() const;

  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();

  std::optional<Role> role() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::string password_hash_;
  Permissions permissions_;
  std::optional<Role> role_;
};

bool operator==(const User &first, const User &second);
}  // namespace auth
