#pragma once

#include <experimental/optional>
#include <string>

#include <json/json.hpp>

namespace auth {

// These permissions must have values that are applicable for usage in a
// bitmask.
enum class Permission : uint64_t {
  MATCH = 0x00000001,
  CREATE = 0x00000002,
  MERGE = 0x00000004,
  DELETE = 0x00000008,
  SET = 0x00000010,
  REMOVE = 0x00000020,
  INDEX = 0x00000040,
  AUTH = 0x00010000,
  STREAM = 0x00020000,
};

// Constant list of all available permissions.
const std::vector<Permission> kPermissionsAll = {
    Permission::MATCH,  Permission::CREATE, Permission::MERGE,
    Permission::DELETE, Permission::SET,    Permission::REMOVE,
    Permission::INDEX,  Permission::AUTH,   Permission::STREAM};

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

  User(const std::string &username, const std::string &password_hash,
       const Permissions &permissions);

  bool CheckPassword(const std::string &password);

  void UpdatePassword(const std::experimental::optional<std::string> &password =
                          std::experimental::nullopt);

  void SetRole(const Role &role);

  void ClearRole();

  const Permissions GetPermissions() const;

  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();

  std::experimental::optional<Role> role() const;

  nlohmann::json Serialize() const;

  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::string password_hash_;
  Permissions permissions_;
  std::experimental::optional<Role> role_;
};

bool operator==(const User &first, const User &second);
}  // namespace auth
