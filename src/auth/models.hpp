#pragma once

#include <experimental/optional>
#include <string>

#include <json/json.hpp>

namespace auth {

// TODO (mferencevic): Add permissions for admin actions.
enum class Permission : uint64_t {
  Read = 0x00000001,

  Create = 0x00000002,

  Update = 0x00000004,

  Delete = 0x00000008,
};

enum class PermissionLevel {
  Grant,
  Neutral,
  Deny,
};

// TODO (mferencevic): Add string conversions to/from permissions.

class Permissions final {
 public:
  Permissions(uint64_t grants = 0, uint64_t denies = 0);

  PermissionLevel Has(Permission permission) const;

  void Grant(Permission permission);

  void Revoke(Permission permission);

  void Deny(Permission permission);

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

// TODO (mferencevic): Implement password strength enforcement.
// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User(const std::string &username);

  User(const std::string &username, const std::string &password_hash,
       const Permissions &permissions);

  bool CheckPassword(const std::string &password);

  void UpdatePassword(const std::string &password);

  void SetRole(const Role &role);

  const Permissions GetPermissions() const;

  const std::string &username() const;

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
