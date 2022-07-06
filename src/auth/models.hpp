// Copyright 2022 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <optional>
#include <string>
#include <unordered_set>

#include <json/json.hpp>

namespace memgraph::auth {
// These permissions must have values that are applicable for usage in a
// bitmask.
// clang-format off
enum class Permission : uint64_t {
  MATCH        = 1,
  CREATE       = 1U << 1U,
  MERGE        = 1U << 2U,
  DELETE       = 1U << 3U,
  SET          = 1U << 4U,
  REMOVE       = 1U << 5U,
  INDEX        = 1U << 6U,
  STATS        = 1U << 7U,
  CONSTRAINT   = 1U << 8U,
  DUMP         = 1U << 9U,
  REPLICATION  = 1U << 10U,
  DURABILITY   = 1U << 11U,
  READ_FILE    = 1U << 12U,
  FREE_MEMORY  = 1U << 13U,
  TRIGGER      = 1U << 14U,
  CONFIG       = 1U << 15U,
  AUTH         = 1U << 16U,
  STREAM       = 1U << 17U,
  MODULE_READ  = 1U << 18U,
  MODULE_WRITE = 1U << 19U,
  WEBSOCKET    = 1U << 20U
};
// clang-format on

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

class LabelPermissions final {
 public:
  explicit LabelPermissions(const std::unordered_set<std::string> &grants = {},
                            const std::unordered_set<std::string> &denies = {});

  PermissionLevel Has(const std::string &permission) const;

  void Grant(const std::string &permission);

  void Revoke(const std::string &permission);

  void Deny(const std::string &permission);

  std::unordered_set<std::string> GetGrants() const;
  std::unordered_set<std::string> GetDenies() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static LabelPermissions Deserialize(const nlohmann::json &data);

  std::unordered_set<std::string> grants() const;
  std::unordered_set<std::string> denies() const;

 private:
  std::unordered_set<std::string> grants_{};
  std::unordered_set<std::string> denies_{};
};

bool operator==(const LabelPermissions &first, const LabelPermissions &second);

bool operator!=(const LabelPermissions &first, const LabelPermissions &second);

class Role final {
 public:
  Role(const std::string &rolename);

  Role(const std::string &rolename, const Permissions &permissions, const LabelPermissions &labelPermissions);

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();

  const LabelPermissions &labelPermissions() const;
  LabelPermissions &labelPermissions();

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Role Deserialize(const nlohmann::json &data);

  friend bool operator==(const Role &first, const Role &second);

 private:
  std::string rolename_;
  Permissions permissions_;
  LabelPermissions labelPermissions_;
};

bool operator==(const Role &first, const Role &second);

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User(const std::string &username);

  User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
       const LabelPermissions &labelPermissions);

  /// @throw AuthException if unable to verify the password.
  bool CheckPassword(const std::string &password);

  /// @throw AuthException if unable to set the password.
  void UpdatePassword(const std::optional<std::string> &password = std::nullopt);

  void SetRole(const Role &role);

  void ClearRole();

  Permissions GetPermissions() const;

  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();

  const LabelPermissions &labelPermissions() const;
  Permissions &labelPermissions();

  const Role *role() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::string password_hash_;
  Permissions permissions_;
  LabelPermissions labelPermissions_;
  std::optional<Role> role_;
};

bool operator==(const User &first, const User &second);
}  // namespace memgraph::auth
