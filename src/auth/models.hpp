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
#include <unordered_map>

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

// clang-format off
enum class LabelPermission : uint64_t {
  READ          = 1,
  EDIT          = 1U << 1U,
  CREATE_DELETE = 1U << 2U
};
// clang-format on

inline uint64_t operator|(LabelPermission a, LabelPermission b) {
  return static_cast<uint64_t>(a) | static_cast<uint64_t>(b);
}

inline uint64_t operator|(uint64_t a, LabelPermission b) { return a | static_cast<uint64_t>(b); }

inline bool operator&(uint64_t a, LabelPermission b) { return (a & static_cast<uint64_t>(b)) != 0; }

// Function that converts a permission to its string representation.
std::string PermissionToString(Permission permission);

// Class that indicates what permission level the user/role has.
enum class PermissionLevel : short { DENY, GRANT, NEUTRAL };

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

class FineGrainedAccessPermissions final {
 public:
  explicit FineGrainedAccessPermissions(const std::unordered_map<std::string, uint64_t> &grants = {},
                                        const std::unordered_map<std::string, uint64_t> &denies = {});

  PermissionLevel Has(const std::string &permission, LabelPermission label_permission);

  void Grant(const std::string &permission, LabelPermission label_permission);

  void Revoke(const std::string &permission);

  void Deny(const std::string &permission, LabelPermission label_permission);

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static FineGrainedAccessPermissions Deserialize(const nlohmann::json &data);

  const std::unordered_map<std::string, uint64_t> &grants() const;
  const std::unordered_map<std::string, uint64_t> &denies() const;

 private:
  std::unordered_map<std::string, uint64_t> grants_{};
  std::unordered_map<std::string, uint64_t> denies_{};
};

bool operator==(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second);

bool operator!=(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second);

class Role final {
 public:
  Role(const std::string &rolename);

  Role(const std::string &rolename, const Permissions &permissions,
       const FineGrainedAccessPermissions &fine_grained_access_permissions);

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();
  const FineGrainedAccessPermissions &fine_grained_access_permissions() const;
  FineGrainedAccessPermissions &fine_grained_access_permissions();

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Role Deserialize(const nlohmann::json &data);

  friend bool operator==(const Role &first, const Role &second);

 private:
  std::string rolename_;
  Permissions permissions_;
  FineGrainedAccessPermissions fine_grained_access_permissions_;
};

bool operator==(const Role &first, const Role &second);

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User(const std::string &username);

  User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
       const FineGrainedAccessPermissions &fine_grained_access_permissions);

  /// @throw AuthException if unable to verify the password.
  bool CheckPassword(const std::string &password);

  /// @throw AuthException if unable to set the password.
  void UpdatePassword(const std::optional<std::string> &password = std::nullopt);

  void SetRole(const Role &role);

  void ClearRole();

  Permissions GetPermissions() const;
  FineGrainedAccessPermissions GetFineGrainedAccessPermissions() const;

  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();
  const FineGrainedAccessPermissions &fine_grained_access_permissions() const;
  FineGrainedAccessPermissions &fine_grained_access_permissions();

  const Role *role() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::string password_hash_;
  Permissions permissions_;
  FineGrainedAccessPermissions fine_grained_access_permissions_;
  std::optional<Role> role_;
};

bool operator==(const User &first, const User &second);
}  // namespace memgraph::auth
