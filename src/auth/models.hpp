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
#include <unordered_set>

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
  explicit Permissions(uint64_t grants = 0, uint64_t denies = 0);

  Permissions(const Permissions &) = default;
  Permissions &operator=(const Permissions &) = default;
  Permissions(Permissions &&) noexcept = default;
  Permissions &operator=(Permissions &&) noexcept = default;
  ~Permissions() = default;

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
  explicit FineGrainedAccessPermissions(const std::unordered_set<std::string> &grants = {},
                                        const std::unordered_set<std::string> &denies = {});

  FineGrainedAccessPermissions(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions &operator=(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions(FineGrainedAccessPermissions &&) = default;
  FineGrainedAccessPermissions &operator=(FineGrainedAccessPermissions &&) = default;
  ~FineGrainedAccessPermissions() = default;

  PermissionLevel Has(const std::string &permission) const;

  void Grant(const std::string &permission);

  void Revoke(const std::string &permission);

  void Deny(const std::string &permission);

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static FineGrainedAccessPermissions Deserialize(const nlohmann::json &data);

  const std::unordered_set<std::string> &grants() const;
  const std::unordered_set<std::string> &denies() const;

 private:
  std::unordered_set<std::string> grants_{};
  std::unordered_set<std::string> denies_{};
};

bool operator==(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second);

bool operator!=(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second);

class FineGrainedAccessHandler final {
 public:
  explicit FineGrainedAccessHandler(FineGrainedAccessPermissions labelPermissions = FineGrainedAccessPermissions(),
                                    FineGrainedAccessPermissions edgeTypePermissions = FineGrainedAccessPermissions());

  FineGrainedAccessHandler(const FineGrainedAccessHandler &) = default;
  FineGrainedAccessHandler &operator=(const FineGrainedAccessHandler &) = default;
  FineGrainedAccessHandler(FineGrainedAccessHandler &&) noexcept = default;
  FineGrainedAccessHandler &operator=(FineGrainedAccessHandler &&) noexcept = default;
  ~FineGrainedAccessHandler() = default;

  const FineGrainedAccessPermissions &label_permissions() const;
  FineGrainedAccessPermissions &label_permissions();

  const FineGrainedAccessPermissions &edge_type_permissions() const;
  FineGrainedAccessPermissions &edge_type_permissions();

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static FineGrainedAccessHandler Deserialize(const nlohmann::json &data);

  friend bool operator==(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second);

 private:
  FineGrainedAccessPermissions label_permissions_;
  FineGrainedAccessPermissions edge_type_permissions_;
};

bool operator==(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second);

class Role final {
 public:
  explicit Role(const std::string &rolename);

  Role(const std::string &rolename, const Permissions &permissions,
       FineGrainedAccessHandler fine_grained_access_handler);

  Role(const Role &) = default;
  Role &operator=(const Role &) = default;
  Role(Role &&) noexcept = default;
  Role &operator=(Role &&) noexcept = default;
  ~Role() = default;

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Role Deserialize(const nlohmann::json &data);

  friend bool operator==(const Role &first, const Role &second);

 private:
  std::string rolename_;
  Permissions permissions_;
  FineGrainedAccessHandler fine_grained_access_handler_;
};

bool operator==(const Role &first, const Role &second);

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User();

  explicit User(const std::string &username);

  User(const std::string &username, const std::string &password_hash, const Permissions &permissions,
       FineGrainedAccessHandler fine_grained_access_handler);

  User(const User &) = default;
  User &operator=(const User &) = default;
  User(User &&) noexcept = default;
  User &operator=(User &&) noexcept = default;
  ~User() = default;

  /// @throw AuthException if unable to verify the password.
  bool CheckPassword(const std::string &password);

  /// @throw AuthException if unable to set the password.
  void UpdatePassword(const std::optional<std::string> &password = std::nullopt);

  void SetRole(const Role &role);

  void ClearRole();

  Permissions GetPermissions() const;
  FineGrainedAccessPermissions GetFineGrainedAccessLabelPermissions() const;
  FineGrainedAccessPermissions GetFineGrainedAccessEdgeTypePermissions() const;

  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();

  const Role *role() const;

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::string password_hash_;
  Permissions permissions_;
  FineGrainedAccessHandler fine_grained_access_handler_;
  std::optional<Role> role_;
};

bool operator==(const User &first, const User &second);
}  // namespace memgraph::auth
