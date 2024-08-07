// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <optional>
#include <set>
#include <string>
#include <unordered_map>

#include <json/json.hpp>
#include <utility>
#include "crypto.hpp"
#include "dbms/constants.hpp"
#include "utils/logging.hpp"

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
  WEBSOCKET    = 1U << 20U,
  TRANSACTION_MANAGEMENT = 1U << 21U,
  STORAGE_MODE = 1U << 22U,
  MULTI_DATABASE_EDIT = 1U << 23U,
  MULTI_DATABASE_USE  = 1U << 24U,
  COORDINATOR  = 1U << 25U,
};
// clang-format on

#ifdef MG_ENTERPRISE
// clang-format off
enum class FineGrainedPermission : uint64_t {
  NOTHING       = 0,
  READ          = 1,
  UPDATE        = 1U << 1U,
  CREATE_DELETE = 1U << 2U
};
// clang-format on

constexpr inline uint64_t operator|(FineGrainedPermission lhs, FineGrainedPermission rhs) {
  return static_cast<uint64_t>(lhs) | static_cast<uint64_t>(rhs);
}

constexpr inline uint64_t operator|(uint64_t lhs, FineGrainedPermission rhs) {
  return lhs | static_cast<uint64_t>(rhs);
}

constexpr inline uint64_t operator&(uint64_t lhs, FineGrainedPermission rhs) {
  return (lhs & static_cast<uint64_t>(rhs)) != 0;
}

constexpr uint64_t kLabelPermissionAll = memgraph::auth::FineGrainedPermission::CREATE_DELETE |
                                         memgraph::auth::FineGrainedPermission::UPDATE |
                                         memgraph::auth::FineGrainedPermission::READ;
constexpr uint64_t kLabelPermissionMax = static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::CREATE_DELETE);
constexpr uint64_t kLabelPermissionMin = static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::READ);
#endif

// Function that converts a permission to its string representation.
std::string PermissionToString(Permission permission);

// Class that indicates what permission level the user/role has.
enum class PermissionLevel : uint8_t { GRANT, NEUTRAL, DENY };

// Function that converts a permission level to its string representation.
std::string PermissionLevelToString(PermissionLevel level);

#ifdef MG_ENTERPRISE
// Function that converts a label permission level to its string representation.
std::string FineGrainedPermissionToString(FineGrainedPermission level);

// Constructs a label permission from a permission
FineGrainedPermission PermissionToFineGrainedPermission(uint64_t permission);
#endif

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

#ifdef MG_ENTERPRISE
class FineGrainedAccessPermissions final {
 public:
  explicit FineGrainedAccessPermissions(const std::unordered_map<std::string, uint64_t> &permissions = {},
                                        const std::optional<uint64_t> &global_permission = std::nullopt);
  FineGrainedAccessPermissions(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions &operator=(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions(FineGrainedAccessPermissions &&) = default;
  FineGrainedAccessPermissions &operator=(FineGrainedAccessPermissions &&) = default;
  ~FineGrainedAccessPermissions() = default;
  PermissionLevel Has(const std::string &permission, FineGrainedPermission fine_grained_permission) const;

  void Grant(const std::string &permission, FineGrainedPermission fine_grained_permission);

  void Revoke(const std::string &permission);

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static FineGrainedAccessPermissions Deserialize(const nlohmann::json &data);

  const std::unordered_map<std::string, uint64_t> &GetPermissions() const;
  const std::optional<uint64_t> &GetGlobalPermission() const;

 private:
  std::unordered_map<std::string, uint64_t> permissions_{};
  std::optional<uint64_t> global_permission_;

  static uint64_t CalculateGrant(FineGrainedPermission fine_grained_permission);
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
#endif

#ifdef MG_ENTERPRISE
class Databases final {
 public:
  Databases() : grants_dbs_{std::string{dbms::kDefaultDB}}, allow_all_(false), main_db_(dbms::kDefaultDB) {}

  Databases(const Databases &) = default;
  Databases &operator=(const Databases &) = default;
  Databases(Databases &&) noexcept = default;
  Databases &operator=(Databases &&) noexcept = default;
  ~Databases() = default;

  /**
   * @brief Add database to the list of granted access. @note allow_all_ will be false after execution
   *
   * @param db name of the database to grant access to
   */
  void Grant(std::string_view db);

  /**
   * @brief Remove database to the list of granted access.
   * @note if allow_all_ is set, the flag will remain set and the
   * database will be added to the set of denied databases.
   *
   * @param db name of the database to grant access to
   */
  void Deny(const std::string &db);

  /**
   * @brief Called when database is dropped. Removes it from granted (if allow_all is false) and denied set.
   * @note allow_all_ is not changed
   *
   * @param db name of the database to grant access to
   */
  void Revoke(const std::string &db);

  /**
   * @brief Set allow_all_ to true and clears grants and denied sets.
   */
  void GrantAll();

  /**
   * @brief Set allow_all_ to false and clears grants and denied sets.
   */
  void DenyAll();

  /**
   * @brief Set allow_all_ to false and clears grants and denied sets.
   */
  void RevokeAll();

  /**
   * @brief Set the default database.
   */
  bool SetMain(std::string_view db);

  /**
   * @brief Checks if access is grated to the database.
   *
   * @param db name of the database
   * @return true if allow_all and not denied or granted
   */
  bool Contains(std::string_view db) const;
  bool Denies(std::string_view db_name) const { return denies_dbs_.contains(db_name); }
  bool Grants(std::string_view db_name) const { return allow_all_ || grants_dbs_.contains(db_name); }

  bool GetAllowAll() const { return allow_all_; }
  const std::set<std::string, std::less<>> &GetGrants() const { return grants_dbs_; }
  const std::set<std::string, std::less<>> &GetDenies() const { return denies_dbs_; }
  const std::string &GetMain() const;

  nlohmann::json Serialize() const;
  /// @throw AuthException if unable to deserialize.
  static Databases Deserialize(const nlohmann::json &data);

 private:
  Databases(bool allow_all, std::set<std::string, std::less<>> grant, std::set<std::string, std::less<>> deny,
            std::string default_db = std::string{dbms::kDefaultDB})
      : grants_dbs_(std::move(grant)),
        denies_dbs_(std::move(deny)),
        allow_all_(allow_all),
        main_db_(std::move(default_db)) {}

  std::set<std::string, std::less<>> grants_dbs_;  //!< set of databases with granted access
  std::set<std::string, std::less<>> denies_dbs_;  //!< set of databases with denied access
  bool allow_all_;                                 //!< flag to allow access to everything (denied overrides this)
  std::string main_db_;                            //!< user's default database
};
#endif

class Role {
 public:
  Role() = default;

  explicit Role(const std::string &rolename);
  Role(const std::string &rolename, const Permissions &permissions);
#ifdef MG_ENTERPRISE
  Role(const std::string &rolename, const Permissions &permissions,
       FineGrainedAccessHandler fine_grained_access_handler, Databases db_access = {});
#endif
  Role(const Role &) = default;
  Role &operator=(const Role &) = default;
  Role(Role &&) noexcept = default;
  Role &operator=(Role &&) noexcept = default;
  ~Role() = default;

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();
  Permissions GetPermissions() const { return permissions_; }
#ifdef MG_ENTERPRISE
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();
  const FineGrainedAccessPermissions &GetFineGrainedAccessLabelPermissions() const;
  const FineGrainedAccessPermissions &GetFineGrainedAccessEdgeTypePermissions() const;
#endif

#ifdef MG_ENTERPRISE
  Databases &db_access() { return db_access_; }
  const Databases &db_access() const { return db_access_; }

  bool DeniesDB(std::string_view db_name) const { return db_access_.Denies(db_name); }
  bool GrantsDB(std::string_view db_name) const { return db_access_.Grants(db_name); }
  bool HasAccess(std::string_view db_name) const { return !DeniesDB(db_name) && GrantsDB(db_name); }
#endif

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static Role Deserialize(const nlohmann::json &data);

  friend bool operator==(const Role &first, const Role &second);

 private:
  std::string rolename_;
  Permissions permissions_;
#ifdef MG_ENTERPRISE
  FineGrainedAccessHandler fine_grained_access_handler_;
  Databases db_access_;
#endif
};

bool operator==(const Role &first, const Role &second);

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User();

  explicit User(const std::string &username);
  User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions);
#ifdef MG_ENTERPRISE
  User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions,
       FineGrainedAccessHandler fine_grained_access_handler, Databases db_access = {});
#endif
  User(const User &) = default;
  User &operator=(const User &) = default;
  User(User &&) noexcept = default;
  User &operator=(User &&) noexcept = default;
  ~User() = default;

  /// @throw AuthException if unable to verify the password.
  bool CheckPassword(const std::string &password);
  bool CheckPasswordExplicit(const std::string &password);

  bool UpgradeHash(const std::string password) {
    if (!password_hash_) return false;
    if (password_hash_->IsSalted()) return false;

    auto const algo = password_hash_->HashAlgo();
    UpdatePassword(password, algo);
    return true;
  }

  /// @throw AuthException if unable to set the password.
  void UpdatePassword(const std::optional<std::string> &password = {},
                      std::optional<PasswordHashAlgorithm> algo_override = std::nullopt);

  void UpdateHash(HashedPassword hashed_password);

  void SetRole(const Role &role);

  void ClearRole();

  Permissions GetPermissions() const;

#ifdef MG_ENTERPRISE
  FineGrainedAccessPermissions GetFineGrainedAccessLabelPermissions() const;
  FineGrainedAccessPermissions GetFineGrainedAccessEdgeTypePermissions() const;
  FineGrainedAccessPermissions GetUserFineGrainedAccessLabelPermissions() const;
  FineGrainedAccessPermissions GetUserFineGrainedAccessEdgeTypePermissions() const;
  FineGrainedAccessPermissions GetRoleFineGrainedAccessLabelPermissions() const;
  FineGrainedAccessPermissions GetRoleFineGrainedAccessEdgeTypePermissions() const;
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();
#endif
  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();

  const Role *role() const;

#ifdef MG_ENTERPRISE
  Databases &db_access() { return database_access_; }
  const Databases &db_access() const { return database_access_; }

  bool DeniesDB(std::string_view db_name) const {
    bool denies = database_access_.Denies(db_name);
    if (role_) denies |= role_->DeniesDB(db_name);
    return denies;
  }
  bool GrantsDB(std::string_view db_name) const {
    bool grants = database_access_.Grants(db_name);
    if (role_) grants |= role_->GrantsDB(db_name);
    return grants;
  }
  bool HasAccess(std::string_view db_name) const { return !DeniesDB(db_name) && GrantsDB(db_name); }
#endif

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static User Deserialize(const nlohmann::json &data);

  friend bool operator==(const User &first, const User &second);

 private:
  std::string username_;
  std::optional<HashedPassword> password_hash_;
  Permissions permissions_;
#ifdef MG_ENTERPRISE
  FineGrainedAccessHandler fine_grained_access_handler_;
  Databases database_access_{};
#endif
  std::optional<Role> role_;
};

bool operator==(const User &first, const User &second);

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions Merge(const FineGrainedAccessPermissions &first,
                                   const FineGrainedAccessPermissions &second);
#endif
}  // namespace memgraph::auth
