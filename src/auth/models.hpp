// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <nlohmann/json_fwd.hpp>
#include <utility>
#include <variant>
#include "auth/profiles/user_profiles.hpp"
#include "crypto.hpp"
#include "dbms/constants.hpp"
#include "utils/logging.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/uuid.hpp"

namespace memgraph::auth {
// These permissions must have values that are applicable for usage in a
// bitmask.
// clang-format off
enum class Permission : uint64_t {
  MATCH                  = 1,
  CREATE                 = 1U << 1U,
  MERGE                  = 1U << 2U,
  DELETE                 = 1U << 3U,
  SET                    = 1U << 4U,
  REMOVE                 = 1U << 5U,
  INDEX                  = 1U << 6U,
  STATS                  = 1U << 7U,
  CONSTRAINT             = 1U << 8U,
  DUMP                   = 1U << 9U,
  REPLICATION            = 1U << 10U,
  DURABILITY             = 1U << 11U,
  READ_FILE              = 1U << 12U,
  FREE_MEMORY            = 1U << 13U,
  TRIGGER                = 1U << 14U,
  CONFIG                 = 1U << 15U,
  AUTH                   = 1U << 16U,
  STREAM                 = 1U << 17U,
  MODULE_READ            = 1U << 18U,
  MODULE_WRITE           = 1U << 19U,
  WEBSOCKET              = 1U << 20U,
  TRANSACTION_MANAGEMENT = 1U << 21U,
  STORAGE_MODE           = 1U << 22U,
  MULTI_DATABASE_EDIT    = 1U << 23U,
  MULTI_DATABASE_USE     = 1U << 24U,
  COORDINATOR            = 1U << 25U,
  IMPERSONATE_USER       = 1U << 26U,
  PROFILE_RESTRICTION    = 1U << 27U,
};
// clang-format on

#ifdef MG_ENTERPRISE
// clang-format off
enum class FineGrainedPermission : uint64_t {
  NOTHING = 0,
  READ    = 1U << 0U,  // 1
  UPDATE  = 1U << 1U,  // 2
  // Bit 2 reserved: was CREATE_DELETE in Memgraph 3.6 and earlier
  CREATE  = 1U << 3U,  // 8
  DELETE  = 1U << 4U   // 16
};
// clang-format on

constexpr inline FineGrainedPermission operator|(FineGrainedPermission lhs, FineGrainedPermission rhs) {
  return static_cast<FineGrainedPermission>(std::underlying_type_t<FineGrainedPermission>(lhs) |
                                            std::underlying_type_t<FineGrainedPermission>(rhs));
}

constexpr inline uint64_t operator|(uint64_t lhs, FineGrainedPermission rhs) {
  return lhs | static_cast<uint64_t>(rhs);
}

constexpr inline uint64_t operator&(uint64_t lhs, FineGrainedPermission rhs) {
  return (lhs & static_cast<uint64_t>(rhs)) != 0;
}

constexpr inline FineGrainedPermission operator&(FineGrainedPermission lhs, FineGrainedPermission rhs) {
  return static_cast<FineGrainedPermission>(std::underlying_type_t<FineGrainedPermission>(lhs) &
                                            std::underlying_type_t<FineGrainedPermission>(rhs));
}

constexpr inline FineGrainedPermission &operator|=(FineGrainedPermission &lhs, FineGrainedPermission rhs) {
  lhs = lhs | rhs;
  return lhs;
}

constexpr FineGrainedPermission kAllPermissions = static_cast<FineGrainedPermission>(
    memgraph::auth::FineGrainedPermission::CREATE | memgraph::auth::FineGrainedPermission::DELETE |
    memgraph::auth::FineGrainedPermission::UPDATE | memgraph::auth::FineGrainedPermission::READ);
#endif

// Function that converts a permission to its string representation.
std::string PermissionToString(Permission permission);

// Class that indicates what permission level the user/role has.
enum class PermissionLevel : uint8_t { GRANT, NEUTRAL, DENY };

// Function that converts a permission level to its string representation.
std::string PermissionLevelToString(PermissionLevel level);

#ifdef MG_ENTERPRISE
// Function that converts a label permission bitmask to its string representation.
std::string FineGrainedPermissionToString(uint64_t permission);

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
class User;
class UserImpersonation {
 public:
  struct UserId {
    std::string name;
    utils::UUID uuid;

    void to_json(nlohmann::json &data, const UserId &uid);
    void from_json(const nlohmann::json &data, UserId &uid);

    friend std::strong_ordering operator<=>(UserId const &lhs, UserId const &rhs) { return lhs.name <=> rhs.name; };
  };
  struct GrantAllUsers {};
  using GrantedUsers = std::variant<std::set<UserId>, GrantAllUsers>;  // Default to no granted user
  using DeniedUsers = std::set<UserId>;

  UserImpersonation() = default;
  UserImpersonation(GrantedUsers granted, DeniedUsers denied)
      : granted_{std::move(granted)}, denied_{std::move(denied)} {}

  void GrantAll();

  void Grant(const std::vector<User> &users);

  void Deny(const std::vector<User> &users);

  bool CanImpersonate(const User &user) const;

  bool IsDenied(const User &user) const;
  bool IsGranted(const User &user) const;

  const auto &granted() const { return granted_; }
  const auto &denied() const { return denied_; }

  friend void to_json(nlohmann::json &data, const UserImpersonation &usr_imp);
  friend void from_json(const nlohmann::json &data, UserImpersonation &usr_imp);

 private:
  void grant_one(const User &user);

  void deny_one(const User &user);

  bool grants_all() const { return std::holds_alternative<GrantAllUsers>(granted_); }

  std::optional<std::set<UserId>::iterator> find_granted(std::string_view username) const {
    DMG_ASSERT(std::holds_alternative<std::set<UserId>>(granted_));
    auto &granted_set = std::get<std::set<UserId>>(granted_);
    auto res = std::find_if(granted_set.begin(), granted_set.end(),
                            [username](const auto &elem) { return elem.name == username; });
    if (res == granted_set.end()) return {};
    return res;
  }

  void erase_granted(auto itr) const {
    DMG_ASSERT(std::holds_alternative<std::set<UserId>>(granted_));
    auto &granted_set = std::get<std::set<UserId>>(granted_);
    granted_set.erase(itr);
  }

  void emplace_granted(auto &&...args) {
    DMG_ASSERT(std::holds_alternative<std::set<UserId>>(granted_));
    auto &granted_set = std::get<std::set<UserId>>(granted_);
    granted_set.emplace(std::forward<decltype(args)>(args)...);
  }

  std::optional<std::set<UserId>::iterator> find_denied(std::string_view username) const {
    auto res =
        std::find_if(denied_.begin(), denied_.end(), [username](const auto &elem) { return elem.name == username; });
    if (res == denied_.end()) return {};
    return res;
  }

  void erase_denied(auto itr) const { denied_.erase(itr); }

  mutable GrantedUsers granted_;
  mutable DeniedUsers denied_;
};
#endif

#ifdef MG_ENTERPRISE
enum class MatchingMode : uint8_t { ANY, EXACTLY };

struct FineGrainedAccessRule {
  std::unordered_set<std::string> symbols;
  FineGrainedPermission permissions;
  MatchingMode matching_mode;

  bool operator==(const FineGrainedAccessRule &other) const = default;
};

class FineGrainedAccessPermissions final {
 public:
  explicit FineGrainedAccessPermissions(std::optional<uint64_t> global_permission = std::nullopt,
                                        std::vector<FineGrainedAccessRule> rules = {});
  FineGrainedAccessPermissions(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions &operator=(const FineGrainedAccessPermissions &) = default;
  FineGrainedAccessPermissions(FineGrainedAccessPermissions &&) = default;
  FineGrainedAccessPermissions &operator=(FineGrainedAccessPermissions &&) = default;
  ~FineGrainedAccessPermissions() = default;

  PermissionLevel Has(std::span<const std::string> symbols, FineGrainedPermission fine_grained_permission) const;

  PermissionLevel HasGlobal(FineGrainedPermission fine_grained_permission) const;

  void Grant(std::unordered_set<std::string> const &symbols, FineGrainedPermission fine_grained_permission,
             MatchingMode matching_mode = MatchingMode::ANY);

  void GrantGlobal(FineGrainedPermission fine_grained_permission);

  void Revoke(std::unordered_set<std::string> const &symbols, FineGrainedPermission fine_grained_permission,
              MatchingMode matching_mode = MatchingMode::ANY);

  void RevokeGlobal(FineGrainedPermission fine_grained_permission);

  void RevokeAll();

  void RevokeAll(FineGrainedPermission fine_grained_permission);

  nlohmann::json Serialize() const;

  /// @throw AuthException if unable to deserialize.
  static FineGrainedAccessPermissions Deserialize(const nlohmann::json &data);

  const std::optional<uint64_t> &GetGlobalPermission() const;
  const std::vector<FineGrainedAccessRule> &GetPermissions() const;

 private:
  std::optional<uint64_t> global_permission_;
  std::vector<FineGrainedAccessRule> rules_;
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
       FineGrainedAccessHandler fine_grained_access_handler, Databases db_access = {},
       std::optional<UserImpersonation> usr_imp = std::nullopt);
#endif
  Role(const Role &) = default;
  Role &operator=(const Role &) = default;
  Role(Role &&) noexcept = default;
  Role &operator=(Role &&) noexcept = default;
  ~Role() = default;

  const std::string &rolename() const;
  const Permissions &permissions() const;
  Permissions &permissions();
  Permissions GetPermissions(std::optional<std::string_view> db_name = std::nullopt) const {
#ifdef MG_ENTERPRISE
    if (!db_name || HasAccess(*db_name)) {
      return permissions_;
    }
    return Permissions{};  // Return empty permissions if no access to the database
#else
    return permissions_;
#endif
  }
#ifdef MG_ENTERPRISE
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();
  const FineGrainedAccessPermissions &GetFineGrainedAccessLabelPermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  const FineGrainedAccessPermissions &GetFineGrainedAccessEdgeTypePermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
#endif

#ifdef MG_ENTERPRISE
  Databases &db_access() { return db_access_; }
  const Databases &db_access() const { return db_access_; }
  const std::string &GetMain() const { return db_access_.GetMain(); }
  bool DeniesDB(std::string_view db_name) const { return db_access_.Denies(db_name); }
  bool GrantsDB(std::string_view db_name) const { return db_access_.Grants(db_name); }
  bool HasAccess(std::string_view db_name) const { return !DeniesDB(db_name) && GrantsDB(db_name); }
#endif

#ifdef MG_ENTERPRISE
  bool CanImpersonate(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    // Check if we have access to the database if specified
    if (db_name && !HasAccess(*db_name)) {
      return false;
    }
    return user_impersonation_ && permissions_.Has(Permission::IMPERSONATE_USER) == PermissionLevel::GRANT &&
           user_impersonation_->CanImpersonate(user);
  }

  bool UserImpIsGranted(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    if (db_name && !HasAccess(*db_name)) {
      return false;
    }
    return user_impersonation_ && user_impersonation_->IsGranted(user);
  }
  bool UserImpIsDenied(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    if (db_name && !HasAccess(*db_name)) {
      return false;
    }
    return user_impersonation_ && user_impersonation_->IsDenied(user);
  }

  void RevokeUserImp() { user_impersonation_.reset(); }
  void GrantUserImp() {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->GrantAll();
  }
  void GrantUserImp(const std::vector<User> &users) {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->Grant(users);
  }
  void DenyUserImp(const std::vector<User> &users) {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->Deny(users);
  }

  const auto &user_impersonation() const { return user_impersonation_; }
#endif

  // Profile management moved to UserProfiles class

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
  std::optional<UserImpersonation> user_impersonation_;
  // Profile data moved to UserProfiles class
#endif
};

bool operator==(const Role &first, const Role &second);

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions Merge(const FineGrainedAccessPermissions &first,
                                   const FineGrainedAccessPermissions &second);
#endif

}  // namespace memgraph::auth

// Hash function for Role to enable use in unordered_set
namespace std {
template <>
struct hash<memgraph::auth::Role> {
  std::size_t operator()(const memgraph::auth::Role &role) const { return std::hash<std::string>{}(role.rolename()); }
};
}  // namespace std

namespace memgraph::auth {

// Class that encapsulates multiple roles and provides read-only API
class Roles {
 public:
  Roles() = default;
  explicit Roles(std::unordered_set<Role> roles) : roles_{std::move(roles)} {}

  // Add a single role
  void AddRole(const Role &role) {
    RemoveRole(role.rolename());
    roles_.insert(role);
  }

  // Remove a role by name
  void RemoveRole(const std::string &rolename) {
    auto it = std::ranges::find(roles_, rolename, &Role::rolename);
    if (it != roles_.end()) {
      roles_.erase(it);
    }
  }

  // Get all roles
  const std::unordered_set<Role> &GetRoles() const { return roles_; }
  std::optional<Role> GetRole(const std::string &rolename) const {
    auto it = std::ranges::find(roles_, rolename, &Role::rolename);
    if (it == roles_.end()) {
      return std::nullopt;
    }
    return *it;
  }

  // Get roles filtered by database access
  std::unordered_set<Role> GetFilteredRoles(std::optional<std::string_view> db_name = std::nullopt) const {
#ifdef MG_ENTERPRISE
    if (!db_name) return roles_;

    std::unordered_set<Role> filtered_roles;
    for (const auto &role : roles_) {
      if (role.HasAccess(*db_name)) {
        filtered_roles.insert(role);
      }
    }
    return filtered_roles;
#else
    return roles_;
#endif
  }

  // Read-only API that combines permissions from all roles
  std::vector<std::string> rolenames() const {
    std::vector<std::string> names;
    names.reserve(roles_.size());
    for (const auto &role : roles_) {
      names.push_back(role.rolename());
    }
    return names;
  }

  Permissions GetPermissions(std::optional<std::string_view> db_name = std::nullopt) const {
    Permissions permissions;
    for (const auto &role : roles_) {
#ifdef MG_ENTERPRISE
      if (!db_name || role.HasAccess(*db_name)) {
        permissions = Permissions{permissions.grants() | role.permissions().grants(),
                                  permissions.denies() | role.permissions().denies()};
      }
#else
      permissions = Permissions{permissions.grants() | role.permissions().grants(),
                                permissions.denies() | role.permissions().denies()};
#endif
    }
    return permissions;
  }

#ifdef MG_ENTERPRISE
  const FineGrainedAccessPermissions &GetFineGrainedAccessLabelPermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;

  const FineGrainedAccessPermissions &GetFineGrainedAccessEdgeTypePermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;

  // No way to define a higher priority database, so we return the first one
  const std::string &GetMain() const {
    static std::string empty_db;
    return roles_.empty() ? empty_db : roles_.begin()->GetMain();
  }

  bool DeniesDB(std::string_view db_name) const {
    return std::ranges::any_of(roles_, [db_name](const auto &role) { return role.DeniesDB(db_name); });
  }

  bool GrantsDB(std::string_view db_name) const {
    return std::ranges::any_of(roles_, [db_name](const auto &role) { return role.GrantsDB(db_name); });
  }

  bool HasAccess(std::string_view db_name) const { return !DeniesDB(db_name) && GrantsDB(db_name); }

  bool UserImpIsGranted(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    return std::ranges::any_of(roles_,
                               [&user, &db_name](const auto &role) { return role.UserImpIsGranted(user, db_name); });
  }

  bool UserImpIsDenied(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    return std::ranges::any_of(roles_,
                               [&user, &db_name](const auto &role) { return role.UserImpIsDenied(user, db_name); });
  }

  bool CanImpersonate(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    return !UserImpIsDenied(user, db_name) && UserImpIsGranted(user, db_name);
  }

  // Profile management moved to UserProfiles class
#endif

  // Iteration support
  auto begin() { return roles_.begin(); }
  auto end() { return roles_.end(); }
  auto begin() const { return roles_.begin(); }
  auto end() const { return roles_.end(); }
  auto cbegin() const { return roles_.cbegin(); }
  auto cend() const { return roles_.cend(); }

  // Size and empty checks
  bool empty() const { return roles_.empty(); }
  size_t size() const { return roles_.size(); }

  // Comparison operators
  friend bool operator==(const Roles &first, const Roles &second) { return first.roles_ == second.roles_; }
  friend bool operator!=(const Roles &first, const Roles &second) { return !(first == second); }

 private:
  std::unordered_set<Role> roles_;
};

// TODO (mferencevic): Implement password expiry.
class User final {
 public:
  User();

  explicit User(const std::string &username);
  User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions,
       utils::UUID uuid = {});
#ifdef MG_ENTERPRISE
  User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions,
       FineGrainedAccessHandler fine_grained_access_handler, Databases db_access = {}, utils::UUID uuid = {},
       std::optional<UserImpersonation> usr_imp = std::nullopt);
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

  void ClearAllRoles();

  void AddRole(const Role &role);
  void RemoveRole(const std::string &rolename) { roles_.RemoveRole(rolename); }
  const Roles &roles() const { return roles_; }
  Roles &roles() { return roles_; }

// Multi-tenant role support
#ifdef MG_ENTERPRISE
  void AddMultiTenantRole(Role role, const std::string &db_name);
  void ClearMultiTenantRoles(const std::string &db_name);
#endif

  // Fine grained access control
#ifdef MG_ENTERPRISE
  FineGrainedAccessPermissions GetFineGrainedAccessLabelPermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  FineGrainedAccessPermissions GetFineGrainedAccessEdgeTypePermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  FineGrainedAccessPermissions GetUserFineGrainedAccessLabelPermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  FineGrainedAccessPermissions GetUserFineGrainedAccessEdgeTypePermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  FineGrainedAccessPermissions GetRoleFineGrainedAccessLabelPermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  FineGrainedAccessPermissions GetRoleFineGrainedAccessEdgeTypePermissions(
      std::optional<std::string_view> db_name = std::nullopt) const;
  const FineGrainedAccessHandler &fine_grained_access_handler() const;
  FineGrainedAccessHandler &fine_grained_access_handler();
#endif
  const std::string &username() const;

  const Permissions &permissions() const;
  Permissions &permissions();

  // Multi-tenant access
#ifdef MG_ENTERPRISE
  Databases &db_access() { return database_access_; }
  const Databases &db_access() const { return database_access_; }

  const std::string &GetMain() const { return database_access_.GetMain(); }

  bool DeniesDB(std::string_view db_name) const { return database_access_.Denies(db_name) || roles_.DeniesDB(db_name); }
  bool GrantsDB(std::string_view db_name) const { return database_access_.Grants(db_name) || roles_.GrantsDB(db_name); }

  bool HasAccess(std::string_view db_name) const { return !DeniesDB(db_name) && GrantsDB(db_name); }
  bool has_access(std::string_view db_name) const {
    return !database_access_.Denies(db_name) && database_access_.Grants(db_name);
  }
#endif

// Impersonate user
#ifdef MG_ENTERPRISE
  bool CanImpersonate(const User &user, std::optional<std::string_view> db_name = std::nullopt) const {
    if (GetPermissions(db_name).Has(Permission::IMPERSONATE_USER) != PermissionLevel::GRANT) return false;
    if (db_name && !HasAccess(*db_name)) return false;  // Users+role level access check

    // Use the Roles class methods that now support database filtering
    bool role_grants = roles_.UserImpIsGranted(user, db_name);
    bool role_denies = roles_.UserImpIsDenied(user, db_name);

    if (!user_impersonation_) return !role_denies && role_grants;
    bool user_grants = role_grants || user_impersonation_->IsGranted(user);
    bool user_denies = role_denies || user_impersonation_->IsDenied(user);
    return !user_denies && user_grants;
  }

  void RevokeUserImp() { user_impersonation_.reset(); }
  void GrantUserImp() {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->GrantAll();
  }
  void GrantUserImp(const std::vector<User> &users) {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->Grant(users);
  }
  void DenyUserImp(const std::vector<User> &users) {
    if (!user_impersonation_) user_impersonation_.emplace();
    user_impersonation_->Deny(users);
  }

  const auto &user_impersonation() const { return user_impersonation_; }
#endif

  // Multi-tenant role management
  Permissions GetPermissions(std::optional<std::string_view> db_name = std::nullopt) const {
#ifdef MG_ENTERPRISE
    if (db_name && !HasAccess(*db_name)) return Permissions{};  // Users+role level access check
    // filter roles based on the database name and combine with user permissions
    const auto &roles_permissions = roles_.GetPermissions(db_name);
    if (!db_name || has_access(*db_name)) {  // User only level access check
      return Permissions{permissions_.grants() | roles_permissions.grants(),
                         permissions_.denies() | roles_permissions.denies()};
    }
    return roles_permissions;
#else
    const auto roles_permissions = roles_.GetPermissions();
    return Permissions{permissions_.grants() | roles_permissions.grants(),
                       permissions_.denies() | roles_permissions.denies()};
#endif
  }

#ifdef MG_ENTERPRISE
  std::unordered_set<Role> GetRoles(std::optional<std::string_view> db_name = std::nullopt) const {
    return roles_.GetFilteredRoles(db_name);
  }

  std::unordered_set<Role> GetMultiTenantRoles(const std::string &db_name) const {
    std::unordered_set<Role> roles;
    try {
      for (const auto &role : db_role_map_.at(db_name)) {
        if (const auto &role_obj = roles_.GetRole(role); role_obj) {
          DMG_ASSERT(role_obj->HasAccess(db_name), "Role {} does not have access to database {}", role, db_name);
          roles.insert(role_obj.value());
        }
      }
    } catch (const std::out_of_range &e) {
      return {};
    }
    return roles;
  }

  // Get multi-tenant role mappings for storage
  const std::unordered_map<std::string, std::unordered_set<std::string>> &GetMultiTenantRoleMappings() const {
    return db_role_map_;
  }

#endif

  const utils::UUID &uuid() const { return uuid_; }

  // Profile management moved to UserProfiles class

  // Read-only API that combines rolenames from all roles
  std::vector<std::string> rolenames() const { return roles_.rolenames(); }

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
  std::optional<UserImpersonation> user_impersonation_{};
  std::unordered_map<std::string, std::unordered_set<std::string>> db_role_map_{};  // Map of database name to role name
  std::unordered_map<std::string, std::unordered_set<std::string>> role_db_map_{};  // Map of role name to database name
  // Profile data moved to UserProfiles class
#endif
  Roles roles_;
  utils::UUID uuid_{};  // To uniquely identify a user
};

bool operator==(const User &first, const User &second);

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions Merge(const FineGrainedAccessPermissions &first,
                                   const FineGrainedAccessPermissions &second);
#endif

}  // namespace memgraph::auth
