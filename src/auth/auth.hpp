// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <mutex>
#include <optional>
#include <regex>
#include <vector>

#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "auth/module.hpp"
#include "glue/auth_global.hpp"
#include "kvstore/kvstore.hpp"
#include "system/action.hpp"
#include "utils/settings.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::auth {

class Auth;
using SynchedAuth = memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock>;

static const constexpr char *const kAllDatabases = "*";

struct RoleWUsername : Role {
  template <typename... Args>
  RoleWUsername(std::string_view username, Args &&...args) : Role{std::forward<Args>(args)...}, username_{username} {}

  std::string username() { return username_; }
  const std::string &username() const { return username_; }

 private:
  std::string username_;
};
using UserOrRole = std::variant<User, RoleWUsername>;

/**
 * This class serves as the main Authentication/Authorization storage.
 * It provides functions for managing Users, Roles, Permissions and FineGrainedAccessPermissions.
 * NOTE: The non-const functions in this class aren't thread safe.
 * TODO (mferencevic): Disable user/role modification functions when they are
 * being managed by the auth module.
 */
class Auth final {
 public:
  struct Config {
    Config() {}
    Config(std::string name_regex, std::string password_regex, bool password_permit_null)
        : name_regex_str{std::move(name_regex)},
          password_regex_str{std::move(password_regex)},
          password_permit_null{password_permit_null},
          custom_name_regex{name_regex_str != glue::kDefaultUserRoleRegex},
          name_regex{name_regex_str},
          custom_password_regex{password_regex_str != glue::kDefaultPasswordRegex},
          password_regex{password_regex_str} {}

    std::string name_regex_str{glue::kDefaultUserRoleRegex};
    std::string password_regex_str{glue::kDefaultPasswordRegex};
    bool password_permit_null{true};

   private:
    friend class Auth;
    bool custom_name_regex{false};
    std::regex name_regex{name_regex_str};
    bool custom_password_regex{false};
    std::regex password_regex{password_regex_str};
  };

  struct Epoch {
    Epoch() : epoch_{0} {}
    Epoch(unsigned e) : epoch_{e} {}

    Epoch operator++() { return ++epoch_; }
    bool operator==(const Epoch &rhs) const = default;

   private:
    unsigned epoch_;
  };

  static const Epoch kStartEpoch;

  enum class Result {
    SUCCESS,
    NO_USER_ROLE,
    NO_ROLE,
  };

  explicit Auth(std::string storage_directory, Config config);

  /**
   * @brief Set the Config object
   *
   * @param config
   */
  void SetConfig(Config config) {
    // NOTE: The Auth class itself is not thread-safe, higher-level code needs to synchronize it when using it.
    config_ = std::move(config);
  }

  /**
   * @brief
   *
   * @return Config
   */
  Config GetConfig() const { return config_; }

  /**
   * Authenticates a user using his username and password.
   *
   * @param username
   * @param password
   *
   * @return a user when the username and password match, nullopt otherwise
   * @throw AuthException if unable to authenticate for whatever reason.
   */
  std::optional<UserOrRole> Authenticate(const std::string &username, const std::string &password);

  /**
   * Gets a user from the storage.
   *
   * @param username
   *
   * @return a user when the user exists, nullopt otherwise
   * @throw AuthException if unable to load user data.
   */
  std::optional<User> GetUser(const std::string &username) const;

  void LinkUser(User &user) const;

  /**
   * Saves a user object to the storage.
   *
   * @param user
   *
   * @throw AuthException if unable to save the user.
   */
  void SaveUser(const User &user, system::Transaction *system_tx = nullptr);

  /**
   * Creates a user if the user doesn't exist.
   *
   * @param username
   * @param password
   *
   * @return a user when the user is created, nullopt if the user exists
   * @throw AuthException if unable to save the user.
   */
  std::optional<User> AddUser(const std::string &username, const std::optional<std::string> &password = std::nullopt,
                              system::Transaction *system_tx = nullptr);

  /**
   * Removes a user from the storage.
   *
   * @param username
   *
   * @return `true` if the user existed and was removed, `false` if the user
   *         doesn't exist
   * @throw AuthException if unable to remove the user.
   */
  bool RemoveUser(const std::string &username, system::Transaction *system_tx = nullptr);

  /**
   * @brief
   *
   * @param user
   * @param password
   */
  void UpdatePassword(auth::User &user, const std::optional<std::string> &password);

  /**
   * Gets all users from the storage.
   *
   * @return a list of users
   * @throw AuthException if unable to load user data.
   */
  std::vector<User> AllUsers() const;

  /**
   * @brief
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> AllUsernames() const;

  /**
   * Returns whether there are users in the storage.
   *
   * @return `true` if the storage contains any users, `false` otherwise
   */
  bool HasUsers() const;

  /**
   * Returns whether the access is controlled by authentication/authorization.
   *
   * @return `true` if auth needs to run
   */
  bool AccessControlled() const;

  /**
   * Gets a role from the storage.
   *
   * @param rolename
   *
   * @return a role when the role exists, nullopt otherwise
   * @throw AuthException if unable to load role data.
   */
  std::optional<Role> GetRole(const std::string &rolename) const;

  std::optional<UserOrRole> GetUserOrRole(const std::optional<std::string> &username,
                                          const std::optional<std::string> &rolename) const {
    auto expect = [](bool condition, std::string &&msg) {
      if (!condition) throw AuthException(std::move(msg));
    };
    // Special case if we are using a module; we must find the specified role
    if (module_.IsUsed()) {
      expect(username && rolename, "When using a module, a role needs to be connected to a username.");
      const auto role = GetRole(*rolename);
      expect(role != std::nullopt, "No role named " + *rolename);
      return UserOrRole(auth::RoleWUsername{*username, *role});
    }

    // First check if we need to find a role
    if (username && rolename) {
      const auto role = GetRole(*rolename);
      expect(role != std::nullopt, "No role named " + *rolename);
      return UserOrRole(auth::RoleWUsername{*username, *role});
    }

    // We are only looking for a user
    if (username) {
      const auto user = GetUser(*username);
      expect(user != std::nullopt, "No user named " + *username);
      return *user;
    }

    // No user or role
    return std::nullopt;
  }

  /**
   * Saves a role object to the storage.
   *
   * @param role
   *
   * @throw AuthException if unable to save the role.
   */
  void SaveRole(const Role &role, system::Transaction *system_tx = nullptr);

  /**
   * Creates a role if the role doesn't exist.
   *
   * @param rolename
   *
   * @return a role when the role is created, nullopt if the role exists
   * @throw AuthException if unable to save the role.
   */
  std::optional<Role> AddRole(const std::string &rolename, system::Transaction *system_tx = nullptr);

  /**
   * Removes a role from the storage.
   *
   * @param rolename
   *
   * @return `true` if the role existed and was removed, `false` if the role
   *         doesn't exist
   * @throw AuthException if unable to remove the role.
   */
  bool RemoveRole(const std::string &rolename, system::Transaction *system_tx = nullptr);

  /**
   * Gets all roles from the storage.
   *
   * @return a list of roles
   * @throw AuthException if unable to load role data.
   */
  std::vector<Role> AllRoles() const;

  /**
   * @brief
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> AllRolenames() const;

  /**
   * Gets all users for a role from the storage.
   *
   * @param rolename
   *
   * @return a list of roles
   * @throw AuthException if unable to load user data.
   */
  std::vector<User> AllUsersForRole(const std::string &rolename) const;

#ifdef MG_ENTERPRISE
  /**
   * @brief Grant access to individual database for a user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  Result GrantDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx = nullptr);
  void GrantDatabase(const std::string &db, User &user, system::Transaction *system_tx = nullptr);
  void GrantDatabase(const std::string &db, Role &role, system::Transaction *system_tx = nullptr);

  /**
   * @brief Revoke access to individual database for a user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  Result DenyDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx = nullptr);
  void DenyDatabase(const std::string &db, User &user, system::Transaction *system_tx = nullptr);
  void DenyDatabase(const std::string &db, Role &role, system::Transaction *system_tx = nullptr);

  /**
   * @brief Revoke access to individual database for a user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  Result RevokeDatabase(const std::string &db, const std::string &name, system::Transaction *system_tx = nullptr);
  void RevokeDatabase(const std::string &db, User &user, system::Transaction *system_tx = nullptr);
  void RevokeDatabase(const std::string &db, Role &role, system::Transaction *system_tx = nullptr);

  /**
   * @brief Delete a database from all users.
   *
   * @param db name of the database to delete
   * @throw AuthException if unable to read data
   */
  void DeleteDatabase(const std::string &db, system::Transaction *system_tx = nullptr);

  /**
   * @brief Set main database for an individual user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  Result SetMainDatabase(std::string_view db, const std::string &name, system::Transaction *system_tx = nullptr);
  void SetMainDatabase(std::string_view db, User &user, system::Transaction *system_tx = nullptr);
  void SetMainDatabase(std::string_view db, Role &role, system::Transaction *system_tx = nullptr);
#endif

  bool UpToDate(Epoch &e) const {
    bool res = e == epoch_;
    e = epoch_;
    return res;
  }

 private:
  /**
   * @brief
   *
   * @param user_or_role
   * @return true
   * @return false
   */
  bool NameRegexMatch(const std::string &user_or_role) const;

  void UpdateEpoch() { ++epoch_; }

  void DisableIfModuleUsed() const {
    if (module_.IsUsed()) throw AuthException("Operation not permited when using an authentication module.");
  }

  // Even though the `kvstore::KVStore` class is guaranteed to be thread-safe,
  // Auth is not thread-safe because modifying users and roles might require
  // more than one operation on the storage.
  kvstore::KVStore storage_;
  auth::Module module_;
  Config config_;
  Epoch epoch_{kStartEpoch};
};
}  // namespace memgraph::auth
