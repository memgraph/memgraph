// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <mutex>
#include <optional>
#include <vector>

#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "auth/module.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/settings.hpp"

namespace memgraph::auth {

static const constexpr char *const kAllDatabases = "*";

/**
 * This class serves as the main Authentication/Authorization storage.
 * It provides functions for managing Users, Roles, Permissions and FineGrainedAccessPermissions.
 * NOTE: The non-const functions in this class aren't thread safe.
 * TODO (mferencevic): Disable user/role modification functions when they are
 * being managed by the auth module.
 */
class Auth final {
 public:
  explicit Auth(const std::string &storage_directory);

  /**
   * Authenticates a user using his username and password.
   *
   * @param username
   * @param password
   *
   * @return a user when the username and password match, nullopt otherwise
   * @throw AuthException if unable to authenticate for whatever reason.
   */
  std::optional<User> Authenticate(const std::string &username, const std::string &password);

  /**
   * Gets a user from the storage.
   *
   * @param username
   *
   * @return a user when the user exists, nullopt otherwise
   * @throw AuthException if unable to load user data.
   */
  std::optional<User> GetUser(const std::string &username) const;

  /**
   * Saves a user object to the storage.
   *
   * @param user
   *
   * @throw AuthException if unable to save the user.
   */
  void SaveUser(const User &user);

  /**
   * Creates a user if the user doesn't exist.
   *
   * @param username
   * @param password
   *
   * @return a user when the user is created, nullopt if the user exists
   * @throw AuthException if unable to save the user.
   */
  std::optional<User> AddUser(const std::string &username, const std::optional<std::string> &password = std::nullopt);

  /**
   * Removes a user from the storage.
   *
   * @param username
   *
   * @return `true` if the user existed and was removed, `false` if the user
   *         doesn't exist
   * @throw AuthException if unable to remove the user.
   */
  bool RemoveUser(const std::string &username);

  /**
   * Gets all users from the storage.
   *
   * @return a list of users
   * @throw AuthException if unable to load user data.
   */
  std::vector<User> AllUsers() const;

  /**
   * Returns whether there are users in the storage.
   *
   * @return `true` if the storage contains any users, `false` otherwise
   */
  bool HasUsers() const;

  /**
   * Gets a role from the storage.
   *
   * @param rolename
   *
   * @return a role when the role exists, nullopt otherwise
   * @throw AuthException if unable to load role data.
   */
  std::optional<Role> GetRole(const std::string &rolename) const;

  /**
   * Saves a role object to the storage.
   *
   * @param role
   *
   * @throw AuthException if unable to save the role.
   */
  void SaveRole(const Role &role);

  /**
   * Creates a role if the role doesn't exist.
   *
   * @param rolename
   *
   * @return a role when the role is created, nullopt if the role exists
   * @throw AuthException if unable to save the role.
   */
  std::optional<Role> AddRole(const std::string &rolename);

  /**
   * Removes a role from the storage.
   *
   * @param rolename
   *
   * @return `true` if the role existed and was removed, `false` if the role
   *         doesn't exist
   * @throw AuthException if unable to remove the role.
   */
  bool RemoveRole(const std::string &rolename);

  /**
   * Gets all roles from the storage.
   *
   * @return a list of roles
   * @throw AuthException if unable to load role data.
   */
  std::vector<Role> AllRoles() const;

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
   * @brief Revoke access to individual database for a user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  bool RevokeDatabaseFromUser(const std::string &db, const std::string &name);

  /**
   * @brief Grant access to individual database for a user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  bool GrantDatabaseToUser(const std::string &db, const std::string &name);

  /**
   * @brief Delete a database from all users.
   *
   * @param db name of the database to delete
   * @throw AuthException if unable to read data
   */
  void DeleteDatabase(const std::string &db);

  /**
   * @brief Set main database for an individual user.
   *
   * @param db name of the database to revoke
   * @param name user's username
   * @return true on success
   * @throw AuthException if unable to find or update the user
   */
  bool SetMainDatabase(std::string_view db, const std::string &name);
#endif

 private:
  // Even though the `kvstore::KVStore` class is guaranteed to be thread-safe,
  // Auth is not thread-safe because modifying users and roles might require
  // more than one operation on the storage.
  kvstore::KVStore storage_;
  auth::Module module_;
};
}  // namespace memgraph::auth
