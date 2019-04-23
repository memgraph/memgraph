#pragma once

#include <mutex>
#include <optional>
#include <vector>

#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "storage/common/kvstore/kvstore.hpp"

namespace auth {

/**
 * Call this function in each `main` file that uses the Auth stack. It is used
 * to initialize all libraries (primarily OpenLDAP).
 *
 * NOTE: This function must be called **exactly** once.
 */
void Init();

/**
 * This class serves as the main Authentication/Authorization storage.
 * It provides functions for managing Users, Roles and Permissions.
 * NOTE: The functions in this class aren't thread safe. Use the `WithLock` lock
 * if you want to have safe modifications of the storage.
 */
class Auth final {
 public:
  Auth(const std::string &storage_directory);

  /**
   * Authenticates a user using his username and password.
   *
   * @param username
   * @param password
   *
   * @return a user when the username and password match, nullopt otherwise
   */
  std::optional<User> Authenticate(const std::string &username,
                                   const std::string &password);

  /**
   * Gets a user from the storage.
   *
   * @param username
   *
   * @return a user when the user exists, nullopt otherwise
   */
  std::optional<User> GetUser(const std::string &username);

  /**
   * Saves a user object to the storage.
   *
   * @param user
   */
  void SaveUser(const User &user);

  /**
   * Creates a user if the user doesn't exist.
   *
   * @param username
   * @param password
   *
   * @return a user when the user is created, nullopt if the user exists
   */
  std::optional<User> AddUser(
      const std::string &username,
      const std::optional<std::string> &password = std::nullopt);

  /**
   * Removes a user from the storage.
   *
   * @param username
   *
   * @return `true` if the user existed and was removed, `false` if the user
   *         doesn't exist
   */
  bool RemoveUser(const std::string &username);

  /**
   * Gets all users from the storage.
   *
   * @return a list of users
   */
  std::vector<User> AllUsers();

  /**
   * Returns whether there are users in the storage.
   *
   * @return `true` if the storage contains any users, `false` otherwise
   */
  bool HasUsers();

  /**
   * Gets a role from the storage.
   *
   * @param rolename
   *
   * @return a role when the role exists, nullopt otherwise
   */
  std::optional<Role> GetRole(const std::string &rolename);

  /**
   * Saves a role object to the storage.
   *
   * @param role
   */
  void SaveRole(const Role &role);

  /**
   * Creates a role if the role doesn't exist.
   *
   * @param rolename
   *
   * @return a role when the role is created, nullopt if the role exists
   */
  std::optional<Role> AddRole(const std::string &rolename);

  /**
   * Removes a role from the storage.
   *
   * @param rolename
   *
   * @return `true` if the role existed and was removed, `false` if the role
   *         doesn't exist
   */
  bool RemoveRole(const std::string &rolename);

  /**
   * Gets all roles from the storage.
   *
   * @return a list of roles
   */
  std::vector<Role> AllRoles();

  /**
   * Gets all users for a role from the storage.
   *
   * @param rolename
   *
   * @return a list of roles
   */
  std::vector<User> AllUsersForRole(const std::string &rolename);

  /**
   * Returns a reference to the lock that should be used for all operations that
   * require more than one interaction with this class.
   */
  std::mutex &WithLock();

 private:
  storage::KVStore storage_;
  // Even though the `storage::KVStore` class is guaranteed to be thread-safe we
  // use a mutex to lock all operations on the `User` and `Role` storage because
  // some operations on the users and/or roles may require more than one
  // operation on the storage.
  std::mutex lock_;
};
}  // namespace auth
