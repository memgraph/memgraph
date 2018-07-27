#pragma once

#include <experimental/optional>
#include <mutex>

#include "auth/models.hpp"
#include "storage/kvstore.hpp"

namespace auth {

/**
 * This class serves as the main Authentication/Authorization storage.
 * It provides functions for managing Users, Roles and Permissions.
 * NOTE: The functions in this class aren't thread safe. Use the `WithLock` lock
 * if you want to have safe modifications of the storage.
 */
class Auth final {
 public:
  Auth(const std::string &storage_directory);

  std::experimental::optional<User> Authenticate(const std::string &username,
                                                 const std::string &password);

  std::experimental::optional<User> GetUser(const std::string &username);

  bool SaveUser(const User &user);

  std::experimental::optional<User> AddUser(const std::string &username);

  bool RemoveUser(const std::string &username);

  bool HasUsers();

  std::experimental::optional<Role> GetRole(const std::string &rolename);

  bool SaveRole(const Role &role);

  std::experimental::optional<Role> AddRole(const std::string &rolename);

  bool RemoveRole(const std::string &rolename);

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
