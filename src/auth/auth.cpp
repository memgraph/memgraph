#include "auth/auth.hpp"

#include "utils/string.hpp"

namespace auth {

const std::string kUserPrefix = "user:";
const std::string kRolePrefix = "role:";
const std::string kLinkPrefix = "link:";

/**
 * All data stored in the `Auth` storage is stored in an underlying
 * `storage::KVStore`. Because we are using a key-value store to store the data,
 * the data has to be encoded. The encoding used is as follows:
 *
 * User: key="user:<username>", value="<json_encoded_members_of_user>"
 * Role: key="role:<rolename>", value="<json_endoded_members_of_role>"
 *
 * The User->Role relationship isn't stored in the `User` encoded data because
 * we want to be able to delete/modify a Role and have it automatically be
 * removed/modified in all linked users. Because of that we store the links to
 * the role as a foreign-key like mapping in the KVStore. It is saved as
 * follows:
 *
 * key="link:<username>", value="<rolename>"
 */

Auth::Auth(const std::string &storage_directory)
    : storage_(storage_directory) {}

std::experimental::optional<User> Auth::Authenticate(
    const std::string &username, const std::string &password) {
  auto user = GetUser(username);
  if (!user) return std::experimental::nullopt;
  if (!user->CheckPassword(password)) return std::experimental::nullopt;
  return user;
}

std::experimental::optional<User> Auth::GetUser(const std::string &username) {
  auto existing_user = storage_.Get(kUserPrefix + username);
  if (!existing_user) return std::experimental::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_user);
  } catch (const nlohmann::json::parse_error &e) {
    throw utils::BasicException("Couldn't load user data!");
  }

  auto user = User::Deserialize(data);
  auto link = storage_.Get(kLinkPrefix + username);

  if (link) {
    auto role = GetRole(*link);
    if (role) {
      user.SetRole(*role);
    }
  }
  return user;
}

bool Auth::SaveUser(const User &user) {
  if (!storage_.Put(kUserPrefix + user.username(), user.Serialize().dump())) {
    return false;
  }
  if (user.role()) {
    return storage_.Put(kLinkPrefix + user.username(), user.role()->rolename());
  } else {
    return storage_.Delete(kLinkPrefix + user.username());
  }
}

std::experimental::optional<User> Auth::AddUser(const std::string &username) {
  auto existing_user = GetUser(username);
  if (existing_user) return std::experimental::nullopt;
  auto new_user = User(username);
  if (!SaveUser(new_user)) return std::experimental::nullopt;
  return new_user;
}

bool Auth::RemoveUser(const std::string &username) {
  if (!storage_.Get(kUserPrefix + username)) return false;
  if (!storage_.Delete(kLinkPrefix + username)) return false;
  return storage_.Delete(kUserPrefix + username);
}

bool Auth::HasUsers() {
  for (auto it = storage_.begin(); it != storage_.end(); ++it) {
    if (utils::StartsWith(it->first, kUserPrefix)) {
      return true;
    }
  }
  return false;
}

std::experimental::optional<Role> Auth::GetRole(const std::string &rolename) {
  auto existing_role = storage_.Get(kRolePrefix + rolename);
  if (!existing_role) return std::experimental::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_role);
  } catch (const nlohmann::json::parse_error &e) {
    throw utils::BasicException("Couldn't load role data!");
  }

  return Role::Deserialize(data);
}

bool Auth::SaveRole(const Role &role) {
  return storage_.Put(kRolePrefix + role.rolename(), role.Serialize().dump());
}

std::experimental::optional<Role> Auth::AddRole(const std::string &rolename) {
  auto existing_role = GetRole(rolename);
  if (existing_role) return std::experimental::nullopt;
  auto new_role = Role(rolename);
  if (!SaveRole(new_role)) return std::experimental::nullopt;
  return new_role;
}

bool Auth::RemoveRole(const std::string &rolename) {
  if (!storage_.Get(kRolePrefix + rolename)) return false;
  std::vector<std::string> links;
  for (auto it = storage_.begin(); it != storage_.end(); ++it) {
    if (utils::StartsWith(it->first, kLinkPrefix) && it->second == rolename) {
      links.push_back(it->first);
    }
  }
  for (const auto &link : links) {
    storage_.Delete(link);
  }
  return storage_.Delete(kRolePrefix + rolename);
}

std::mutex &Auth::WithLock() { return lock_; }

}  // namespace auth
