#include "auth/auth.hpp"

#include "auth/exceptions.hpp"
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

std::experimental::optional<User> Auth::GetUser(
    const std::string &username_orig) {
  auto username = utils::ToLowerCase(username_orig);
  auto existing_user = storage_.Get(kUserPrefix + username);
  if (!existing_user) return std::experimental::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_user);
  } catch (const nlohmann::json::parse_error &e) {
    throw AuthException("Couldn't load user data!");
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

void Auth::SaveUser(const User &user) {
  bool success = false;
  if (user.role()) {
    success = storage_.PutMultiple(
        {{kUserPrefix + user.username(), user.Serialize().dump()},
         {kLinkPrefix + user.username(), user.role()->rolename()}});
  } else {
    success = storage_.PutAndDeleteMultiple(
        {{kUserPrefix + user.username(), user.Serialize().dump()}},
        {kLinkPrefix + user.username()});
  }
  if (!success) {
    throw AuthException("Couldn't save user '{}'!", user.username());
  }
}

std::experimental::optional<User> Auth::AddUser(
    const std::string &username,
    const std::experimental::optional<std::string> &password) {
  auto existing_user = GetUser(username);
  if (existing_user) return std::experimental::nullopt;
  auto existing_role = GetRole(username);
  if (existing_role) return std::experimental::nullopt;
  auto new_user = User(username);
  new_user.UpdatePassword(password);
  SaveUser(new_user);
  return new_user;
}

bool Auth::RemoveUser(const std::string &username_orig) {
  auto username = utils::ToLowerCase(username_orig);
  if (!storage_.Get(kUserPrefix + username)) return false;
  std::vector<std::string> keys(
      {kLinkPrefix + username, kUserPrefix + username});
  if (!storage_.DeleteMultiple(keys)) {
    throw AuthException("Couldn't remove user '{}'!", username);
  }
  return true;
}

std::vector<auth::User> Auth::AllUsers() {
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kUserPrefix); it != storage_.end(kUserPrefix);
       ++it) {
    auto username = it->first.substr(kUserPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    auto user = GetUser(username);
    if (user) {
      ret.push_back(*user);
    }
  }
  return ret;
}

bool Auth::HasUsers() {
  return storage_.begin(kUserPrefix) != storage_.end(kUserPrefix);
}

std::experimental::optional<Role> Auth::GetRole(
    const std::string &rolename_orig) {
  auto rolename = utils::ToLowerCase(rolename_orig);
  auto existing_role = storage_.Get(kRolePrefix + rolename);
  if (!existing_role) return std::experimental::nullopt;

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*existing_role);
  } catch (const nlohmann::json::parse_error &e) {
    throw AuthException("Couldn't load role data!");
  }

  return Role::Deserialize(data);
}

void Auth::SaveRole(const Role &role) {
  if (!storage_.Put(kRolePrefix + role.rolename(), role.Serialize().dump())) {
    throw AuthException("Couldn't save role '{}'!", role.rolename());
  }
}

std::experimental::optional<Role> Auth::AddRole(const std::string &rolename) {
  auto existing_role = GetRole(rolename);
  if (existing_role) return std::experimental::nullopt;
  auto existing_user = GetUser(rolename);
  if (existing_user) return std::experimental::nullopt;
  auto new_role = Role(rolename);
  SaveRole(new_role);
  return new_role;
}

bool Auth::RemoveRole(const std::string &rolename_orig) {
  auto rolename = utils::ToLowerCase(rolename_orig);
  if (!storage_.Get(kRolePrefix + rolename)) return false;
  std::vector<std::string> keys;
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix);
       ++it) {
    if (utils::ToLowerCase(it->second) == rolename) {
      keys.push_back(it->first);
    }
  }
  keys.push_back(kRolePrefix + rolename);
  if (!storage_.DeleteMultiple(keys)) {
    throw AuthException("Couldn't remove role '{}'!", rolename);
  }
  return true;
}

std::vector<auth::Role> Auth::AllRoles() {
  std::vector<auth::Role> ret;
  for (auto it = storage_.begin(kRolePrefix); it != storage_.end(kRolePrefix);
       ++it) {
    auto rolename = it->first.substr(kRolePrefix.size());
    if (rolename != utils::ToLowerCase(rolename)) continue;
    auto role = GetRole(rolename);
    if (role) {
      ret.push_back(*role);
    } else {
      throw AuthException("Couldn't load role '{}'!", rolename);
    }
  }
  return ret;
}

std::vector<auth::User> Auth::AllUsersForRole(const std::string &rolename_orig) {
  auto rolename = utils::ToLowerCase(rolename_orig);
  std::vector<auth::User> ret;
  for (auto it = storage_.begin(kLinkPrefix); it != storage_.end(kLinkPrefix);
       ++it) {
    auto username = it->first.substr(kLinkPrefix.size());
    if (username != utils::ToLowerCase(username)) continue;
    if (it->second != utils::ToLowerCase(it->second)) continue;
    if (it->second == rolename) {
      auto user = GetUser(username);
      if (user) {
        ret.push_back(*user);
      } else {
        throw AuthException("Couldn't load user '{}'!", username);
      }
    }
  }
  return ret;
}

std::mutex &Auth::WithLock() { return lock_; }

}  // namespace auth
