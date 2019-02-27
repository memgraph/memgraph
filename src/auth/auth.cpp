#include "auth/auth.hpp"

#include <cstring>
#include <limits>
#include <utility>

#include <fmt/format.h>
#include <glog/logging.h>

#include <ldap.h>

#include "auth/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/string.hpp"

DEFINE_bool(auth_ldap_enabled, false,
            "Set to true to enable LDAP authentication.");
DEFINE_bool(
    auth_ldap_issue_starttls, false,
    "Set to true to enable issuing of STARTTLS on LDAP server connections.");
DEFINE_string(auth_ldap_prefix, "cn=",
              "The prefix used when forming the DN for LDAP authentication.");
DEFINE_string(auth_ldap_suffix, "",
              "The suffix used when forming the DN for LDAP authentication.");
DEFINE_string(auth_ldap_host, "", "Host used for LDAP authentication.");
DEFINE_VALIDATED_int32(auth_ldap_port, LDAP_PORT,
                       "Port used for LDAP authentication.",
                       FLAG_IN_RANGE(1, std::numeric_limits<uint16_t>::max()));
DEFINE_bool(auth_ldap_create_user, true,
            "Set to false to disable creation of missing users.");
DEFINE_bool(auth_ldap_create_role, true,
            "Set to false to disable creation of missing roles.");
DEFINE_string(auth_ldap_role_mapping_root_dn, "",
              "Set this value to the DN that contains all role mappings.");

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

#define INIT_ABORT_ON_ERROR(expr) \
  CHECK(expr == LDAP_SUCCESS) << "Couldn't initialize auth stack!";

void Init() {
  // The OpenLDAP manual states that we should call either `ldap_set_option` or
  // `ldap_get_option` once from a single thread so that the internal state of
  // the library is initialized. This is noted in the manual for
  // `ldap_initialize` under the 'Note:'
  // ```
  // Note:  the first call into the LDAP library also initializes the global
  // options for the library. As such  the  first  call  should  be  single-
  // threaded or otherwise protected to insure that only one call is active.
  // It is recommended that ldap_get_option() or ldap_set_option()  be  used
  // in the program's main thread before any additional threads are created.
  // See ldap_get_option(3).
  // ```
  // https://www.openldap.org/software/man.cgi?query=ldap_initialize&sektion=3&apropos=0&manpath=OpenLDAP+2.4-Release
  LDAP *ld = nullptr;
  INIT_ABORT_ON_ERROR(ldap_initialize(&ld, ""));
  int ldap_version = LDAP_VERSION3;
  INIT_ABORT_ON_ERROR(
      ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_version));
  INIT_ABORT_ON_ERROR(ldap_unbind_ext(ld, NULL, NULL));
}

Auth::Auth(const std::string &storage_directory)
    : storage_(storage_directory) {}

/// Converts a `std::string` to a `struct berval`.
std::pair<std::unique_ptr<char[]>, struct berval> LdapConvertString(
    const std::string &s) {
  std::unique_ptr<char[]> data(new char[s.size() + 1]);
  char *ptr = data.get();
  memcpy(ptr, s.c_str(), s.size());
  ptr[s.size()] = '\0';
  return {std::move(data), {s.size(), ptr}};
}

/// Escapes a string so that it can't be used for LDAP DN injection.
/// https://ldapwiki.com/wiki/DN%20Escape%20Values
std::string LdapEscapeString(const std::string &src) {
  std::string ret;
  ret.reserve(src.size() * 2);

  int spaces_leading = 0, spaces_trailing = 0;
  for (int i = 0; i < src.size(); ++i) {
    if (src[i] == ' ') {
      ++spaces_leading;
    } else {
      break;
    }
  }
  for (int i = src.size() - 1; i >= 0; --i) {
    if (src[i] == ' ') {
      ++spaces_trailing;
    } else {
      break;
    }
  }

  for (int i = 0; i < spaces_leading; ++i) {
    ret.append("\\ ");
  }
  for (int i = spaces_leading; i < src.size() - spaces_trailing; ++i) {
    char c = src[i];
    if (c == ',' || c == '\\' || c == '#' || c == '+' || c == '<' || c == '>' ||
        c == ';' || c == '"' || c == '=') {
      ret.append(1, '\\');
    }
    ret.append(1, c);
  }
  for (int i = 0; i < spaces_trailing; ++i) {
    ret.append("\\ ");
  }

  return ret;
}

/// This function searches for a role mapping for the given `user_dn` by
/// searching all first level children of the `role_base_dn` and finding that
/// item that has a `mapping` attribute to the given `user_dn`. The found item's
/// `cn` is used as the role name.
std::experimental::optional<std::string> LdapFindRole(
    LDAP *ld, const std::string &role_base_dn, const std::string &user_dn,
    const std::string &username) {
  auto ldap_user_dn = LdapConvertString(user_dn);

  char *attrs[1] = {nullptr};
  LDAPMessage *msg = nullptr;

  int ret =
      ldap_search_ext_s(ld, role_base_dn.c_str(), LDAP_SCOPE_ONELEVEL, NULL,
                        attrs, 0, NULL, NULL, NULL, LDAP_NO_LIMIT, &msg);
  utils::OnScopeExit cleanup([&msg] { ldap_msgfree(msg); });

  if (ret != LDAP_SUCCESS) {
    LOG(WARNING) << "Couldn't find role for user '" << username
                 << "' using LDAP due to error: " << ldap_err2string(ret);
    return std::experimental::nullopt;
  }

  if (ret == LDAP_SUCCESS && msg != nullptr) {
    for (LDAPMessage *entry = ldap_first_entry(ld, msg); entry != nullptr;
         entry = ldap_next_entry(ld, entry)) {
      char *entry_dn = ldap_get_dn(ld, entry);
      ret = ldap_compare_ext_s(ld, entry_dn, "member", &ldap_user_dn.second,
                               NULL, NULL);
      ldap_memfree(entry_dn);
      if (ret == LDAP_COMPARE_TRUE) {
        auto values = ldap_get_values_len(ld, entry, "cn");
        if (ldap_count_values_len(values) != 1) {
          LOG(WARNING) << "Couldn't find role for user '" << username
                       << "' using LDAP because to the role object doesn't "
                          "have a unique CN attribute!";
          return std::experimental::nullopt;
        }
        return std::string(values[0]->bv_val, values[0]->bv_len);
      } else if (ret != LDAP_COMPARE_FALSE) {
        LOG(WARNING) << "Couldn't find role for user '" << username
                     << "' using LDAP due to error: " << ldap_err2string(ret);
        return std::experimental::nullopt;
      }
    }
  }
  return std::experimental::nullopt;
}

#define LDAP_EXIT_ON_ERROR(expr, username)                                 \
  {                                                                        \
    int r = expr;                                                          \
    if (r != LDAP_SUCCESS) {                                               \
      LOG(WARNING) << "Couldn't authenticate user '" << username           \
                   << "' using LDAP due to error: " << ldap_err2string(r); \
      return std::experimental::nullopt;                                   \
    }                                                                      \
  }

std::experimental::optional<User> Auth::Authenticate(
    const std::string &username, const std::string &password) {
  if (FLAGS_auth_ldap_enabled) {
    LDAP *ld = nullptr;

    // Initialize the LDAP struct.
    std::string uri =
        fmt::format("ldap://{}:{}", FLAGS_auth_ldap_host, FLAGS_auth_ldap_port);
    LDAP_EXIT_ON_ERROR(ldap_initialize(&ld, uri.c_str()), username);

    // After this point the struct is valid and we need to clean it up on exit.
    utils::OnScopeExit cleanup([&ld] { ldap_unbind_ext(ld, NULL, NULL); });

    // Set protocol version used.
    int ldap_version = LDAP_VERSION3;
    LDAP_EXIT_ON_ERROR(
        ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &ldap_version),
        username);

    // Create DN used for authentication.
    std::string distinguished_name = FLAGS_auth_ldap_prefix +
                                     LdapEscapeString(username) +
                                     FLAGS_auth_ldap_suffix;

    // Issue STARTTLS if we are using TLS.
    if (FLAGS_auth_ldap_issue_starttls) {
      LDAP_EXIT_ON_ERROR(ldap_start_tls_s(ld, NULL, NULL), username);
    }

    // Try to authenticate.
    // Since `ldap_simple_bind_s` is now deprecated, we use `ldap_sasl_bind_s`
    // to emulate the simple bind behavior. This is inspired by the following
    // link. They use the async version, we use the sync version.
    // https://github.com/openldap/openldap/blob/b45a6a7dc728d9df18aa1ca7a9aa43dabb1d4037/clients/tools/common.c#L1618
    {
      auto cred = LdapConvertString(password);
      LDAP_EXIT_ON_ERROR(
          ldap_sasl_bind_s(ld, distinguished_name.c_str(), LDAP_SASL_SIMPLE,
                           &cred.second, NULL, NULL, NULL),
          username);
    }

    // Find role name.
    std::experimental::optional<std::string> rolename;
    if (!FLAGS_auth_ldap_role_mapping_root_dn.empty()) {
      rolename = LdapFindRole(ld, FLAGS_auth_ldap_role_mapping_root_dn,
                              distinguished_name, username);
    }

    // Find or create the user and return it.
    auto user = GetUser(username);
    if (!user) {
      if (FLAGS_auth_ldap_create_user) {
        user = AddUser(username, password);
        if (!user) {
          LOG(WARNING)
              << "Couldn't authenticate user '" << username
              << "' using LDAP because the user already exists as a role!";
          return std::experimental::nullopt;
        }
      } else {
        LOG(WARNING) << "Couldn't authenticate user '" << username
                     << "' using LDAP because the user doesn't exist!";
        return std::experimental::nullopt;
      }
    } else {
      user->UpdatePassword(password);
    }
    if (rolename) {
      auto role = GetRole(*rolename);
      if (!role) {
        if (FLAGS_auth_ldap_create_role) {
          role = AddRole(*rolename);
          if (!role) {
            LOG(WARNING) << "Couldn't authenticate user '" << username
                         << "' using LDAP because the user's role '"
                         << *rolename << "' already exists as a user!";
            return std::experimental::nullopt;
          }
          SaveRole(*role);
        } else {
          LOG(WARNING) << "Couldn't authenticate user '" << username
                       << "' using LDAP because the user's role '" << *rolename
                       << "' doesn't exist!";
          return std::experimental::nullopt;
        }
      }
      user->SetRole(*role);
    } else {
      user->ClearRole();
    }
    SaveUser(*user);
    return user;
  } else {
    auto user = GetUser(username);
    if (!user) return std::experimental::nullopt;
    if (!user->CheckPassword(password)) return std::experimental::nullopt;
    return user;
  }
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

std::vector<auth::User> Auth::AllUsersForRole(
    const std::string &rolename_orig) {
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
