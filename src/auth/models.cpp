#include "auth/models.hpp"

#include "auth/crypto.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"

namespace auth {

Permissions::Permissions(uint64_t grants, uint64_t denies) {
  // The deny bitmask has higher priority than the grant bitmask.
  denies_ = denies;
  // Mask out the grant bitmask to make sure that it is correct.
  grants_ = grants & (~denies);
}

PermissionLevel Permissions::Has(Permission permission) const {
  // Check for the deny first because it has greater priority than a grant.
  if (denies_ & utils::UnderlyingCast(permission)) {
    return PermissionLevel::Deny;
  } else if (grants_ & utils::UnderlyingCast(permission)) {
    return PermissionLevel::Grant;
  }
  return PermissionLevel::Neutral;
}

void Permissions::Grant(Permission permission) {
  // Remove the possible deny.
  denies_ &= ~utils::UnderlyingCast(permission);
  // Now we grant the permission.
  grants_ |= utils::UnderlyingCast(permission);
}

void Permissions::Revoke(Permission permission) {
  // Remove the possible grant.
  grants_ &= ~utils::UnderlyingCast(permission);
  // Remove the possible deny.
  denies_ &= ~utils::UnderlyingCast(permission);
}

void Permissions::Deny(Permission permission) {
  // First deny the permission.
  denies_ |= utils::UnderlyingCast(permission);
  // Remove the possible grant.
  grants_ &= ~utils::UnderlyingCast(permission);
}

nlohmann::json Permissions::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["grants"] = grants_;
  data["denies"] = denies_;
  return data;
}

Permissions Permissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw utils::BasicException("Couldn't load permissions data!");
  }
  if (!data["grants"].is_number_unsigned() ||
      !data["denies"].is_number_unsigned()) {
    throw utils::BasicException("Couldn't load permissions data!");
  }
  return {data["grants"], data["denies"]};
}

uint64_t Permissions::grants() const { return grants_; }
uint64_t Permissions::denies() const { return denies_; }

bool operator==(const Permissions &first, const Permissions &second) {
  return first.grants() == second.grants() && first.denies() == second.denies();
}

bool operator!=(const Permissions &first, const Permissions &second) {
  return !(first == second);
}

Role::Role(const std::string &rolename) : rolename_(rolename) {}

Role::Role(const std::string &rolename, const Permissions &permissions)
    : rolename_(rolename), permissions_(permissions) {}

const std::string &Role::rolename() const { return rolename_; }
const Permissions &Role::permissions() const { return permissions_; }
Permissions &Role::permissions() { return permissions_; }

nlohmann::json Role::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["rolename"] = rolename_;
  data["permissions"] = permissions_.Serialize();
  return data;
}

Role Role::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw utils::BasicException("Couldn't load role data!");
  }
  if (!data["rolename"].is_string() || !data["permissions"].is_object()) {
    throw utils::BasicException("Couldn't load role data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
  return {data["rolename"], permissions};
}

bool operator==(const Role &first, const Role &second) {
  return first.rolename_ == second.rolename_ &&
         first.permissions_ == second.permissions_;
}

User::User(const std::string &username) : username_(username) {}

User::User(const std::string &username, const std::string &password_hash,
           const Permissions &permissions)
    : username_(username),
      password_hash_(password_hash),
      permissions_(permissions) {}

bool User::CheckPassword(const std::string &password) {
  return VerifyPassword(password, password_hash_);
}

void User::UpdatePassword(const std::string &password) {
  password_hash_ = EncryptPassword(password);
}

void User::SetRole(const Role &role) { role_.emplace(role); }

const Permissions User::GetPermissions() const {
  if (role_) {
    return Permissions(permissions_.grants() | role_->permissions().grants(),
                       permissions_.denies() | role_->permissions().denies());
  }
  return permissions_;
}

const std::string &User::username() const { return username_; }

Permissions &User::permissions() { return permissions_; }

std::experimental::optional<Role> User::role() const { return role_; }

nlohmann::json User::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data["username"] = username_;
  data["password_hash"] = password_hash_;
  data["permissions"] = permissions_.Serialize();
  // The role shouldn't be serialized here, it is stored as a foreign key.
  return data;
}

User User::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw utils::BasicException("Couldn't load user data!");
  }
  if (!data["username"].is_string() || !data["password_hash"].is_string() ||
      !data["permissions"].is_object()) {
    throw utils::BasicException("Couldn't load user data!");
  }
  auto permissions = Permissions::Deserialize(data["permissions"]);
  return {data["username"], data["password_hash"], permissions};
}

bool operator==(const User &first, const User &second) {
  return first.username_ == second.username_ &&
         first.password_hash_ == second.password_hash_ &&
         first.permissions_ == second.permissions_ &&
         first.role_ == second.role_;
}
}  // namespace auth
