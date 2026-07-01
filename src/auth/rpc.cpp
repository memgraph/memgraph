// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "auth/rpc.hpp"

#include <nlohmann/json.hpp>
#include "auth/auth.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/enum.hpp"

namespace {
void AttachRolesToUser(
    memgraph::auth::User &user, std::vector<memgraph::auth::Role> const &roles,
    [[maybe_unused]] std::unordered_map<std::string, std::unordered_set<std::string>> const &mt_map) {
  user.ClearAllRoles();
#ifdef MG_ENTERPRISE
  for (auto const &[db, role_names] : mt_map) {
    for (auto const &role_name : role_names) {
      if (auto it = std::ranges::find_if(roles, [&role_name](auto const &r) { return r.rolename() == role_name; });
          it != roles.end()) {
        user.AddMultiTenantRole(*it, db);
      }
    }
  }
#endif
  for (auto const &role : roles) {
    try {
      user.AddRole(role);
    } catch (memgraph::auth::AuthException const &) {
      continue;
    }
  }
}
}  // namespace

namespace memgraph::slk {
// Serialize code for auth::Role
void Save(const auth::Role &self, Builder *builder) { memgraph::slk::Save(self.Serialize().dump(), builder); }

namespace {
auth::Role LoadAuthRole(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  auto const json = nlohmann::json::parse(tmp);
  return memgraph::auth::Role::Deserialize(json);
}

auth::Role LoadAndMigrateRole(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  auto json = nlohmann::json::parse(tmp);
  memgraph::auth::MigrateAuthJson(json);
  return memgraph::auth::Role::Deserialize(json);
}

auth::User LoadAndMigrateUser(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  auto json = nlohmann::json::parse(tmp);
  memgraph::auth::MigrateAuthJson(json);
  auto user = memgraph::auth::User::Deserialize(json);

  // Load roles vector manually so each role gets migrated
  uint64_t num_roles = 0;
  memgraph::slk::Load(&num_roles, reader);
  std::vector<auth::Role> roles;
  roles.reserve(num_roles);
  for (uint64_t i = 0; i < num_roles; ++i) {
    roles.push_back(LoadAndMigrateRole(reader));
  }

#ifdef MG_ENTERPRISE
  std::unordered_map<std::string, std::unordered_set<std::string>> mt_map;
  memgraph::slk::Load(&mt_map, reader);
  AttachRolesToUser(user, roles, mt_map);
#else
  AttachRolesToUser(user, roles, {});
#endif
  return user;
}

std::string LoadJsonStringRaw(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  return tmp;
}
}  // namespace

// Deserialize code for auth::Role
void Load(auth::Role *self, memgraph::slk::Reader *reader) { *self = LoadAuthRole(reader); }

// Special case for optional<Role>
template <>
inline void Load<auth::Role>(std::optional<auth::Role> *obj, Reader *reader) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    obj->emplace(LoadAuthRole(reader));
  } else {
    *obj = std::nullopt;
  }
}

// Serialize code for auth::User
void Save(const auth::User &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.Serialize().dump(), builder);
  std::vector<auth::Role> roles{self.roles().cbegin(), self.roles().cend()};
  memgraph::slk::Save(roles, builder);
#ifdef MG_ENTERPRISE
  memgraph::slk::Save(self.GetMultiTenantRoleMappings(), builder);
#endif
}

// Deserialize code for auth::User
void Load(auth::User *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  auto const json = nlohmann::json::parse(tmp);
  *self = memgraph::auth::User::Deserialize(json);
  std::vector<auth::Role> roles;
  memgraph::slk::Load(&roles, reader);
#ifdef MG_ENTERPRISE
  std::unordered_map<std::string, std::unordered_set<std::string>> mt_map;
  memgraph::slk::Load(&mt_map, reader);
  AttachRolesToUser(*self, roles, mt_map);
#else
  AttachRolesToUser(*self, roles, {});
#endif
}

// Serialize code for auth::Auth::Config
void Save(const auth::Auth::Config &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.name_regex_str, builder);
  memgraph::slk::Save(self.password_regex_str, builder);
  memgraph::slk::Save(self.password_permit_null, builder);
}

// Deserialize code for auth::Auth::Config
void Load(auth::Auth::Config *self, memgraph::slk::Reader *reader) {
  std::string name_regex_str{};
  std::string password_regex_str{};
  bool password_permit_null{};

  memgraph::slk::Load(&name_regex_str, reader);
  memgraph::slk::Load(&password_regex_str, reader);
  memgraph::slk::Load(&password_permit_null, reader);

  *self = auth::Auth::Config{std::move(name_regex_str), std::move(password_regex_str), password_permit_null};
}

void Save(const memgraph::replication::UpdateAuthDataReqV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  bool const has_user = self.user_json.has_value();
  memgraph::slk::Save(has_user, builder);
  if (has_user) {
    memgraph::slk::Save(*self.user_json, builder);
    static std::vector<std::string> const kEmptyRoleJsons;
    auto const &role_jsons = self.user_role_jsons ? *self.user_role_jsons : kEmptyRoleJsons;
    memgraph::slk::Save(static_cast<uint64_t>(role_jsons.size()), builder);
    for (auto const &rj : role_jsons) {
      memgraph::slk::Save(rj, builder);
    }
    static std::unordered_map<std::string, std::unordered_set<std::string>> const kEmptyMtMap;
    memgraph::slk::Save(self.user_mt_mappings ? *self.user_mt_mappings : kEmptyMtMap, builder);
  }
  bool const has_role = self.role_json.has_value();
  memgraph::slk::Save(has_role, builder);
  if (has_role) {
    memgraph::slk::Save(*self.role_json, builder);
  }
  memgraph::slk::Save(self.profile, builder);
}

void Load(memgraph::replication::UpdateAuthDataReqV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);

  // Read optional<User> in raw form: same wire format as slk::Load<optional<User>>
  bool has_user = false;
  memgraph::slk::Load(&has_user, reader);
  if (has_user) {
    // User JSON string
    std::string user_json;
    memgraph::slk::Load(&user_json, reader);
    self->user_json = std::move(user_json);

    // Roles vector: slk encodes as uint64_t size followed by each element
    uint64_t num_roles = 0;
    memgraph::slk::Load(&num_roles, reader);
    std::vector<std::string> role_jsons;
    role_jsons.reserve(num_roles);
    for (uint64_t i = 0; i < num_roles; ++i) {
      role_jsons.push_back(LoadJsonStringRaw(reader));
    }
    self->user_role_jsons = std::move(role_jsons);

    std::unordered_map<std::string, std::unordered_set<std::string>> mt_map;
    memgraph::slk::Load(&mt_map, reader);
    self->user_mt_mappings = std::move(mt_map);
  }

  // Read optional<Role> in raw form
  bool has_role = false;
  memgraph::slk::Load(&has_role, reader);
  if (has_role) {
    self->role_json = LoadJsonStringRaw(reader);
  }

  // Profile (unchanged format)
  memgraph::slk::Load(&self->profile, reader);
}

void Save(const memgraph::replication::UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.user, builder);
  memgraph::slk::Save(self.role, builder);
  memgraph::slk::Save(self.profile, builder);
}

// Migrating helpers are used instead of slk::Load<User/Role> so that a future
// entity version (e.g. V5) is automatically migrated on reception without
// requiring a new RPC version. Currently, as Auth RPC is version 2, we know
// this corresponds to auth entity version 4 and so the actual upgrade is a
// no-op.
void Load(memgraph::replication::UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);

  // User: same wire format as optional<User>, but with migration
  bool has_user = false;
  memgraph::slk::Load(&has_user, reader);
  if (has_user) {
    self->user = LoadAndMigrateUser(reader);
  }

  // Role: same wire format as optional<Role>, but with migration
  bool has_role = false;
  memgraph::slk::Load(&has_role, reader);
  if (has_role) {
    self->role = LoadAndMigrateRole(reader);
  }

  memgraph::slk::Load(&self->profile, reader);
}

void Save(const memgraph::replication::UpdateAuthDataResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::replication::UpdateAuthDataResV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::replication::UpdateAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::replication::UpdateAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for DropAuthDataReq
void Save(const memgraph::replication::DropAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.type, builder);
  memgraph::slk::Save(self.name, builder);
}

void Load(memgraph::replication::DropAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
  uint8_t type_tmp = 0;
  memgraph::slk::Load(&type_tmp, reader);
  if (!utils::NumToEnum(type_tmp, self->type)) {
    throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
  memgraph::slk::Load(&self->name, reader);
}

// Serialize code for DropAuthDataRes
void Save(const memgraph::replication::DropAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::replication::DropAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

}  // namespace memgraph::slk

namespace memgraph::replication {

UpdateAuthDataReq UpdateAuthDataReq::Upgrade(UpdateAuthDataReqV1 const &v1) {
  UpdateAuthDataReq req;
  req.main_uuid = v1.main_uuid;
  req.expected_group_timestamp = v1.expected_group_timestamp;
  req.new_group_timestamp = v1.new_group_timestamp;

  if (v1.user_json) {
    auto user_json = nlohmann::json::parse(*v1.user_json);
    auth::MigrateAuthJson(user_json);
    auto user = auth::User::Deserialize(user_json);

    std::vector<auth::Role> roles;
    if (v1.user_role_jsons) {
      roles.reserve(v1.user_role_jsons->size());
      for (auto const &rj : *v1.user_role_jsons) {
        auto rj_json = nlohmann::json::parse(rj);
        auth::MigrateAuthJson(rj_json);
        roles.push_back(auth::Role::Deserialize(rj_json));
      }
    }

    AttachRolesToUser(user, roles, v1.user_mt_mappings.value_or({}));
    req.user = std::move(user);
  }

  if (v1.role_json) {
    auto rj = nlohmann::json::parse(*v1.role_json);
    auth::MigrateAuthJson(rj);
    req.role = auth::Role::Deserialize(rj);
  }

  req.profile = v1.profile;
  return req;
}

void UpdateAuthDataReqV1::Save(const UpdateAuthDataReqV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UpdateAuthDataReqV1::Load(UpdateAuthDataReqV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UpdateAuthDataReq::Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UpdateAuthDataReq::Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UpdateAuthDataResV1::Save(const UpdateAuthDataResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UpdateAuthDataResV1::Load(UpdateAuthDataResV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UpdateAuthDataRes::Save(const UpdateAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UpdateAuthDataRes::Load(UpdateAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DropAuthDataReq::Save(const DropAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DropAuthDataReq::Load(DropAuthDataReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void DropAuthDataRes::Save(const DropAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DropAuthDataRes::Load(DropAuthDataRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

}  // namespace memgraph::replication
