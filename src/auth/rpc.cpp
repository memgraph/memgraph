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
      // Role already set as multi-tenant role
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
  try {
    auto json = nlohmann::json::parse(tmp);
    memgraph::auth::MigrateAuthJson(json);
    return memgraph::auth::Role::Deserialize(json);
  } catch (nlohmann::json::exception const &e) {
    throw auth::AuthException("Failed to deserialize role from RPC: {}", e.what());
  }
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
  try {
    auto json = nlohmann::json::parse(tmp);
    memgraph::auth::MigrateAuthJson(json);
    *self = memgraph::auth::User::Deserialize(json);
  } catch (nlohmann::json::exception const &e) {
    throw auth::AuthException("Failed to deserialize user from RPC: {}", e.what());
  }
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

void Save(const memgraph::replication::UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.user, builder);
  memgraph::slk::Save(self.role, builder);
  memgraph::slk::Save(self.profile, builder);
}

void Load(memgraph::replication::UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
  memgraph::slk::Load(&self->user, reader);
  memgraph::slk::Load(&self->role, reader);
  memgraph::slk::Load(&self->profile, reader);
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

void UpdateAuthDataReq::Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UpdateAuthDataReq::Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
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
