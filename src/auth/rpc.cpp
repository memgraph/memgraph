// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::slk {
// Serialize code for auth::Role
void Save(const auth::Role &self, Builder *builder) { memgraph::slk::Save(self.Serialize().dump(), builder); }

namespace {
auth::Role LoadAuthRole(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  auto role = memgraph::auth::Role::Deserialize(json);
  return role;
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
  // MT roles
  memgraph::slk::Save(self.GetMultiTenantRoleMappings(), builder);
#endif
}

// Deserialize code for auth::User
void Load(auth::User *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  *self = memgraph::auth::User::Deserialize(json);
  std::vector<auth::Role> roles;
  memgraph::slk::Load(&roles, reader);
  self->ClearAllRoles();

#ifdef MG_ENTERPRISE
  // Handle multi-tenant roles (has to be done before adding all roles)
  std::unordered_map<std::string, std::unordered_set<std::string>> db_role_map;
  memgraph::slk::Load(&db_role_map, reader);
  for (const auto &[db, role_names] : db_role_map) {
    for (const auto &role : role_names) {
      if (auto role_it =
              std::find_if(roles.begin(), roles.end(), [&role](const auto &r) { return r.rolename() == role; });
          role_it != roles.end()) {
        self->AddMultiTenantRole(*role_it, db);
      }
    }
  }
#endif

  // Handle global roles
  for (const auto &role : roles) {
    try {
      self->AddRole(role);
    } catch (const auth::AuthException &e) {
      // Absorb the exception and continue with the next role (role already set as multi-tenant role)
      continue;
    }
  }

#ifdef MG_ENTERPRISE
  // Profile management moved to UserProfiles class
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

// Serialize code for UpdateAuthDataReq
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

// Serialize code for UpdateAuthDataRes
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
