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

#pragma once

#include <optional>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "rpc/messages.hpp"

namespace memgraph::replication {

struct UpdateAuthDataReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_REQ, .name = "UpdateAuthDataReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder);
  UpdateAuthDataReq() = default;
  UpdateAuthDataReq(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, auth::User user)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        user{std::move(user)} {}
  UpdateAuthDataReq(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, auth::Role role)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        role{std::move(role)} {}
  UpdateAuthDataReq(const utils::UUID &main_uuid, uint64_t expected_ts, uint64_t new_ts,
                    auth::UserProfiles::Profile profile)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        profile{std::move(profile)} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  std::optional<auth::User> user;
  std::optional<auth::Role> role;
  std::optional<auth::UserProfiles::Profile> profile{};
};

struct UpdateAuthDataRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_RES, .name = "UpdateAuthDataRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
  UpdateAuthDataRes() = default;
  explicit UpdateAuthDataRes(bool success) : success{success} {}

  bool success;
};

using UpdateAuthDataRpc = rpc::RequestResponse<UpdateAuthDataReq, UpdateAuthDataRes>;

struct DropAuthDataReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DROP_AUTH_DATA_REQ, .name = "DropAuthDataReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(DropAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataReq &self, memgraph::slk::Builder *builder);
  DropAuthDataReq() = default;

  enum class DataType : uint8_t { USER, ROLE, PROFILE };

  DropAuthDataReq(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, DataType const type,
                  std::string_view const name)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        type{type},
        name{name} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  DataType type;
  std::string name;
};

struct DropAuthDataRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DROP_AUTH_DATA_RES, .name = "DropAuthDataRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(DropAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataRes &self, memgraph::slk::Builder *builder);
  DropAuthDataRes() = default;
  explicit DropAuthDataRes(bool const success) : success{success} {}

  bool success;
};

using DropAuthDataRpc = rpc::RequestResponse<DropAuthDataReq, DropAuthDataRes>;

}  // namespace memgraph::replication

namespace memgraph::slk {

void Save(const auth::Role &self, memgraph::slk::Builder *builder);
void Load(auth::Role *self, memgraph::slk::Reader *reader);
void Save(const auth::User &self, memgraph::slk::Builder *builder);
void Load(auth::User *self, memgraph::slk::Reader *reader);
void Save(const auth::UserProfiles::Profile &self, memgraph::slk::Builder *builder);
void Load(auth::UserProfiles::Profile *self, memgraph::slk::Reader *reader);
void Save(const auth::Auth::Config &self, memgraph::slk::Builder *builder);
void Load(auth::Auth::Config *self, memgraph::slk::Reader *reader);

void Save(const memgraph::replication::UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::UpdateAuthDataReq * /*self*/, memgraph::slk::Reader * /*reader*/);
void Save(const memgraph::replication::DropAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::DropAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::DropAuthDataReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::DropAuthDataReq * /*self*/, memgraph::slk::Reader * /*reader*/);
}  // namespace memgraph::slk
