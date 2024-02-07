// Copyright 2024 Memgraph Ltd.
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
#include "rpc/messages.hpp"
#include "slk/streams.hpp"

namespace memgraph::replication {

struct UpdateAuthDataReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder);
  UpdateAuthDataReq() = default;
  UpdateAuthDataReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_ts, uint64_t new_ts,
                    auth::User user)
      : main_uuid(main_uuid),
        epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        user{std::move(user)} {}
  UpdateAuthDataReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_ts, uint64_t new_ts,
                    auth::Role role)
      : main_uuid(main_uuid),
        epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        role{std::move(role)} {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  std::optional<auth::User> user;
  std::optional<auth::Role> role;
};

struct UpdateAuthDataRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
  UpdateAuthDataRes() = default;
  explicit UpdateAuthDataRes(bool success) : success{success} {}

  bool success;
};

using UpdateAuthDataRpc = rpc::RequestResponse<UpdateAuthDataReq, UpdateAuthDataRes>;

struct DropAuthDataReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DropAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataReq &self, memgraph::slk::Builder *builder);
  DropAuthDataReq() = default;

  enum class DataType { USER, ROLE };

  DropAuthDataReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_ts, uint64_t new_ts,
                  DataType type, std::string_view name)
      : main_uuid(main_uuid),
        epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        type{type},
        name{name} {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  DataType type;
  std::string name;
};

struct DropAuthDataRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DropAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataRes &self, memgraph::slk::Builder *builder);
  DropAuthDataRes() = default;
  explicit DropAuthDataRes(bool success) : success{success} {}

  bool success;
};

using DropAuthDataRpc = rpc::RequestResponse<DropAuthDataReq, DropAuthDataRes>;

}  // namespace memgraph::replication

namespace memgraph::slk {

void Save(const auth::Role &self, memgraph::slk::Builder *builder);
void Load(auth::Role *self, memgraph::slk::Reader *reader);
void Save(const auth::User &self, memgraph::slk::Builder *builder);
void Load(auth::User *self, memgraph::slk::Reader *reader);
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
