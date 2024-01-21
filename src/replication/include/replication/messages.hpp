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

#include <cstdint>
#include "auth/models.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"

namespace memgraph::replication {
struct SystemHeartbeatReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SystemHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const SystemHeartbeatReq &self, memgraph::slk::Builder *builder);
  SystemHeartbeatReq() = default;
};

struct SystemHeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SystemHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const SystemHeartbeatRes &self, memgraph::slk::Builder *builder);
  SystemHeartbeatRes() = default;
  explicit SystemHeartbeatRes(uint64_t system_timestamp) : system_timestamp(system_timestamp) {}

  uint64_t system_timestamp;
};

using SystemHeartbeatRpc = rpc::RequestResponse<SystemHeartbeatReq, SystemHeartbeatRes>;

struct UpdateAuthDataReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder);
  UpdateAuthDataReq() = default;
  UpdateAuthDataReq(std::string epoch_id, uint64_t expected_ts, uint64_t new_ts, auth::User user)
      : epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        user{std::move(user)} {}
  UpdateAuthDataReq(std::string epoch_id, uint64_t expected_ts, uint64_t new_ts, auth::Role role)
      : epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        role{std::move(role)} {}

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

  DropAuthDataReq(std::string epoch_id, uint64_t expected_ts, uint64_t new_ts, DataType type, std::string_view name)
      : epoch_id{std::move(epoch_id)},
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        type{type},
        name{name} {}

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

/**
 * @brief
 *
 * @param req_reader
 * @param res_builder
 */
void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);

}  // namespace memgraph::replication

namespace memgraph::slk {
void Save(const memgraph::replication::SystemHeartbeatRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::SystemHeartbeatRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::SystemHeartbeatReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::SystemHeartbeatReq * /*self*/, memgraph::slk::Reader * /*reader*/);
void Save(const memgraph::replication::UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::UpdateAuthDataReq * /*self*/, memgraph::slk::Reader * /*reader*/);
void Save(const memgraph::replication::DropAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::DropAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::DropAuthDataReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::DropAuthDataReq * /*self*/, memgraph::slk::Reader * /*reader*/);
}  // namespace memgraph::slk
