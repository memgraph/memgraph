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
#include <string>
#include <utility>

#include "rpc/messages.hpp"
#include "slk/streams.hpp"
#include "storage/v2/config.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::replication {

struct CreateDatabaseReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CreateDatabaseReq *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseReq &self, memgraph::slk::Builder *builder);
  CreateDatabaseReq() = default;
  CreateDatabaseReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_group_timestamp,
                    uint64_t new_group_timestamp, storage::SalientConfig config)
      : main_uuid(main_uuid),
        epoch_id(std::move(epoch_id)),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp(new_group_timestamp),
        config(std::move(config)) {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  storage::SalientConfig config;
};

struct CreateDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(CreateDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseRes &self, memgraph::slk::Builder *builder);
  CreateDatabaseRes() = default;
  explicit CreateDatabaseRes(Result res) : result(res) {}

  Result result;
};

using CreateDatabaseRpc = rpc::RequestResponse<CreateDatabaseReq, CreateDatabaseRes>;

struct DropDatabaseReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DropDatabaseReq *self, memgraph::slk::Reader *reader);
  static void Save(const DropDatabaseReq &self, memgraph::slk::Builder *builder);
  DropDatabaseReq() = default;
  DropDatabaseReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_group_timestamp,
                  uint64_t new_group_timestamp, const utils::UUID &uuid)
      : main_uuid(main_uuid),
        epoch_id(std::move(epoch_id)),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp(new_group_timestamp),
        uuid(uuid) {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  utils::UUID uuid;
};

struct DropDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(DropDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const DropDatabaseRes &self, memgraph::slk::Builder *builder);
  DropDatabaseRes() = default;
  explicit DropDatabaseRes(Result res) : result(res) {}

  Result result;
};

using DropDatabaseRpc = rpc::RequestResponse<DropDatabaseReq, DropDatabaseRes>;

}  // namespace memgraph::storage::replication

// SLK serialization declarations
namespace memgraph::slk {

void Save(const memgraph::storage::replication::CreateDatabaseReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CreateDatabaseReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::CreateDatabaseRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CreateDatabaseRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::DropDatabaseReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::DropDatabaseReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::DropDatabaseRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::DropDatabaseRes *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk
