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

#pragma once

#include "parameters/parameters.hpp"
#include "rpc/messages.hpp"
#include "slk/streams.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::replication {

struct SetParameterReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SET_PARAMETER_REQ, .name = "SetParameterReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(SetParameterReq *self, memgraph::slk::Reader *reader);
  static void Save(const SetParameterReq &self, memgraph::slk::Builder *builder);
  SetParameterReq() = default;

  SetParameterReq(const utils::UUID &main_uuid, uint64_t expected_group_timestamp, uint64_t new_group_timestamp,
                  parameters::ParameterInfo parameter)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp{new_group_timestamp},
        parameter(std::move(parameter)) {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  parameters::ParameterInfo parameter;
};

struct SetParameterRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SET_PARAMETER_RES, .name = "SetParameterRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(SetParameterRes *self, memgraph::slk::Reader *reader);
  static void Save(const SetParameterRes &self, memgraph::slk::Builder *builder);
  SetParameterRes() = default;

  explicit SetParameterRes(bool success) : success(success) {}

  bool success;
};

using SetParameterRpc = rpc::RequestResponse<SetParameterReq, SetParameterRes>;

struct UnsetParameterReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UNSET_PARAMETER_REQ, .name = "UnsetParameterReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnsetParameterReq *self, memgraph::slk::Reader *reader);
  static void Save(const UnsetParameterReq &self, memgraph::slk::Builder *builder);
  UnsetParameterReq() = default;

  UnsetParameterReq(const utils::UUID &main_uuid, uint64_t expected_group_timestamp, uint64_t new_group_timestamp,
                    std::string name, parameters::ParameterScope scope)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp{new_group_timestamp},
        name(std::move(name)),
        scope(scope) {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  std::string name;
  parameters::ParameterScope scope;
};

struct UnsetParameterRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UNSET_PARAMETER_RES, .name = "UnsetParameterRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnsetParameterRes *self, memgraph::slk::Reader *reader);
  static void Save(const UnsetParameterRes &self, memgraph::slk::Builder *builder);
  UnsetParameterRes() = default;

  explicit UnsetParameterRes(bool success) : success(success) {}

  bool success;
};

using UnsetParameterRpc = rpc::RequestResponse<UnsetParameterReq, UnsetParameterRes>;

struct DeleteAllParametersReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DELETE_ALL_PARAMETERS_REQ,
                                         .name = "DeleteAllParametersReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(DeleteAllParametersReq *self, memgraph::slk::Reader *reader);
  static void Save(const DeleteAllParametersReq &self, memgraph::slk::Builder *builder);
  DeleteAllParametersReq() = default;

  DeleteAllParametersReq(const utils::UUID &main_uuid, uint64_t expected_group_timestamp, uint64_t new_group_timestamp)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp{new_group_timestamp} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
};

struct DeleteAllParametersRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DELETE_ALL_PARAMETERS_RES,
                                         .name = "DeleteAllParametersRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(DeleteAllParametersRes *self, memgraph::slk::Reader *reader);
  static void Save(const DeleteAllParametersRes &self, memgraph::slk::Builder *builder);
  DeleteAllParametersRes() = default;

  explicit DeleteAllParametersRes(bool success) : success(success) {}

  bool success;
};

using DeleteAllParametersRpc = rpc::RequestResponse<DeleteAllParametersReq, DeleteAllParametersRes>;

}  // namespace memgraph::storage::replication

// SLK serialization declarations
namespace memgraph::slk {

void Save(const memgraph::storage::replication::SetParameterReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::SetParameterReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::storage::replication::SetParameterRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::SetParameterRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::storage::replication::UnsetParameterReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::UnsetParameterReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::storage::replication::UnsetParameterRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::UnsetParameterRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::storage::replication::DeleteAllParametersReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::DeleteAllParametersReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::storage::replication::DeleteAllParametersRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::storage::replication::DeleteAllParametersRes *self, memgraph::slk::Reader *reader);
}  // namespace memgraph::slk
