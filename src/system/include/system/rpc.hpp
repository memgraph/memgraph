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

#include "rpc/messages.hpp"
#include "slk/streams.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

namespace memgraph::replication {

struct FinalizeSystemTxReq {
  static constexpr utils::TypeInfo kType{
      .id = utils::TypeId::REP_FINALIZE_SYS_TX_REQ, .name = "FinalizeSystemTxReq", .superclass = nullptr};
  static constexpr uint64_t kVersion{1};
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FinalizeSystemTxReq *self, memgraph::slk::Reader *reader);
  static void Save(const FinalizeSystemTxReq &self, memgraph::slk::Builder *builder);
  FinalizeSystemTxReq() = default;
  FinalizeSystemTxReq(const utils::UUID &main_uuid, uint64_t expected_ts, uint64_t new_ts)
      : main_uuid(main_uuid), expected_group_timestamp{expected_ts}, new_group_timestamp{new_ts} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
};

struct FinalizeSystemTxRes {
  static constexpr utils::TypeInfo kType{
      .id = utils::TypeId::REP_FINALIZE_SYS_TX_RES, .name = "FinalizeSystemTxRes", .superclass = nullptr};
  static constexpr uint64_t kVersion{1};

  static void Load(FinalizeSystemTxRes *self, memgraph::slk::Reader *reader);
  static void Save(const FinalizeSystemTxRes &self, memgraph::slk::Builder *builder);
  FinalizeSystemTxRes() = default;
  explicit FinalizeSystemTxRes(bool const success) : success{success} {}

  bool success;
};

using FinalizeSystemTxRpc = rpc::RequestResponse<FinalizeSystemTxReq, FinalizeSystemTxRes>;

}  // namespace memgraph::replication

namespace memgraph::slk {
void Save(const memgraph::replication::FinalizeSystemTxRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::FinalizeSystemTxRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::FinalizeSystemTxReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::FinalizeSystemTxReq * /*self*/, memgraph::slk::Reader * /*reader*/);
}  // namespace memgraph::slk
