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
#include "slk/serialization.hpp"
#include "utils/uuid.hpp"

namespace memgraph::replication_coordination_glue {

struct FrequentHeartbeatReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ,
                                         .name = "FrequentHeartbeatReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReq() = default;
};

struct FrequentHeartbeatRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_RES,
                                         .name = "FrequentHeartbeatRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatRes() = default;
};

using FrequentHeartbeatRpc = rpc::RequestResponse<FrequentHeartbeatReq, FrequentHeartbeatRes>;

struct SwapMainUUIDReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SWAP_UUID_REQ, .name = "SwapMainUUIDReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(SwapMainUUIDReq *self, memgraph::slk::Reader *reader);
  static void Save(const SwapMainUUIDReq &self, memgraph::slk::Builder *builder);

  explicit SwapMainUUIDReq(const utils::UUID &uuid) : uuid(uuid) {}

  SwapMainUUIDReq() = default;

  utils::UUID uuid;
};

struct SwapMainUUIDRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SWAP_UUID_RES, .name = "SwapMainUUIDRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(SwapMainUUIDRes *self, memgraph::slk::Reader *reader);
  static void Save(const SwapMainUUIDRes &self, memgraph::slk::Builder *builder);

  explicit SwapMainUUIDRes(bool success) : success(success) {}
  SwapMainUUIDRes() = default;

  bool success;
};

using SwapMainUUIDRpc = rpc::RequestResponse<SwapMainUUIDReq, SwapMainUUIDRes>;

}  // namespace memgraph::replication_coordination_glue

namespace memgraph::slk {
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatReq & /*self*/,
          memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatReq * /*self*/, memgraph::slk::Reader * /*reader*/);

// SwapMainUUIDRpc
void Save(const memgraph::replication_coordination_glue::SwapMainUUIDReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::SwapMainUUIDReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::SwapMainUUIDRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::SwapMainUUIDRes *self, memgraph::slk::Reader *reader);
}  // namespace memgraph::slk
