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

#include <cstdint>
#include <utility>
#include <vector>

#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "utils/uuid.hpp"

namespace memgraph::replication_coordination_glue {

struct FrequentHeartbeatReqV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ,
                                         .name = "FrequentHeartbeatReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(FrequentHeartbeatReqV1 *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReqV1 &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReqV1() = default;
};

/// Same content as V1. The server answers at the request's version, so V2 exists to ask for a V2 response.
struct FrequentHeartbeatReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ,
                                         .name = "FrequentHeartbeatReq"};
  static constexpr uint64_t kVersion{2};

  static void Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReq() = default;

  static FrequentHeartbeatReq Upgrade(FrequentHeartbeatReqV1 const & /*v1*/) { return {}; }
};

struct FrequentHeartbeatResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_RES,
                                         .name = "FrequentHeartbeatRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(FrequentHeartbeatResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatResV1 &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatResV1() = default;
};

/// (database uuid, last durable commit timestamp) for each database the replica holds.
using DatabaseCommitTimestamps = std::vector<std::pair<utils::UUID, uint64_t>>;

struct FrequentHeartbeatRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_FREQUENT_HEARTBEAT_RES,
                                         .name = "FrequentHeartbeatRes"};
  static constexpr uint64_t kVersion{2};

  static void Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatRes() = default;

  explicit FrequentHeartbeatRes(DatabaseCommitTimestamps database_commit_timestamps)
      : database_commit_timestamps{std::move(database_commit_timestamps)} {}

  FrequentHeartbeatResV1 Downgrade() const { return {}; }

  DatabaseCommitTimestamps database_commit_timestamps;
};

using FrequentHeartbeatRpc = rpc::RequestResponse<FrequentHeartbeatReq, FrequentHeartbeatRes>;
using FrequentHeartbeatRpcV1 = rpc::RequestResponse<FrequentHeartbeatReqV1, FrequentHeartbeatResV1>;

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
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatResV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatResV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatReqV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatReqV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatReq & /*self*/,
          memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatReq * /*self*/, memgraph::slk::Reader * /*reader*/);

// SwapMainUUIDRpc
void Save(const memgraph::replication_coordination_glue::SwapMainUUIDReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::SwapMainUUIDReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication_coordination_glue::SwapMainUUIDRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication_coordination_glue::SwapMainUUIDRes *self, memgraph::slk::Reader *reader);
}  // namespace memgraph::slk
