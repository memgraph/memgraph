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

#include "replication_coordination_glue/messages.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::slk {
// Serialize code for FrequentHeartbeatRes
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {}
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {}

// Serialize code for FrequentHeartbeatReq
void Save(const memgraph::replication_coordination_glue::FrequentHeartbeatReq & /*self*/,
          memgraph::slk::Builder * /*builder*/) {
  /* Nothing to serialize */
}
void Load(memgraph::replication_coordination_glue::FrequentHeartbeatReq * /*self*/,
          memgraph::slk::Reader * /*reader*/) {
  /* Nothing to serialize */
}

// Serialize code for SwapMainUUIDRes

void Save(const memgraph::replication_coordination_glue::SwapMainUUIDRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::replication_coordination_glue::SwapMainUUIDRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for SwapMainUUIDReq
void Save(const memgraph::replication_coordination_glue::SwapMainUUIDReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.uuid, builder);
}

void Load(memgraph::replication_coordination_glue::SwapMainUUIDReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->uuid, reader);
}

}  // namespace memgraph::slk

namespace memgraph::replication_coordination_glue {

constexpr utils::TypeInfo FrequentHeartbeatReq::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ, "FrequentHeartbeatReq",
                                                      nullptr};

constexpr utils::TypeInfo FrequentHeartbeatRes::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_RES, "FrequentHeartbeatRes",
                                                      nullptr};

constexpr utils::TypeInfo SwapMainUUIDReq::kType{utils::TypeId::COORD_SWAP_UUID_REQ, "SwapUUIDReq", nullptr};

constexpr utils::TypeInfo SwapMainUUIDRes::kType{utils::TypeId::COORD_SWAP_UUID_RES, "SwapUUIDRes", nullptr};

void FrequentHeartbeatReq::Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FrequentHeartbeatReq::Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void FrequentHeartbeatRes::Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FrequentHeartbeatRes::Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SwapMainUUIDReq::Save(const SwapMainUUIDReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SwapMainUUIDReq::Load(SwapMainUUIDReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void SwapMainUUIDRes::Save(const SwapMainUUIDRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SwapMainUUIDRes::Load(SwapMainUUIDRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

}  // namespace memgraph::replication_coordination_glue
