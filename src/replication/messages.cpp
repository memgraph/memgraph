// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "replication/messages.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::slk {
// Serialize code for FrequentHeartbeatRes
void Save(const memgraph::replication::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}
void Load(memgraph::replication::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for FrequentHeartbeatReq
void Save(const memgraph::replication::FrequentHeartbeatReq & /*self*/, memgraph::slk::Builder * /*builder*/) {
  /* Nothing to serialize */
}
void Load(memgraph::replication::FrequentHeartbeatReq * /*self*/, memgraph::slk::Reader * /*reader*/) {
  /* Nothing to serialize */
}

}  // namespace memgraph::slk

namespace memgraph::replication {

constexpr utils::TypeInfo FrequentHeartbeatReq::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_REQ, "FrequentHeartbeatReq",
                                                      nullptr};

constexpr utils::TypeInfo FrequentHeartbeatRes::kType{utils::TypeId::REP_FREQUENT_HEARTBEAT_RES, "FrequentHeartbeatRes",
                                                      nullptr};

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

void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  FrequentHeartbeatReq req;
  FrequentHeartbeatReq::Load(&req, req_reader);
  memgraph::slk::Load(&req, req_reader);
  FrequentHeartbeatRes res{true};
  memgraph::slk::Save(res, res_builder);
}

}  // namespace memgraph::replication
