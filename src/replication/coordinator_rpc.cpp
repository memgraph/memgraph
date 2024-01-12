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

#include "replication/coordinator_rpc.hpp"
#include "replication/coordinator_slk.hpp"
#include "slk/serialization.hpp"

#ifdef MG_ENTERPRISE

namespace memgraph {

namespace replication {

void FailoverReq::Save(const FailoverReq &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }

void FailoverReq::Load(FailoverReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void FailoverRes::Save(const FailoverRes &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }

void FailoverRes::Load(FailoverRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

}  // namespace replication

constexpr utils::TypeInfo replication::FailoverReq::kType{utils::TypeId::COORD_FAILOVER_REQ, "CoordFailoverReq",
                                                          nullptr};

constexpr utils::TypeInfo replication::FailoverRes::kType{utils::TypeId::COORD_FAILOVER_RES, "CoordFailoverRes",
                                                          nullptr};

namespace slk {

// Serialize code for FailoverRes

void Save(const memgraph::replication::FailoverRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::replication::FailoverRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for FailoverReq

void Save(const memgraph::replication::FailoverReq &self, memgraph::slk::Builder *builder) {
  std::function<void(const ReplicationClientInfo &, Builder *)> item_save_function =
      [](const auto &item, auto *builder) -> void { memgraph::slk::Save(item, builder); };
  memgraph::slk::Save(self.replication_clients_info, builder, item_save_function);
}

void Load(memgraph::replication::FailoverReq *self, memgraph::slk::Reader *reader) {
  std::function<void(ReplicationClientInfo *, Reader *)> item_load_function = [](auto *item, auto *reader) -> void {
    memgraph::slk::Load(item, reader);
  };
  memgraph::slk::Load(&self->replication_clients_info, reader, item_load_function);
}

}  // namespace slk

}  // namespace memgraph

#endif
