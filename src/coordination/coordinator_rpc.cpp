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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_rpc.hpp"

#include "coordination/coordinator_slk.hpp"
#include "slk/serialization.hpp"

namespace memgraph {

namespace coordination {

void PromoteReplicaToMainReq::Save(const PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteReplicaToMainReq::Load(PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void PromoteReplicaToMainRes::Save(const PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteReplicaToMainRes::Load(PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SetMainToReplicaReq::Save(const SetMainToReplicaReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SetMainToReplicaReq::Load(SetMainToReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SetMainToReplicaRes::Save(const SetMainToReplicaRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SetMainToReplicaRes::Load(SetMainToReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

}  // namespace coordination

constexpr utils::TypeInfo coordination::PromoteReplicaToMainReq::kType{utils::TypeId::COORD_FAILOVER_REQ,
                                                                       "CoordPromoteReplicaToMainReq", nullptr};

constexpr utils::TypeInfo coordination::PromoteReplicaToMainRes::kType{utils::TypeId::COORD_FAILOVER_RES,
                                                                       "CoordPromoteReplicaToMainRes", nullptr};

constexpr utils::TypeInfo coordination::SetMainToReplicaReq::kType{utils::TypeId::COORD_SET_REPL_MAIN_REQ,
                                                                   "CoordSetReplMainReq", nullptr};

constexpr utils::TypeInfo coordination::SetMainToReplicaRes::kType{utils::TypeId::COORD_SET_REPL_MAIN_RES,
                                                                   "CoordSetReplMainRes", nullptr};

namespace slk {

void Save(const memgraph::coordination::PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::coordination::PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.replication_clients_info, builder);
}

void Load(memgraph::coordination::PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->replication_clients_info, reader);
}

void Save(const memgraph::coordination::SetMainToReplicaReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.replication_client_info, builder);
}

void Load(memgraph::coordination::SetMainToReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->replication_client_info, reader);
}

void Save(const memgraph::coordination::SetMainToReplicaRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::SetMainToReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

}  // namespace slk

}  // namespace memgraph

#endif
