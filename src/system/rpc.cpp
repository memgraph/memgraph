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

#include "system/rpc.hpp"

#include <nlohmann/json.hpp>
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::slk {
// Serialize code for FinalizeSystemTxReq
void Save(const memgraph::replication::FinalizeSystemTxReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
}
void Load(memgraph::replication::FinalizeSystemTxReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
}

// Serialize code for FinalizeSystemTxRes
void Save(const memgraph::replication::FinalizeSystemTxRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}
void Load(memgraph::replication::FinalizeSystemTxRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

}  // namespace memgraph::slk

namespace memgraph::replication {

constexpr utils::TypeInfo FinalizeSystemTxReq::kType{utils::TypeId::REP_FINALIZE_SYS_TX_REQ, "FinalizeSystemTxReq",
                                                     nullptr};

constexpr utils::TypeInfo FinalizeSystemTxRes::kType{utils::TypeId::REP_FINALIZE_SYS_TX_RES, "FinalizeSystemTxRes",
                                                     nullptr};

void FinalizeSystemTxReq::Save(const FinalizeSystemTxReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FinalizeSystemTxReq::Load(FinalizeSystemTxReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void FinalizeSystemTxRes::Save(const FinalizeSystemTxRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FinalizeSystemTxRes::Load(FinalizeSystemTxRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

}  // namespace memgraph::replication
