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

#include "parameters/rpc.hpp"

#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::storage::replication {

void SetParameterReq::Save(const SetParameterReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SetParameterReq::Load(SetParameterReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void SetParameterRes::Save(const SetParameterRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SetParameterRes::Load(SetParameterRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void UnsetParameterReq::Save(const UnsetParameterReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UnsetParameterReq::Load(UnsetParameterReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UnsetParameterRes::Save(const UnsetParameterRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UnsetParameterRes::Load(UnsetParameterRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DeleteAllParametersReq::Save(const DeleteAllParametersReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DeleteAllParametersReq::Load(DeleteAllParametersReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DeleteAllParametersRes::Save(const DeleteAllParametersRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DeleteAllParametersRes::Load(DeleteAllParametersRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

}  // namespace memgraph::storage::replication

namespace memgraph::slk {

void Save(const memgraph::storage::replication::SetParameterReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.parameter.name, builder);
  memgraph::slk::Save(self.parameter.value, builder);
  memgraph::slk::Save(self.parameter.scope_context, builder);
}

void Load(memgraph::storage::replication::SetParameterReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
  memgraph::slk::Load(&self->parameter.name, reader);
  memgraph::slk::Load(&self->parameter.value, reader);
  memgraph::slk::Load(&self->parameter.scope_context, reader);
}

void Save(const memgraph::storage::replication::SetParameterRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::storage::replication::SetParameterRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::storage::replication::UnsetParameterReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.name, builder);
  memgraph::slk::Save(self.scope_context, builder);
}

void Load(memgraph::storage::replication::UnsetParameterReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
  memgraph::slk::Load(&self->name, reader);
  memgraph::slk::Load(&self->scope_context, reader);
}

void Save(const memgraph::storage::replication::UnsetParameterRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::storage::replication::UnsetParameterRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::storage::replication::DeleteAllParametersReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
}

void Load(memgraph::storage::replication::DeleteAllParametersReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
}

void Save(const memgraph::storage::replication::DeleteAllParametersRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::storage::replication::DeleteAllParametersRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

}  // namespace memgraph::slk
