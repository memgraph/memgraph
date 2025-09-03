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

#include "rpc_messages.hpp"

namespace memgraph::slk {
void Save(const SumReqV1 &sum, Builder *builder) {
  Save(sum.x, builder);
  Save(sum.y, builder);
}

void Load(SumReqV1 *sum, Reader *reader) {
  Load(&sum->x, reader);
  Load(&sum->y, reader);
}

void Save(const SumReq &sum, Builder *builder) { Save(sum.nums_, builder); }
void Load(SumReq *sum, Reader *reader) { Load(&sum->nums_, reader); }

void Save(const SumRes &res, Builder *builder) { Save(res.sum, builder); }
void Load(SumRes *res, Reader *reader) { Load(&res->sum, reader); }

void Save(const SumResV1 &res, Builder *builder) { Save(res.sum, builder); }
void Load(SumResV1 *res, Reader *reader) { Load(&res->sum, reader); }

void Save(const EchoMessage &echo, Builder *builder) { Save(echo.data, builder); }
void Load(EchoMessage *echo, Reader *reader) { Load(&echo->data, reader); }
}  // namespace memgraph::slk

void SumReq::Load(SumReq *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReq::Save(const SumReq &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumReqV1::Load(SumReqV1 *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReqV1::Save(const SumReqV1 &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumRes::Load(SumRes *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumRes::Save(const SumRes &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumResV1::Load(SumResV1 *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumResV1::Save(const SumResV1 &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void EchoMessage::Load(EchoMessage *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void EchoMessage::Save(const EchoMessage &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }
