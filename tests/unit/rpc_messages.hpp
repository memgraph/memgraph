// Copyright 2022 Memgraph Ltd.
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
#include "utils/typeinfo.hpp"

struct SumReq {
  static const memgraph::utils::TypeInfo kType;

  SumReq() {}  // Needed for serialization.
  SumReq(int x, int y) : x(x), y(y) {}

  static void Load(SumReq *obj, memgraph::slk::Reader *reader);
  static void Save(const SumReq &obj, memgraph::slk::Builder *builder);

  int x;
  int y;
};

const memgraph::utils::TypeInfo SumReq::kType{0, "SumReq"};

struct SumRes {
  static const memgraph::utils::TypeInfo kType;

  SumRes() {}  // Needed for serialization.
  SumRes(int sum) : sum(sum) {}

  static void Load(SumRes *obj, memgraph::slk::Reader *reader);
  static void Save(const SumRes &obj, memgraph::slk::Builder *builder);

  int sum;
};

const memgraph::utils::TypeInfo SumRes::kType{1, "SumRes"};

namespace memgraph::slk {
void Save(const SumReq &sum, Builder *builder);
void Load(SumReq *sum, Reader *reader);

void Save(const SumRes &res, Builder *builder);
void Load(SumRes *res, Reader *reader);
}  // namespace memgraph::slk

using Sum = memgraph::rpc::RequestResponse<SumReq, SumRes>;

struct EchoMessage {
  static const memgraph::utils::TypeInfo kType;

  EchoMessage() {}  // Needed for serialization.
  EchoMessage(const std::string &data) : data(data) {}

  static void Load(EchoMessage *obj, memgraph::slk::Reader *reader);
  static void Save(const EchoMessage &obj, memgraph::slk::Builder *builder);

  std::string data;
};

const memgraph::utils::TypeInfo EchoMessage::kType{2, "EchoMessage"};

namespace memgraph::slk {
void Save(const EchoMessage &echo, Builder *builder);
void Load(EchoMessage *echo, Reader *reader);
}  // namespace memgraph::slk

using Echo = memgraph::rpc::RequestResponse<EchoMessage, EchoMessage>;
