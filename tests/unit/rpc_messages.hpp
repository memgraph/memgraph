// Copyright 2021 Memgraph Ltd.
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
  static const utils::TypeInfo kType;

  SumReq() {}  // Needed for serialization.
  SumReq(int x, int y) : x(x), y(y) {}

  static void Load(SumReq *obj, slk::Reader *reader);
  static void Save(const SumReq &obj, slk::Builder *builder);

  int x;
  int y;
};

const utils::TypeInfo SumReq::kType{0, "SumReq"};

struct SumRes {
  static const utils::TypeInfo kType;

  SumRes() {}  // Needed for serialization.
  SumRes(int sum) : sum(sum) {}

  static void Load(SumRes *obj, slk::Reader *reader);
  static void Save(const SumRes &obj, slk::Builder *builder);

  int sum;
};

const utils::TypeInfo SumRes::kType{1, "SumRes"};

namespace slk {
void Save(const SumReq &sum, Builder *builder);
void Load(SumReq *sum, Reader *reader);

void Save(const SumRes &res, Builder *builder);
void Load(SumRes *res, Reader *reader);
}  // namespace slk

using Sum = rpc::RequestResponse<SumReq, SumRes>;

struct EchoMessage {
  static const utils::TypeInfo kType;

  EchoMessage() {}  // Needed for serialization.
  EchoMessage(const std::string &data) : data(data) {}

  static void Load(EchoMessage *obj, slk::Reader *reader);
  static void Save(const EchoMessage &obj, slk::Builder *builder);

  std::string data;
};

const utils::TypeInfo EchoMessage::kType{2, "EchoMessage"};

namespace slk {
void Save(const EchoMessage &echo, Builder *builder);
void Load(EchoMessage *echo, Reader *reader);
}  // namespace slk

using Echo = rpc::RequestResponse<EchoMessage, EchoMessage>;
