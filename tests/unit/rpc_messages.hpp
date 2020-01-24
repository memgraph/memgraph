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
