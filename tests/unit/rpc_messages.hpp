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

#pragma once

#include <utility>

#include "rpc/messages.hpp"
#include "utils/typeinfo.hpp"

struct SumReqV1 {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumReq"};
  static constexpr uint64_t kVersion{1};

  SumReqV1() = default;  // Needed for serialization.
  SumReqV1(int x, int y) : x(x), y(y) {}

  static void Load(SumReqV1 *obj, memgraph::slk::Reader *reader);
  static void Save(const SumReqV1 &obj, memgraph::slk::Builder *builder);

  int x;
  int y;
};
// v2 request accepts the vector of numbers
// v2 response return the sum as a vector with the single-element (some random reason)
struct SumReq {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumReq"};
  static constexpr uint64_t kVersion{2};

  SumReq() = default;  // Needed for serialization.
  explicit SumReq(std::initializer_list<int> const nums) : nums_(nums) {}

  static void Load(SumReq *obj, memgraph::slk::Reader *reader);
  static void Save(const SumReq &obj, memgraph::slk::Builder *builder);

  static SumReq Upgrade(SumReqV1 const &prev) { return SumReq{{prev.x, prev.y}}; }

  std::vector<int> nums_;
};

struct SumResV1 {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumRes"};
  static constexpr uint64_t kVersion{1};

  SumResV1() = default;  // Needed for serialization.
  explicit SumResV1(int const sum) : sum(sum) {}

  static void Load(SumResV1 *obj, memgraph::slk::Reader *reader);
  static void Save(const SumResV1 &obj, memgraph::slk::Builder *builder);

  int sum;
};

struct SumRes {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumRes"};
  static constexpr uint64_t kVersion{2};

  SumRes() = default;  // Needed for serialization.
  explicit SumRes(std::initializer_list<int> const sum) : sum(sum) {}

  static void Load(SumRes *obj, memgraph::slk::Reader *reader);
  static void Save(const SumRes &obj, memgraph::slk::Builder *builder);

  SumResV1 Downgrade() const { return SumResV1{sum[0]}; }

  std::vector<int> sum;
};

namespace memgraph::slk {
void Save(const SumReqV1 &sum, Builder *builder);
void Load(SumReqV1 *sum, Reader *reader);
void Save(const SumReq &sum, Builder *builder);
void Load(SumReq *sum, Reader *reader);

void Save(const SumRes &res, Builder *builder);
void Load(SumRes *res, Reader *reader);
void Save(const SumResV1 &res, Builder *builder);
void Load(SumResV1 *res, Reader *reader);
}  // namespace memgraph::slk

using Sum = memgraph::rpc::RequestResponse<SumReq, SumRes>;
using SumV1 = memgraph::rpc::RequestResponse<SumReqV1, SumResV1>;

struct EchoMessage {
  // Intentionally set to a random value to avoid polluting typeinfo.hpp
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::COORD_UNREGISTER_REPLICA_REQ,
                                                   "EchoMessage"};
  static constexpr uint64_t kVersion{1};

  EchoMessage() = default;  // Needed for serialization.
  explicit EchoMessage(std::string data) : data(std::move(data)) {}

  static void Load(EchoMessage *obj, memgraph::slk::Reader *reader);
  static void Save(const EchoMessage &obj, memgraph::slk::Builder *builder);

  std::string data;
};

namespace memgraph::slk {
void Save(const EchoMessage &echo, Builder *builder);
void Load(EchoMessage *echo, Reader *reader);
}  // namespace memgraph::slk

using Echo = memgraph::rpc::RequestResponse<EchoMessage, EchoMessage>;
