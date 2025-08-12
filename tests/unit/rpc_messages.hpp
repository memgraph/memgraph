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
#include "slk/serialization.hpp"
#include "utils/typeinfo.hpp"

struct SumReqV2 {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumReq"};
  static constexpr uint64_t kVersion{2};

  SumReqV2() = default;  // Needed for serialization.
  explicit SumReqV2(std::initializer_list<int> const nums) : nums_(nums) {}

  static void Load(SumReqV2 *obj, memgraph::slk::Reader *reader);
  static void Save(const SumReqV2 &obj, memgraph::slk::Builder *builder);

  std::vector<int> nums_;
};

struct SumReq {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumReq"};
  static constexpr uint64_t kVersion{1};

  SumReq() = default;  // Needed for serialization.
  SumReq(int x, int y) : x(x), y(y) {}

  static void Load(SumReq *obj, memgraph::slk::Reader *reader);
  static void Save(const SumReq &obj, memgraph::slk::Builder *builder);

  SumReqV2 Upgrade() const { return SumReqV2({x, y}); }

  int x;
  int y;
};

using SumReqTypes = std::variant<SumReqV1, SumReq>;

struct SumRes {
  static constexpr memgraph::utils::TypeInfo kType{.id = memgraph::utils::TypeId::UNKNOWN, .name = "SumRes"};
  static constexpr uint64_t kVersion{1};

  SumRes() = default;  // Needed for serialization.
  explicit SumRes(int sum) : sum(sum) {}

  static void Load(SumRes *obj, memgraph::slk::Reader *reader);
  static void Save(const SumRes &obj, memgraph::slk::Builder *builder);

  int sum;
};

// If new version is exactly the same as old version
struct SumResV2 : SumRes {
  static constexpr uint64_t kVersion{2};
};

using SumResTypes = std::variant<SumRes, SumResV2>;

namespace memgraph::slk {
void Save(const SumReq &sum, Builder *builder);
void Load(SumReq *sum, Reader *reader);
void Save(const SumReqV2 &sum, Builder *builder);
void Load(SumReqV2 *sum, Reader *reader);

void Save(const SumRes &res, Builder *builder);
void Load(SumRes *res, Reader *reader);
}  // namespace memgraph::slk

using Sum = memgraph::rpc::RequestResponse<SumReq, SumRes>;
using SumV2 = memgraph::rpc::RequestResponse<SumReqV2, SumResV2>;

using Func = std::function<SumReqTypes(memgraph::slk::Reader *reader)>;
inline static const std::unordered_map<uint64_t, Func> sum_req_factory{
    {SumReq::kVersion,
     [](memgraph::slk::Reader *req_reader) {
       auto local_req = SumReq{};
       Load(&local_req, req_reader);
       return local_req;
     }},
    {SumReqV2::kVersion, [](memgraph::slk::Reader *req_reader) {
       auto local_req = SumReqV2{};
       Load(&local_req, req_reader);
       return local_req;
     }}};

auto SumReqFactory(uint64_t const request_version, memgraph::slk::Reader *req_reader) -> SumReqTypes {
  if (request_version == SumReq::kVersion) {
    auto local_req = SumReq{};
    Load(&local_req, req_reader);
    return local_req;
  }
  if (request_version == SumReqV2::kVersion) {
    auto local_req = SumReqV2{};
    Load(&local_req, req_reader);
    return local_req;
  }
  LOG_FATAL("Unknown sum req type");
}

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
