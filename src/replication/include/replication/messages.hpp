// Copyright 2023 Memgraph Ltd.
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

namespace memgraph::replication {

struct FrequentHeartbeatReq {
  static const utils::TypeInfo kType;                            // TODO: make constexpr?
  static const utils::TypeInfo &GetTypeInfo() { return kType; }  // WHAT?

  static void Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReq() {}
};

struct FrequentHeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatRes() {}
  explicit FrequentHeartbeatRes(bool success) : success(success) {}

  bool success;
};

using FrequentHeartbeatRpc = rpc::RequestResponse<FrequentHeartbeatReq, FrequentHeartbeatRes>;

void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);

}  // namespace memgraph::replication
