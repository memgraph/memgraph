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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_config.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"

namespace memgraph::coordination {

struct FailoverReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FailoverReq *self, memgraph::slk::Reader *reader);
  static void Save(const FailoverReq &self, memgraph::slk::Builder *builder);

  explicit FailoverReq(std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info)
      : replication_clients_info(std::move(replication_clients_info)) {}
  FailoverReq() = default;

  std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info;
};

struct FailoverRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FailoverRes *self, memgraph::slk::Reader *reader);
  static void Save(const FailoverRes &self, memgraph::slk::Builder *builder);

  explicit FailoverRes(bool success) : success(success) {}
  FailoverRes() = default;

  bool success;
};

using FailoverRpc = rpc::RequestResponse<FailoverReq, FailoverRes>;

}  // namespace memgraph::coordination

// SLK serialization declarations
namespace memgraph::slk {

void Save(const memgraph::coordination::FailoverRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::FailoverRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::coordination::FailoverReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::FailoverReq *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

#endif
