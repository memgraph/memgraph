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

struct PromoteReplicaToMainReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder);

  explicit PromoteReplicaToMainReq(std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info)
      : replication_clients_info(std::move(replication_clients_info)) {}
  PromoteReplicaToMainReq() = default;

  std::vector<CoordinatorClientConfig::ReplicationClientInfo> replication_clients_info;
};

struct PromoteReplicaToMainRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder);

  explicit PromoteReplicaToMainRes(bool success) : success(success) {}
  PromoteReplicaToMainRes() = default;

  bool success;
};

using PromoteReplicaToMainRpc = rpc::RequestResponse<PromoteReplicaToMainReq, PromoteReplicaToMainRes>;

struct DemoteMainToReplicaReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaReq(CoordinatorClientConfig::ReplicationClientInfo replication_client_info)
      : replication_client_info(std::move(replication_client_info)) {}

  DemoteMainToReplicaReq() = default;

  CoordinatorClientConfig::ReplicationClientInfo replication_client_info;
};

struct DemoteMainToReplicaRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaRes(bool success) : success(success) {}
  DemoteMainToReplicaRes() = default;

  bool success;
};

using DemoteMainToReplicaRpc = rpc::RequestResponse<DemoteMainToReplicaReq, DemoteMainToReplicaRes>;

}  // namespace memgraph::coordination

// SLK serialization declarations
namespace memgraph::slk {

void Save(const memgraph::coordination::PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::coordination::PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::coordination::DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::coordination::DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::coordination::DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

#endif
