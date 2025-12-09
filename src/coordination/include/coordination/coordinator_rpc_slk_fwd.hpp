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

#ifdef MG_ENTERPRISE

namespace memgraph::slk {
class Reader;
class Builder;
}  // namespace memgraph::slk

// Forward declarations of coordinator RPC types (defined in module memgraph.coordination.coordinator_rpc)
namespace memgraph::coordination {

struct PromoteToMainReq;
struct PromoteToMainRes;
struct DemoteMainToReplicaReq;
struct DemoteMainToReplicaRes;
struct RegisterReplicaOnMainReq;
struct RegisterReplicaOnMainRes;
struct UnregisterReplicaReq;
struct UnregisterReplicaRes;
struct EnableWritingOnMainReq;
struct EnableWritingOnMainRes;
struct GetDatabaseHistoriesReq;
struct GetDatabaseHistoriesResV1;
struct GetDatabaseHistoriesRes;
struct ShowInstancesReq;
struct ShowInstancesRes;
struct GetRoutingTableReq;
struct GetRoutingTableRes;
struct StateCheckReqV1;
struct StateCheckReq;
struct StateCheckResV1;
struct StateCheckRes;
struct ReplicationLagReq;
struct ReplicationLagRes;

}  // namespace memgraph::coordination

// SLK function declarations for coordinator RPC types
// These enable ADL to find them from templates in headers
namespace memgraph::slk {

// PromoteToMainRpc
void Save(coordination::PromoteToMainReq const &self, Builder *builder);
void Load(coordination::PromoteToMainReq *self, Reader *reader);
void Save(coordination::PromoteToMainRes const &self, Builder *builder);
void Load(coordination::PromoteToMainRes *self, Reader *reader);

// DemoteMainToReplicaRpc
void Save(coordination::DemoteMainToReplicaReq const &self, Builder *builder);
void Load(coordination::DemoteMainToReplicaReq *self, Reader *reader);
void Save(coordination::DemoteMainToReplicaRes const &self, Builder *builder);
void Load(coordination::DemoteMainToReplicaRes *self, Reader *reader);

// RegisterReplicaOnMainRpc
void Save(coordination::RegisterReplicaOnMainReq const &self, Builder *builder);
void Load(coordination::RegisterReplicaOnMainReq *self, Reader *reader);
void Save(coordination::RegisterReplicaOnMainRes const &self, Builder *builder);
void Load(coordination::RegisterReplicaOnMainRes *self, Reader *reader);

// UnregisterReplicaRpc
void Save(coordination::UnregisterReplicaReq const &self, Builder *builder);
void Load(coordination::UnregisterReplicaReq *self, Reader *reader);
void Save(coordination::UnregisterReplicaRes const &self, Builder *builder);
void Load(coordination::UnregisterReplicaRes *self, Reader *reader);

// EnableWritingOnMainRpc
void Save(coordination::EnableWritingOnMainReq const &self, Builder *builder);
void Load(coordination::EnableWritingOnMainReq *self, Reader *reader);
void Save(coordination::EnableWritingOnMainRes const &self, Builder *builder);
void Load(coordination::EnableWritingOnMainRes *self, Reader *reader);

// GetDatabaseHistoriesRpc
void Save(coordination::GetDatabaseHistoriesReq const &self, Builder *builder);
void Load(coordination::GetDatabaseHistoriesReq *self, Reader *reader);
void Save(coordination::GetDatabaseHistoriesResV1 const &self, Builder *builder);
void Load(coordination::GetDatabaseHistoriesResV1 *self, Reader *reader);
void Save(coordination::GetDatabaseHistoriesRes const &self, Builder *builder);
void Load(coordination::GetDatabaseHistoriesRes *self, Reader *reader);

// ShowInstancesRpc
void Save(coordination::ShowInstancesReq const &self, Builder *builder);
void Load(coordination::ShowInstancesReq *self, Reader *reader);
void Save(coordination::ShowInstancesRes const &self, Builder *builder);
void Load(coordination::ShowInstancesRes *self, Reader *reader);

// GetRoutingTableRpc
void Save(coordination::GetRoutingTableReq const &self, Builder *builder);
void Load(coordination::GetRoutingTableReq *self, Reader *reader);
void Save(coordination::GetRoutingTableRes const &self, Builder *builder);
void Load(coordination::GetRoutingTableRes *self, Reader *reader);

// StateCheckRpc
void Save(coordination::StateCheckReqV1 const &self, Builder *builder);
void Load(coordination::StateCheckReqV1 *self, Reader *reader);
void Save(coordination::StateCheckReq const &self, Builder *builder);
void Load(coordination::StateCheckReq *self, Reader *reader);
void Save(coordination::StateCheckResV1 const &self, Builder *builder);
void Load(coordination::StateCheckResV1 *self, Reader *reader);
void Save(coordination::StateCheckRes const &self, Builder *builder);
void Load(coordination::StateCheckRes *self, Reader *reader);

// ReplicationLagRpc
void Save(coordination::ReplicationLagReq const &self, Builder *builder);
void Load(coordination::ReplicationLagReq *self, Reader *reader);
void Save(coordination::ReplicationLagRes const &self, Builder *builder);
void Load(coordination::ReplicationLagRes *self, Reader *reader);

}  // namespace memgraph::slk

#endif
