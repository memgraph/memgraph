// Copyright 2026 Memgraph Ltd.
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

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/replication_lag_info.hpp"
#include "replication_coordination_glue/common.hpp"
#include "slk/serialization.hpp"

namespace memgraph::slk {

using ReplicationClientInfo = coordination::ReplicationClientInfo;
using InstanceStatus = coordination::InstanceStatus;

inline void Save(io::network::Endpoint const &obj, Builder *builder) {
  Save(obj.GetAddress(), builder);
  Save(obj.GetPort(), builder);
}

inline void Load(io::network::Endpoint *obj, Reader *reader) {
  Load(&obj->GetAddress(), reader);
  Load(&obj->GetPort(), reader);
}

inline void Save(coordination::UpdateInstanceConfig const &config, Builder *builder) {
  slk::Save(config.data, builder);
  slk::Save(config.bolt_endpoint, builder);
}

inline void Load(coordination::UpdateInstanceConfig *config, Reader *reader) {
  slk::Load(&config->data, reader);
  slk::Load(&config->bolt_endpoint, reader);
}

inline void Save(ReplicationClientInfo const &obj, Builder *builder) {
  Save(obj.instance_name, builder);
  Save(obj.replication_mode, builder);
  Save(obj.replication_server, builder);
}

inline void Load(ReplicationClientInfo *obj, Reader *reader) {
  Load(&obj->instance_name, reader);
  Load(&obj->replication_mode, reader);
  Load(&obj->replication_server, reader);
}

// InstanceInfo
inline void Save(const replication_coordination_glue::InstanceInfoV1 &obj, Builder *builder) {
  Save(obj.last_committed_system_timestamp, builder);
  Save(obj.dbs_info, builder);
}

inline void Load(replication_coordination_glue::InstanceInfoV1 *obj, Reader *reader) {
  Load(&obj->last_committed_system_timestamp, reader);
  Load(&obj->dbs_info, reader);
}

inline void Save(const replication_coordination_glue::InstanceInfo &obj, Builder *builder) {
  Save(obj.last_committed_system_timestamp, builder);
  Save(obj.dbs_info, builder);
}

inline void Load(replication_coordination_glue::InstanceInfo *obj, Reader *reader) {
  Load(&obj->last_committed_system_timestamp, reader);
  Load(&obj->dbs_info, reader);
}

// InstanceDBInfo
inline void Save(const replication_coordination_glue::InstanceDBInfoV1 &obj, Builder *builder) {
  Save(obj.db_uuid, builder);
  Save(obj.latest_durable_timestamp, builder);
}

inline void Load(replication_coordination_glue::InstanceDBInfoV1 *obj, Reader *reader) {
  Load(&obj->db_uuid, reader);
  Load(&obj->latest_durable_timestamp, reader);
}

inline void Save(const replication_coordination_glue::InstanceDBInfo &obj, Builder *builder) {
  Save(obj.db_uuid, builder);
  Save(obj.num_committed_txns, builder);
}

inline void Load(replication_coordination_glue::InstanceDBInfo *obj, Reader *reader) {
  Load(&obj->db_uuid, reader);
  Load(&obj->num_committed_txns, reader);
}

inline void Save(const InstanceStatus &obj, Builder *builder) {
  Save(obj.instance_name, builder);
  Save(obj.coordinator_server, builder);
  Save(obj.management_server, builder);
  Save(obj.bolt_server, builder);
  Save(obj.health, builder);
  Save(obj.last_succ_resp_ms, builder);
  Save(obj.cluster_role, builder);
}

inline void Load(InstanceStatus *obj, Reader *reader) {
  Load(&obj->instance_name, reader);
  Load(&obj->coordinator_server, reader);
  Load(&obj->management_server, reader);
  Load(&obj->bolt_server, reader);
  Load(&obj->health, reader);
  Load(&obj->last_succ_resp_ms, reader);
  Load(&obj->cluster_role, reader);
}

inline void Save(const coordination::InstanceStateV1 &obj, Builder *builder) {
  Save(obj.is_replica, builder);
  Save(obj.uuid, builder);
  Save(obj.is_writing_enabled, builder);
}

inline void Load(coordination::InstanceStateV1 *obj, Reader *reader) {
  Load(&obj->is_replica, reader);
  Load(&obj->uuid, reader);
  Load(&obj->is_writing_enabled, reader);
}

inline void Save(const coordination::InstanceStateV2 &obj, Builder *builder) {
  Save(obj.is_replica, builder);
  Save(obj.uuid, builder);
  Save(obj.is_writing_enabled, builder);
  Save(obj.main_num_txns, builder);
  Save(obj.replicas_num_txns, builder);
}

inline void Load(coordination::InstanceStateV2 *obj, Reader *reader) {
  Load(&obj->is_replica, reader);
  Load(&obj->uuid, reader);
  Load(&obj->is_writing_enabled, reader);
  Load(&obj->main_num_txns, reader);
  Load(&obj->replicas_num_txns, reader);
}

inline void Save(const coordination::InstanceState &obj, Builder *builder) {
  Save(obj.inner_state, builder);
  Save(obj.deltas_batch_progress_size, builder);
}

inline void Load(coordination::InstanceState *obj, Reader *reader) {
  Load(&obj->inner_state, reader);
  Load(&obj->deltas_batch_progress_size, reader);
}

inline void Save(const coordination::ReplicaDBLagData &obj, Builder *builder) {
  Save(obj.num_committed_txns_, builder);
  Save(obj.num_txns_behind_main_, builder);
}

inline void Load(coordination::ReplicaDBLagData *obj, Reader *reader) {
  Load(&obj->num_committed_txns_, reader);
  Load(&obj->num_txns_behind_main_, reader);
}

inline void Save(const coordination::ReplicationLagInfo &obj, Builder *builder) {
  Save(obj.dbs_main_committed_txns_, builder);
  Save(obj.replicas_info_, builder);
}

inline void Load(coordination::ReplicationLagInfo *obj, Reader *reader) {
  Load(&obj->dbs_main_committed_txns_, reader);
  Load(&obj->replicas_info_, reader);
}

inline void Save(coordination::CoordinatorInstanceConfig const &config, Builder *builder) {
  Save(config.coordinator_hostname, builder);
  Save(config.coordinator_id, builder);
  Save(config.coordinator_server, builder);
  Save(config.bolt_server, builder);
  Save(config.management_server, builder);
}

inline void Load(coordination::CoordinatorInstanceConfig *obj, Reader *reader) {
  Load(&obj->coordinator_hostname, reader);
  Load(&obj->coordinator_id, reader);
  Load(&obj->coordinator_server, reader);
  Load(&obj->bolt_server, reader);
  Load(&obj->management_server, reader);
}

inline void Save(coordination::DataInstanceConfig const &config, Builder *builder) {
  Save(config.instance_name, builder);
  Save(config.replication_client_info, builder);
  Save(config.bolt_server, builder);
  Save(config.mgt_server, builder);
}

inline void Load(coordination::DataInstanceConfig *obj, Reader *reader) {
  Load(&obj->instance_name, reader);
  Load(&obj->replication_client_info, reader);
  Load(&obj->bolt_server, reader);
  Load(&obj->mgt_server, reader);
}

}  // namespace memgraph::slk
#endif
