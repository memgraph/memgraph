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

#include "coordination/coordinator_communication_config.hpp"
#include "replication_coordination_glue/common.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::slk {

using ReplicationClientInfo = coordination::ReplicationClientInfo;

inline void Save(io::network::Endpoint const &obj, Builder *builder) {
  Save(obj.address, builder);
  Save(obj.port, builder);
  Save(obj.family, builder);
}

inline void Load(io::network::Endpoint *obj, Reader *reader) {
  Load(&obj->address, reader);
  Load(&obj->port, reader);
  Load(&obj->family, reader);
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

inline void Save(const replication_coordination_glue::DatabaseHistory &obj, Builder *builder) {
  Save(obj.db_uuid, builder);
  Save(obj.history, builder);
  Save(obj.name, builder);
}

inline void Load(replication_coordination_glue::DatabaseHistory *obj, Reader *reader) {
  Load(&obj->db_uuid, reader);
  Load(&obj->history, reader);
  Load(&obj->name, reader);
}

}  // namespace memgraph::slk
#endif
