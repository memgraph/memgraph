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

#include "replication/coordinator_config.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

#ifdef MG_ENTERPRISE

namespace memgraph::slk {

using ReplicationClientInfo = replication::CoordinatorClientConfig::ReplicationClientInfo;

inline void Save(replication::ReplicationMode obj, Builder *builder) {
  Save(static_cast<std::underlying_type_t<replication::ReplicationMode>>(obj), builder);
}

inline void Load(replication::ReplicationMode *obj, Reader *reader) {
  using enum_type = std::underlying_type_t<replication::ReplicationMode>;
  enum_type obj_encoded{};
  slk::Load(&obj_encoded, reader);
  *obj = replication::ReplicationMode(utils::MemcpyCast<enum_type>(obj_encoded));
}

inline void Save(const ReplicationClientInfo &obj, Builder *builder) {
  Save(obj.instance_name, builder);
  Save(obj.replication_mode, builder);
  Save(obj.replication_ip_address, builder);
  Save(obj.replication_port, builder);
}

inline void Load(ReplicationClientInfo *obj, Reader *reader) {
  Load(&obj->instance_name, reader);
  Load(&obj->replication_mode, reader);
  Load(&obj->replication_ip_address, reader);
  Load(&obj->replication_port, reader);
}
}  // namespace memgraph::slk
#endif
