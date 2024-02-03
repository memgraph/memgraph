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
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph::slk {

using ReplicationClientInfo = coordination::CoordinatorClientConfig::ReplicationClientInfo;

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
