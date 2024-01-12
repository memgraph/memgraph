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

#include "replication/config.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

#ifdef MG_ENTERPRISE

namespace memgraph::slk {

inline void Save(replication::ReplicationMode obj, Builder *builder) {
  Save(static_cast<std::underlying_type_t<replication::ReplicationMode>>(obj), builder);
}

inline void Load(replication::ReplicationMode *obj, Reader *reader) {
  using enum_type = std::underlying_type_t<replication::ReplicationMode>;
  enum_type obj_encoded{};
  slk::Load(&obj_encoded, reader);
  *obj = replication::ReplicationMode(utils::MemcpyCast<enum_type>(obj_encoded));
}

inline void Save(const std::chrono::seconds &obj, Builder *builder) { Save(obj.count(), builder); }

inline void Load(std::chrono::seconds *obj, Reader *reader) {
  using chrono_type = std::chrono::seconds;
  typename chrono_type::rep obj_encoded{};
  slk::Load(&obj_encoded, reader);
  *obj = chrono_type(utils::MemcpyCast<typename chrono_type::rep>(obj_encoded));
}

inline void Save(const replication::ReplicationClientConfig::SSL &obj, Builder *builder) {
  Save(obj.key_file, builder);
  Save(obj.cert_file, builder);
}

inline void Load(replication::ReplicationClientConfig::SSL *obj, Reader *reader) {
  Load(&obj->key_file, reader);
  Load(&obj->cert_file, reader);
}

inline void Save(const replication::ReplicationClientConfig &obj, Builder *builder) {
  Save(obj.name, builder);
  Save(obj.mode, builder);
  Save(obj.ip_address, builder);
  Save(obj.port, builder);
  Save(obj.replica_check_frequency, builder);
  Save(obj.ssl, builder);
}

inline void Load(replication::ReplicationClientConfig *obj, Reader *reader) {
  Load(&obj->name, reader);
  Load(&obj->mode, reader);
  Load(&obj->ip_address, reader);
  Load(&obj->port, reader);
  Load(&obj->replica_check_frequency, reader);
  Load(&obj->ssl, reader);
}
}  // namespace memgraph::slk
#endif
