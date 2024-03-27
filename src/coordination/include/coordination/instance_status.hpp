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

#include "io/network/endpoint.hpp"

#include <string_view>

namespace memgraph::coordination {

// TODO: (andi) For phase IV. Some instances won't have raft_socket_address, coord_socket_address, replication_role and
// cluster role... At the end, all instances will have everything.
struct InstanceStatus {
  std::string instance_name;
  std::string raft_socket_address;
  std::string coord_socket_address;
  std::string cluster_role;
  std::string health;
};

}  // namespace memgraph::coordination

#endif
