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

struct InstanceStatus {
  std::string instance_name;
  std::string coordinator_server;
  std::string management_server;
  std::string bolt_server;
  std::string cluster_role;
  std::string health;
  int64_t last_succ_resp_ms;

  friend bool operator==(InstanceStatus const &lhs, InstanceStatus const &rhs) = default;
};

}  // namespace memgraph::coordination

#endif
