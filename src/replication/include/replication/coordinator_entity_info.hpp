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

#include "io/network/endpoint.hpp"

#include <string>

namespace memgraph::replication {

#ifdef MG_ENTERPRISE

// TODO: (andi) Eventually unite with one global replica info
struct CoordinatorEntityInfo {
  std::string name;
  io::network::Endpoint endpoint;
};

struct CoordinatorEntityHealthInfo {
  std::string name;
  bool alive;
};
#endif

}  // namespace memgraph::replication
