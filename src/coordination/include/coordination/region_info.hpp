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

#include <string>
#include <vector>

namespace memgraph::coordination {

// One row of SHOW REGION INFO.
struct RegionInfo {
  std::string name;
  std::vector<std::string> instances;
  bool is_primary_region{false};

  friend bool operator==(RegionInfo const &lhs, RegionInfo const &rhs) = default;
};

}  // namespace memgraph::coordination

#endif
