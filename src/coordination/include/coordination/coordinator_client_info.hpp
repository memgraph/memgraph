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

#include <atomic>
#include <chrono>

namespace memgraph::coordination {

// TODO: better connect this
struct CoordinatorClientInfo {
  std::atomic<std::chrono::system_clock::time_point> last_response_time_{};
  bool is_alive_{false};
  std::string_view instance_name_;
};

}  // namespace memgraph::coordination

#endif
