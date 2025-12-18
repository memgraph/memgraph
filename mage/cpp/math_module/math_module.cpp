// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <mgp.hpp>

#include "algorithm/math.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddFunction(
        Math::Round, Math::kProcedureRound,
        {mgp::Parameter(std::string(Math::kArgumentValue).c_str(), mgp::Type::Double, 0.0),
         mgp::Parameter(std::string(Math::kArgumentPrecision).c_str(), mgp::Type::Int, static_cast<int64_t>(0)),
         mgp::Parameter(std::string(Math::kArgumentMode).c_str(), mgp::Type::String, "HALF_UP")},
        module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
