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

#include <mgp.hpp>

#include "algorithm/util.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(Util::Md5Procedure, std::string(Util::kProcedureMd5), mgp::ProcedureType::Read,
                 {mgp::Parameter(std::string(Util::kArgumentValuesMd5), mgp::Type::Any)},
                 {mgp::Return(std::string(Util::kArgumentResultMd5), mgp::Type::String)}, module, memory);

    mgp::AddFunction(Util::Md5Function, std::string(Util::kProcedureMd5),
                     {mgp::Parameter(std::string(Util::kArgumentStringToHash), mgp::Type::Any)}, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
