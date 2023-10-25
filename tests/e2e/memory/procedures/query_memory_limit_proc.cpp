// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>
#include <cassert>
#include <exception>
#include <functional>
#include <mgp.hpp>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mg_procedure.h"
#include "utils/on_scope_exit.hpp"

enum mgp_error Alloc(void *ptr) {
  const size_t mb_size_268 = 1 << 28;

  return mgp_global_alloc(mb_size_268, (void **)(&ptr));
}

void Regular(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit<std::function<void(void)>> cleanup{[&ptr]() {
      if (nullptr == ptr) {
        return;
      }
      mgp_global_free(ptr);
    }};

    const enum mgp_error alloc_err = Alloc(ptr);
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard mdg{memory};

    AddProcedure(Regular, std::string("regular").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
