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

enum mgp_error Alloc_256(mgp_memory *memory, void *ptr) {
  const size_t mib_size_256 = 1 << 28;

  return mgp_alloc(memory, mib_size_256, (void **)(&ptr));
}

void Alloc_256_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit<std::function<void(void)>> cleanup{[&memory, &ptr]() {
      if (nullptr == ptr) {
        return;
      }
      mgp_free(memory, ptr);
    }};

    const enum mgp_error alloc_err = Alloc_256(memory, ptr);
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

enum mgp_error Alloc_32(mgp_memory *memory, void *ptr) {
  const size_t mib_size_32 = 1 << 25;

  return mgp_alloc(memory, mib_size_32, (void **)(&ptr));
}

void Alloc_32_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
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

    const enum mgp_error alloc_err = Alloc_32(memory, ptr);
    if (alloc_err != mgp_error::MGP_ERROR_NO_ERROR) {
      record_factory.SetErrorMessage("Unable to allocate");
    }
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard mdg{memory};

    AddProcedure(Alloc_256_MiB, std::string("alloc_256_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(Alloc_32_MiB, std::string("alloc_32_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
