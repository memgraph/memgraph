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
  const size_t two_sixty_eight_mb = 1 << 28;

  return mgp_global_alloc(two_sixty_eight_mb, (void **)(&ptr));
}

// change communication between threads with feature and promise
std::atomic<int> num_allocations{0};
std::vector<void *> ptrs_;

void AllocFunc(mgp_graph *graph) {
  [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(graph);
  void *ptr = nullptr;

  ptrs_.emplace_back(ptr);
  try {
    enum mgp_error alloc_err { mgp_error::MGP_ERROR_NO_ERROR };
    alloc_err = Alloc(ptr);
    if (alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE) {
      num_allocations.fetch_add(1, std::memory_order_relaxed);
    }
    if (alloc_err != mgp_error::MGP_ERROR_NO_ERROR) {
      assert(false);
    }
  } catch (const std::exception &e) {
    assert(false);
  }

  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(graph);
}

void DualThread(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  num_allocations.store(0, std::memory_order_relaxed);
  try {
    std::vector<std::thread> threads;

    for (int i = 0; i < 2; i++) {
      threads.emplace_back(AllocFunc, memgraph_graph);
    }

    for (int i = 0; i < 2; i++) {
      threads[i].join();
    }
    for (void *ptr : ptrs_) {
      if (ptr != nullptr) {
        mgp_global_free(ptr);
      }
    }

    auto new_record = record_factory.NewRecord();

    new_record.Insert("allocated_all", num_allocations.load(std::memory_order_relaxed) == 2);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::memory = memory;

    AddProcedure(DualThread, std::string("dual_thread").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated_all").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
