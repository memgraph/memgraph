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

#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mg_procedure.h"
#include "mgp.hpp"
#include "utils/on_scope_exit.hpp"

enum mgp_error Alloc(mgp_memory *memory, void *ptr) {
  const size_t mb_size_512 = 1 << 29;

  return mgp_alloc(memory, mb_size_512, (void **)(&ptr));
}

// change communication between threads with feature and promise
std::atomic<int> num_allocations{0};
void *ptr_;

void AllocFunc(mgp_memory *memory, mgp_graph *graph) {
  try {
    [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(graph);
    enum mgp_error alloc_err { mgp_error::MGP_ERROR_NO_ERROR };
    alloc_err = Alloc(memory, ptr_);
    if (alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE) {
      num_allocations.fetch_add(1, std::memory_order_relaxed);
    }
    if (alloc_err != mgp_error::MGP_ERROR_NO_ERROR) {
      assert(false);
    }
  } catch (const std::exception &e) {
    [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(graph);
    assert(false);
  }
  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(graph);
}

void Thread(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  num_allocations.store(0, std::memory_order_relaxed);
  try {
    std::thread thread{AllocFunc, memory, memgraph_graph};

    thread.join();

    if (ptr_ != nullptr) {
      mgp_free(memory, ptr_);
    }

    auto new_record = record_factory.NewRecord();

    new_record.Insert("allocated_all", num_allocations.load(std::memory_order_relaxed) == 1);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::memory = memory;

    AddProcedure(Thread, std::string("thread").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated_all").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
