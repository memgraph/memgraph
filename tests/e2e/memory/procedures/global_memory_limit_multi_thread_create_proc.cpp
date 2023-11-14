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

// change communication between threads with feature and promise
std::atomic<int> created_vertices{0};
constexpr int num_vertices_per_thread{100'000};
constexpr int num_threads{2};

void CallCreate(mgp_graph *graph, mgp_memory *memory) {
  [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(graph);
  for (int i = 0; i < num_vertices_per_thread; i++) {
    struct mgp_vertex *vertex{nullptr};
    auto enum_error = mgp_graph_create_vertex(graph, memory, &vertex);
    if (enum_error != mgp_error::MGP_ERROR_NO_ERROR) {
      break;
    }
    created_vertices.fetch_add(1, std::memory_order_acq_rel);
  }
  [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(graph);
}

void AllocFunc(mgp_graph *graph, mgp_memory *memory) {
  try {
    CallCreate(graph, memory);
  } catch (const std::exception &e) {
    return;
  }
}

void MultiCreate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    std::vector<std::thread> threads;

    for (int i = 0; i < 2; i++) {
      threads.emplace_back(AllocFunc, memgraph_graph, memory);
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    if (created_vertices.load(std::memory_order_acquire) != num_vertices_per_thread * num_threads) {
      record_factory.SetErrorMessage("Unable to allocate");
      return;
    }

    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated_all",
                      created_vertices.load(std::memory_order_acquire) == num_vertices_per_thread * num_threads);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(MultiCreate, std::string("multi_create").c_str(), mgp::ProcedureType::Write, {},
                 {mgp::Return(std::string("allocated_all").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
