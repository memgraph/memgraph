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

#include <atomic>
#include <cassert>
#include <exception>
#include <functional>
#include <future>

#include <exception>
#include <latch>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "mg_procedure.h"
#include "mgp.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"

using safe_ptr = std::unique_ptr<void, decltype([](void *p) { mgp_global_free(p); })>;
enum class AllocFuncRes { NoIssues, UnableToAlloc, Unexpected };
using result_t = std::pair<AllocFuncRes, std::list<safe_ptr>>;

constexpr auto N_THREADS = 2;
static_assert(N_THREADS > 0 && (N_THREADS & (N_THREADS - 1)) == 0);

constexpr auto mb_size_512 = 1 << 29;
constexpr auto mb_size_16 = 1 << 24;

static_assert(mb_size_512 % N_THREADS == 0);
static_assert(mb_size_16 % N_THREADS == 0);
static_assert(mb_size_512 % mb_size_16 == 0);

void AllocFunc(std::latch &start_latch, std::promise<result_t> promise, mgp_graph *graph) {
  [[maybe_unused]] const enum mgp_error tracking_error = mgp_track_current_thread_allocations(graph);
  auto on_exit = memgraph::utils::OnScopeExit{[&]() {
    [[maybe_unused]] const enum mgp_error untracking_error = mgp_untrack_current_thread_allocations(graph);
  }};

  std::list<safe_ptr> ptrs;

  // Ensure test would concurrently run these allocations, wait until both are ready
  start_latch.arrive_and_wait();

  try {
    constexpr auto allocation_limit = mb_size_512 / N_THREADS;
    // many allocation to increase chance of seeing any concurent issues
    for (auto total = 0; total < allocation_limit; total += mb_size_16) {
      void *ptr = nullptr;
      auto alloc_err = mgp_global_alloc(mb_size_16, &ptr);
      if (alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE && ptr != nullptr) {
        ptrs.emplace_back(ptr);
      } else if (alloc_err == mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE) {
        // this is expected, the test checks N threads allocating to a limit of 512MB
        promise.set_value({AllocFuncRes::UnableToAlloc, std::move(ptrs)});
        return;
      } else {
        promise.set_value({AllocFuncRes::Unexpected, std::move(ptrs)});
        return;
      }
    }
  } catch (const std::exception &e) {
    promise.set_value({AllocFuncRes::Unexpected, std::move(ptrs)});
    return;
  }
  promise.set_value({AllocFuncRes::NoIssues, std::move(ptrs)});
  return;
}

void DualThread(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  // 1 byte allocation to
  auto ptr = std::invoke([&] {
    void *ptr;
    [[maybe_unused]] auto alloc_err = mgp_global_alloc(1, &ptr);
    return safe_ptr{ptr};
  });

  try {
    auto futures = std::vector<std::future<result_t>>{};
    futures.reserve(N_THREADS);
    std::latch start_latch{N_THREADS};
    {
      auto threads = std::vector<std::jthread>{};
      threads.reserve(N_THREADS);
      for (int i = 0; i < N_THREADS; i++) {
        auto promise = std::promise<result_t>{};
        futures.emplace_back(promise.get_future());
        threads.emplace_back([&, promise = std::move(promise)]() mutable {
          AllocFunc(start_latch, std::move(promise), memgraph_graph);
        });
      }
    }  // ~jthread will join

    int alloc_errors = 0;
    int unexpected_errors = 0;
    for (auto &x : futures) {
      auto [res, ptrs] = x.get();
      alloc_errors += (res == AllocFuncRes::UnableToAlloc);
      unexpected_errors += (res == AllocFuncRes::Unexpected);
      // regardless of outcome, we want this thread to do the deallocation
      ptrs.clear();
    }

    if (unexpected_errors != 0) {
      record_factory.SetErrorMessage("Unanticipated error happened");
      return;
    }

    if (alloc_errors < 1) {
      record_factory.SetErrorMessage("Didn't hit the QUERY MEMORY LIMIT we expected");
      return;
    }

    auto new_record = record_factory.NewRecord();
    new_record.Insert("test_passed", true);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::memory = memory;

    AddProcedure(DualThread, std::string("dual_thread").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("test_passed").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
