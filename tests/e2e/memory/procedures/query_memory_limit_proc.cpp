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

#include <atomic>
#include <cassert>
#include <exception>
#include <functional>
#include <iostream>
#include <mgp.hpp>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mg_procedure.h"
#include "utils/on_scope_exit.hpp"
namespace {
constexpr size_t mb_size_268 = 1UL << 28U;
}  // namespace

enum mgp_error Alloc(void *&ptr) { return mgp_global_alloc(mb_size_268, (void **)(&ptr)); }

void Regular(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  std::cout << 1.23 << "test that the cout will not cause a crash" << std::endl;

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit cleanup{[&ptr]() {
      if (nullptr != ptr) mgp_global_free(ptr);
    }};

    const enum mgp_error alloc_err = Alloc(ptr);
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void Malloc(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  std::cout << 1.23 << "test that the cout will not cause a crash" << std::endl;

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit cleanup{[&ptr]() {
      if (nullptr != ptr) free(ptr);
    }};

    ptr = malloc(mb_size_268);
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", ptr != nullptr);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void New(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  std::cout << 1.23 << "test that the cout will not cause a crash" << std::endl;

  try {
    auto *ptr = new char[mb_size_268];
    // Prevent compiler from optimizing away the allocation
    asm volatile("" : : "r"(ptr) : "memory");

    memgraph::utils::OnScopeExit cleanup{[&ptr]() {
      if (nullptr != ptr) delete[] ptr;
    }};

    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", ptr != nullptr);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void LocalHeap(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  std::cout << 1.23 << "test that the cout will not cause a crash" << std::endl;

  try {
    static std::vector<uint8_t> vec __attribute__((__used__));
    memgraph::utils::OnScopeExit cleanup{[]() {
      vec.clear();
      vec.shrink_to_fit();
    }};
    // Trying to always allocate new memory
    vec.resize(vec.size() + mb_size_268);
    for (int i = 0; i < vec.size(); i += 4096) {
      vec[i] = 1;
    }
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", true);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard mdg{memory};

    AddProcedure(Regular, std::string("regular").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(LocalHeap, std::string("local_heap").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(Malloc, std::string("malloc").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(New, std::string("new").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
