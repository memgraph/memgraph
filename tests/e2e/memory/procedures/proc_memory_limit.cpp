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
#include <mgp.hpp>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mg_procedure.h"
#include "utils/on_scope_exit.hpp"

void Alloc(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, size_t size) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit cleanup{[&memory, &ptr]() {
      if (nullptr == ptr) {
        return;
      }
      mgp_free(memory, ptr);
    }};

    const enum mgp_error alloc_err = mgp_alloc(memory, size, (void **)(&ptr));
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", alloc_err != mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void Alloc_256_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_256 = 1 << 28;
  Alloc(args, memgraph_graph, result, memory, mib_size_256);
}

void Alloc_32_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_32 = 1 << 25;
  Alloc(args, memgraph_graph, result, memory, mib_size_32);
}

void Malloc(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, size_t size) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    void *ptr{nullptr};

    memgraph::utils::OnScopeExit cleanup{[&ptr]() {
      if (nullptr != ptr) free(ptr);
    }};

    ptr = malloc(size);
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", ptr != nullptr);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void Malloc_256_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_256 = 1 << 28;
  Malloc(args, memgraph_graph, result, memory, mib_size_256);
}

void Malloc_32_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_32 = 1 << 25;
  Malloc(args, memgraph_graph, result, memory, mib_size_32);
}

void New(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, size_t size) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    auto *ptr = new char[size];
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

void New_256_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_256 = 1 << 28;
  New(args, memgraph_graph, result, memory, mib_size_256);
}

void New_32_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_32 = 1 << 25;
  New(args, memgraph_graph, result, memory, mib_size_32);
}

void LocalHeap(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, size_t size) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    static std::vector<uint8_t> vec __attribute__((__used__));
    // Trying to always allocate new memory
    vec.resize(vec.size() + size);
    for (int i = 0; i < vec.size(); i += 4096) {
      vec[i] = 1;
    }
    auto new_record = record_factory.NewRecord();
    new_record.Insert("allocated", true);
  } catch (std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void LocalHeap_256_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_256 = 1 << 28;
  LocalHeap(args, memgraph_graph, result, memory, mib_size_256);
}

void LocalHeap_32_MiB(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const size_t mib_size_32 = 1 << 25;
  LocalHeap(args, memgraph_graph, result, memory, mib_size_32);
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard mdg{memory};

    AddProcedure(Alloc_256_MiB, std::string("alloc_256_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(Alloc_32_MiB, std::string("alloc_32_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(Malloc_256_MiB, std::string("malloc_256_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(Malloc_32_MiB, std::string("malloc_32_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(LocalHeap_256_MiB, std::string("local_heap_256_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(LocalHeap_32_MiB, std::string("local_heap_32_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(New_256_MiB, std::string("new_256_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

    AddProcedure(New_32_MiB, std::string("new_32_mib").c_str(), mgp::ProcedureType::Read, {},
                 {mgp::Return(std::string("allocated").c_str(), mgp::Type::Bool)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
