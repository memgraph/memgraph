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

#include <exception>
#include <stdexcept>

#include <functional>

#include "mg_procedure.h"

#include "mgp.hpp"

namespace {

using mgp_int = decltype(mgp::Value().ValueInt());
mgp_int num_ints{0};
mgp_int returned_ints{0};

mgp_int num_strings{0};
int returned_strings{0};

const char *kReturnOutput = "output";

void NumsBatchInit(struct mgp_list *args, mgp_graph *graph, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  if (arguments.Empty()) {
    throw std::runtime_error("Expected to recieve argument");
  }
  if (arguments[0].Type() != mgp::Type::Int) {
    throw std::runtime_error("Wrong type of first arguments");
  }
  const auto num = arguments[0].ValueInt();
  num_ints = num;
}

void NumsBatch(struct mgp_list *args, mgp_graph *graph, mgp_result *result, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  if (returned_ints < num_ints) {
    auto record = record_factory.NewRecord();
    record.Insert(kReturnOutput, ++returned_ints);
  }
}

void NumsBatchCleanup() {
  returned_ints = 0;
  num_ints = 0;
}

void StringsBatchInit(struct mgp_list *args, mgp_graph *graph, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  if (arguments.Empty()) {
    throw std::runtime_error("Expected to recieve argument");
  }
  if (arguments[0].Type() != mgp::Type::Int) {
    throw std::runtime_error("Wrong type of first arguments");
  }
  const auto num = arguments[0].ValueInt();
  num_strings = num;
}

void StringsBatch(struct mgp_list *args, mgp_graph *graph, mgp_result *result, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  if (returned_strings < num_strings) {
    auto record = record_factory.NewRecord();
    returned_strings++;
    record.Insert(kReturnOutput, "output");
  }
}

void StringsBatchCleanup() {
  returned_strings = 0;
  num_strings = 0;
}

}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple functions/procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    mgp_proc *proc{nullptr};
    auto err_code =
        mgp_module_add_batch_read_procedure(module, "batch_nums", NumsBatch, NumsBatchInit, NumsBatchCleanup, &proc);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *type_int{nullptr};
    static_cast<void>(mgp_type_int(&type_int));
    err_code = mgp_proc_add_arg(proc, "num_ints", type_int);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }

    mgp_type *return_type_int{nullptr};
    static_cast<void>(mgp_type_int(&return_type_int));
    err_code = mgp_proc_add_result(proc, "output", return_type_int);
    if (err_code != mgp_error::MGP_ERROR_NO_ERROR) {
      return 1;
    }
  }

  {
    try {
      mgp::MemoryDispatcherGuard guard(memory);
      mgp::AddBatchProcedure(StringsBatch, StringsBatchInit, StringsBatchCleanup, "batch_strings",
                             mgp::ProcedureType::Read, {mgp::Parameter("num_strings", mgp::Type::Int)},
                             {mgp::Return("output", mgp::Type::String)}, module, memory);

    } catch (const std::exception &e) {
      return 1;
    }
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() { return 0; }
