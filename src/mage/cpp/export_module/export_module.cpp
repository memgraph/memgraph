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

#include "algorithm/export.hpp"

namespace {

// `file` is nullable (null means "stream or discard"), `config` is nullable (null means "no config", as on the
// reference), and so is `data` (null whenever the payload went to a file).
void AddSignature(mgp_proc *proc, mgp_value *default_config) {
  mgp::proc_add_arg(proc, Export::kArgumentFile, mgp::type_nullable(mgp::type_string()));
  mgp::proc_add_opt_arg(proc, Export::kArgumentConfig, mgp::type_nullable(mgp::type_map()), default_config);

  mgp::proc_add_result(proc, Export::kReturnFile, mgp::type_nullable(mgp::type_string()));
  mgp::proc_add_result(proc, Export::kReturnSource, mgp::type_string());
  mgp::proc_add_result(proc, Export::kReturnFormat, mgp::type_string());
  mgp::proc_add_result(proc, Export::kReturnNodes, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnRelationships, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnProperties, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnTime, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnRows, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnBatchSize, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnBatches, mgp::type_int());
  mgp::proc_add_result(proc, Export::kReturnDone, mgp::type_bool());
  mgp::proc_add_result(proc, Export::kReturnData, mgp::type_nullable(mgp::type_any()));
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};
    // RAII rather than a raw mgp_value*: the map would leak if wrapping it threw. proc_add_opt_arg deep-copies the
    // default, so one instance can back all three procedures and die at the end of this scope.
    const auto default_config = mgp::Value(mgp::Map{});

    // The low-level path rather than mgp::AddProcedure: `nodes`/`rels` must accept NULL (the reference coerces it to an
    // empty list) and mgp::AddProcedure only builds non-nullable list types.
    auto *json_data = mgp::module_add_read_procedure(module, Export::kProcedureJsonData, Export::JsonData);
    mgp::proc_add_arg(json_data, Export::kArgumentNodes, mgp::type_nullable(mgp::type_list(mgp::type_node())));
    mgp::proc_add_arg(
        json_data, Export::kArgumentRelationships, mgp::type_nullable(mgp::type_list(mgp::type_relationship())));
    AddSignature(json_data, default_config.ptr());

    AddSignature(mgp::module_add_read_procedure(module, Export::kProcedureJsonAll, Export::JsonAll),
                 default_config.ptr());

    auto *json_graph = mgp::module_add_read_procedure(module, Export::kProcedureJsonGraph, Export::JsonGraph);
    mgp::proc_add_arg(json_graph, Export::kArgumentGraph, mgp::type_map());
    AddSignature(json_graph, default_config.ptr());
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
