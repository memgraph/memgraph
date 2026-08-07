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

#include "algorithm/path.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddFunction(Path::Elements,
                     Path::kProcedureElements,
                     {mgp::Parameter(Path::kElementsArg1, mgp::Type::Path)},
                     module,
                     memory);

    mgp::AddFunction(
        Path::Combine,
        Path::kProcedureCombine,
        {mgp::Parameter(Path::kCombineArg1, mgp::Type::Path), mgp::Parameter(Path::kCombineArg2, mgp::Type::Path)},
        module,
        memory);

    mgp::AddFunction(Path::Slice,
                     Path::kProcedureSlice,
                     {mgp::Parameter(Path::kSliceArg1, mgp::Type::Path),
                      mgp::Parameter(Path::kSliceArg2, mgp::Type::Int, static_cast<int64_t>(0)),
                      mgp::Parameter(Path::kSliceArg3, mgp::Type::Int, static_cast<int64_t>(-1))},
                     module,
                     memory);

    // The start node is registered through the low-level API because mgp::Parameter cannot express a
    // nullable type, and a null start has to reach the procedure body to return no rows rather than
    // being rejected as a type error.
    auto *expand = mgp::module_add_read_procedure(module, Path::kProcedureExpand.data(), Path::Expand);
    mgp::proc_add_arg(expand, Path::kArgumentStartExpand.data(), mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(expand, Path::kArgumentRelationshipsExpand.data(), mgp::type_list(mgp::type_string()));
    mgp::proc_add_arg(expand, Path::kArgumentLabelsExpand.data(), mgp::type_list(mgp::type_string()));
    mgp::proc_add_arg(expand, Path::kArgumentMinHopsExpand.data(), mgp::type_int());
    mgp::proc_add_arg(expand, Path::kArgumentMaxHopsExpand.data(), mgp::type_int());
    mgp::proc_add_result(expand, Path::kResultExpand.data(), mgp::type_path());

    auto *expand_config =
        mgp::module_add_read_procedure(module, Path::kProcedureExpandConfig.data(), Path::ExpandConfig);
    mgp::proc_add_arg(expand_config, Path::kArgumentStartExpand.data(), mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(expand_config, Path::kArgumentConfigExpandConfig.data(), mgp::type_map());
    mgp::proc_add_result(expand_config, Path::kResultExpand.data(), mgp::type_path());

    auto empty_list = mgp::Value(mgp::List{});
    auto empty_map = mgp::Map{};
    empty_map.Insert("key", empty_list);

    AddProcedure(Path::Create,
                 Path::kProcedureCreate,
                 mgp::ProcedureType::Read,
                 {mgp::Parameter(Path::kCreateArg1, mgp::Type::Node),
                  mgp::Parameter(Path::kCreateArg2, {mgp::Type::Map, mgp::Type::List}, mgp::Value(empty_map))},
                 {mgp::Return(Path::kResultCreate, mgp::Type::Path)},
                 module,
                 memory);

    auto *subgraph_nodes =
        mgp::module_add_read_procedure(module, Path::kProcedureSubgraphNodes.data(), Path::SubgraphNodes);
    mgp::proc_add_arg(subgraph_nodes, Path::kArgumentsStart.data(), mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(subgraph_nodes, Path::kArgumentsConfig.data(), mgp::type_map());
    mgp::proc_add_result(subgraph_nodes, Path::kReturnSubgraphNodes.data(), mgp::type_node());

    auto *subgraph_all = mgp::module_add_read_procedure(module, Path::kProcedureSubgraphAll.data(), Path::SubgraphAll);
    mgp::proc_add_arg(subgraph_all, Path::kArgumentsStart.data(), mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(subgraph_all, Path::kArgumentsConfig.data(), mgp::type_map());
    mgp::proc_add_result(subgraph_all, Path::kReturnNodesSubgraphAll.data(), mgp::type_list(mgp::type_node()));
    mgp::proc_add_result(subgraph_all, Path::kReturnRelsSubgraphAll.data(), mgp::type_list(mgp::type_relationship()));

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
