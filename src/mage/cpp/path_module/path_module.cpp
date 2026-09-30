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

    // Low-level API throughout: mgp::Parameter cannot express a nullable type, and a null path has to
    // reach the body to return null rather than be rejected as a type error. An OPTIONAL MATCH feeding
    // one of these is normal, and a type error there would abort the whole query.
    auto *nullable_path = mgp::type_nullable(mgp::type_path());

    auto *elements = mgp::module_add_function(module, Path::kProcedureElements, Path::Elements);
    mgp::func_add_arg(elements, Path::kElementsArg1, nullable_path);

    auto *combine = mgp::module_add_function(module, Path::kProcedureCombine, Path::Combine);
    mgp::func_add_arg(combine, Path::kCombineArg1, nullable_path);
    mgp::func_add_arg(combine, Path::kCombineArg2, nullable_path);

    auto default_offset = mgp::Value(static_cast<int64_t>(0));
    auto default_length = mgp::Value(Path::kSliceToEnd);
    auto *slice = mgp::module_add_function(module, Path::kProcedureSlice, Path::Slice);
    mgp::func_add_arg(slice, Path::kSliceArg1, nullable_path);
    mgp::func_add_opt_arg(slice, Path::kSliceArg2, mgp::type_int(), default_offset.ptr());
    mgp::func_add_opt_arg(slice, Path::kSliceArg3, mgp::type_int(), default_length.ptr());

    // A null start returns no rows, for the same reason.
    auto *expand = mgp::module_add_read_procedure(module, Path::kProcedureExpand, Path::Expand);
    mgp::proc_add_arg(expand, Path::kArgumentStartExpand, mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(expand, Path::kArgumentRelationshipsExpand, mgp::type_list(mgp::type_string()));
    mgp::proc_add_arg(expand, Path::kArgumentLabelsExpand, mgp::type_list(mgp::type_string()));
    mgp::proc_add_arg(expand, Path::kArgumentMinHopsExpand, mgp::type_int());
    mgp::proc_add_arg(expand, Path::kArgumentMaxHopsExpand, mgp::type_int());
    mgp::proc_add_result(expand, Path::kResultExpand, mgp::type_path());

    auto *expand_config = mgp::module_add_read_procedure(module, Path::kProcedureExpandConfig, Path::ExpandConfig);
    mgp::proc_add_arg(expand_config, Path::kArgumentStartExpand, mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(expand_config, Path::kArgumentConfigExpandConfig, mgp::type_map());
    mgp::proc_add_result(expand_config, Path::kResultExpand, mgp::type_path());

    auto empty_list = mgp::Value(mgp::List{});
    auto empty_map = mgp::Map{};
    empty_map.Insert("key", empty_list);
    auto default_relationships = mgp::Value(std::move(empty_map));

    // Nullable for the same reason: a start node from an OPTIONAL MATCH is normal to pass.
    auto *create = mgp::module_add_read_procedure(module, Path::kProcedureCreate, Path::Create);
    mgp::proc_add_arg(create, Path::kCreateArg1, mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_opt_arg(create, Path::kCreateArg2, mgp::type_map(), default_relationships.ptr());
    mgp::proc_add_result(create, Path::kResultCreate, mgp::type_path());

    auto *subgraph_nodes = mgp::module_add_read_procedure(module, Path::kProcedureSubgraphNodes, Path::SubgraphNodes);
    mgp::proc_add_arg(subgraph_nodes, Path::kArgumentsStart, mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(subgraph_nodes, Path::kArgumentsConfig, mgp::type_map());
    mgp::proc_add_result(subgraph_nodes, Path::kReturnSubgraphNodes, mgp::type_node());

    auto *subgraph_all = mgp::module_add_read_procedure(module, Path::kProcedureSubgraphAll, Path::SubgraphAll);
    mgp::proc_add_arg(subgraph_all, Path::kArgumentsStart, mgp::type_nullable(mgp::type_any()));
    mgp::proc_add_arg(subgraph_all, Path::kArgumentsConfig, mgp::type_map());
    mgp::proc_add_result(subgraph_all, Path::kReturnNodesSubgraphAll, mgp::type_list(mgp::type_node()));
    mgp::proc_add_result(subgraph_all, Path::kReturnRelsSubgraphAll, mgp::type_list(mgp::type_relationship()));

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
