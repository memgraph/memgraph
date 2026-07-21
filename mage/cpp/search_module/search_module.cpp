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

#include "algorithm/search.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    const auto register_procedure = [module](const char *name, mgp_proc_cb callback) {
      auto *proc = mgp::module_add_read_procedure(module, name, callback);
      mgp::proc_add_arg(proc, Search::kArgumentLabelPropertyMap, mgp::type_any());
      mgp::proc_add_arg(proc, Search::kArgumentOperator, mgp::type_string());
      mgp::proc_add_arg(proc, Search::kArgumentValue, mgp::type_nullable(mgp::type_string()));
      mgp::proc_add_result(proc, Search::kReturnNode, mgp::type_node());
    };

    register_procedure(Search::kProcedureNode, Search::Node);
    register_procedure(Search::kProcedureNodeAll, Search::NodeAll);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
