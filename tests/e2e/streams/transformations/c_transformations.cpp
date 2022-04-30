// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "mg_procedure.h"

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  static const auto no_op_cb = [](mgp_messages *msg, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {};

  if (MGP_ERROR_NO_ERROR != mgp_module_add_transformation(module, "empty_transformation", no_op_cb)) {
    return 1;
  }

  return 0;
}