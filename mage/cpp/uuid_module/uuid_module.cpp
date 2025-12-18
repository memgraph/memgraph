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

#include <uuid/uuid.h>
#include <mg_exceptions.hpp>
#include <mg_utils.hpp>

namespace {

constexpr char const *kProcedureGenerate = "get";

constexpr char const *kFieldUuid = "uuid";

namespace {

// This way of generating UUID is also used in Memgraph repo
std::string GenerateUUID() {
  uuid_t uuid;
  char decoded[37];  // 36 bytes for UUID + 1 for null-terminator
  uuid_generate(uuid);
  uuid_unparse(uuid, decoded);
  return {decoded};
}

}  // namespace

void Generate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto const uuid = GenerateUUID();
    auto *record = mgp::result_new_record(result);
    mg_utility::InsertStringValueResult(record, kFieldUuid, uuid.c_str(), memory);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    struct mgp_proc *uuid_proc = mgp::module_add_read_procedure(module, kProcedureGenerate, Generate);

    mgp::proc_add_result(uuid_proc, kFieldUuid, mgp::type_string());
  } catch (std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
