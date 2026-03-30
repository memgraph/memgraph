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
#include <string>

// Procedure that accepts a ZonedDateTime argument and returns its timestamp as a string.
// Used to test that mgp_type_zoned_date_time() works as a type annotation.
void ZonedDateTimeToString(mgp_list *args, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  mgp_value *arg{nullptr};
  mgp_list_at(args, 0, &arg);

  mgp_zoned_date_time *zdt{nullptr};
  mgp_value_get_zoned_date_time(arg, &zdt);

  int64_t timestamp{0};
  mgp_zoned_date_time_timestamp(zdt, &timestamp);

  auto str = std::to_string(timestamp);
  auto *record = mgp::result_new_record(result);
  mgp::result_record_insert(record, "result", mgp::value_make_string(str.c_str(), memory));
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  try {
    auto *proc = mgp::module_add_read_procedure(query_module, "zoned_date_time_to_string", ZonedDateTimeToString);
    mgp::proc_add_arg(proc, "val", mgp::type_zoned_date_time());
    mgp::proc_add_result(proc, "result", mgp::type_string());
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
