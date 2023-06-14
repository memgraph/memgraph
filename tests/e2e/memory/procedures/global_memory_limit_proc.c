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

int *gVal = NULL;

void set_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Something went wrong"); }

void set_out_of_memory_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Out of memory"); }

static void error(struct mgp_list *args, struct mgp_graph *graph, struct mgp_result *result,
                  struct mgp_memory *memory) {
  const size_t one_gb = 1 << 30;
  if (gVal) {
    mgp_global_free(gVal);
    gVal = NULL;
  }
  if (!gVal) {
    const enum mgp_error err = mgp_global_alloc(one_gb, (void **)(&gVal));
    if (err == MGP_ERROR_UNABLE_TO_ALLOCATE) return set_out_of_memory_error(result);
    if (err != MGP_ERROR_NO_ERROR) return set_error(result);
  }
  struct mgp_result_record *record = NULL;
  const enum mgp_error new_record_err = mgp_result_new_record(result, &record);
  if (new_record_err != MGP_ERROR_NO_ERROR) return set_error(result);
  struct mgp_value *error_value = NULL;
  const enum mgp_error make_string_err = mgp_value_make_string("ERROR", memory, &error_value);
  if (make_string_err != MGP_ERROR_NO_ERROR) return set_error(result);
  const enum mgp_error result_inserted = mgp_result_record_insert(record, "error_result", error_value);
  mgp_value_destroy(error_value);
  if (result_inserted != MGP_ERROR_NO_ERROR) return set_error(result);
}

static void success(struct mgp_list *args, struct mgp_graph *graph, struct mgp_result *result,
                    struct mgp_memory *memory) {
  const size_t bytes = 1024;
  if (!gVal) {
    const enum mgp_error err = mgp_global_alloc(bytes, (void **)(&gVal));
    if (err == MGP_ERROR_UNABLE_TO_ALLOCATE) return set_out_of_memory_error(result);
    if (err != MGP_ERROR_NO_ERROR) return set_error(result);
  }

  struct mgp_result_record *record = NULL;
  const enum mgp_error new_record_err = mgp_result_new_record(result, &record);
  if (new_record_err != MGP_ERROR_NO_ERROR) return set_error(result);

  struct mgp_value *success_value = NULL;
  const enum mgp_error make_string_err = mgp_value_make_string("success", memory, &success_value);
  if (make_string_err != MGP_ERROR_NO_ERROR) return set_error(result);
  const enum mgp_error result_inserted = mgp_result_record_insert(record, "success_result", success_value);
  mgp_value_destroy(success_value);
  if (result_inserted != MGP_ERROR_NO_ERROR) return set_error(result);
}

int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  struct mgp_proc *error_proc = NULL;
  const enum mgp_error error_proc_err = mgp_module_add_read_procedure(module, "error", error, &error_proc);
  if (error_proc_err != MGP_ERROR_NO_ERROR) return 1;

  struct mgp_type *string_type = NULL;
  const enum mgp_error string_type_err = mgp_type_string(&string_type);
  if (string_type_err != MGP_ERROR_NO_ERROR) return 1;
  if (mgp_proc_add_result(error_proc, "error_result", string_type) != MGP_ERROR_NO_ERROR) return 1;

  struct mgp_proc *succ_proc = NULL;
  const enum mgp_error succ_proc_err = mgp_module_add_read_procedure(module, "success", success, &succ_proc);
  if (succ_proc_err != MGP_ERROR_NO_ERROR) return 1;

  if (mgp_proc_add_result(succ_proc, "success_result", string_type) != MGP_ERROR_NO_ERROR) return 1;

  return 0;
}

int mgp_shutdown_module() {
  if (gVal) mgp_global_free(gVal);
  return 0;
}
