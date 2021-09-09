#include "mg_procedure.h"

int *gVal = NULL;

void set_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Something went wrong"); }

static void procedure(const struct mgp_list *args, const struct mgp_graph *graph, struct mgp_result *result,
                      struct mgp_memory *memory) {
  struct mgp_result_record *record = NULL;
  const enum mgp_error new_record_err = mgp_result_new_record(result, &record);
  if (new_record_err != MGP_ERROR_NO_ERROR) return set_error(result);

  struct mgp_value *result_msg = NULL;
  const enum mgp_error make_string_err = mgp_value_make_string("mgp_init_module allocation works", memory, &result_msg);
  if (make_string_err != MGP_ERROR_NO_ERROR) return set_error(result);

  const enum mgp_error result_inserted = mgp_result_record_insert(record, "result", result_msg);
  mgp_value_destroy(result_msg);
  if (result_inserted != MGP_ERROR_NO_ERROR) return set_error(result);
}

int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  const size_t one_gb = 1 << 30;
  const enum mgp_error alloc_err = mgp_global_alloc(one_gb, &gVal);
  if (alloc_err != MGP_ERROR_NO_ERROR) return 1;

  struct mgp_proc *proc = NULL;
  const enum mgp_error proc_err = mgp_module_add_read_procedure(module, "procedure", procedure, &proc);
  if (proc_err != MGP_ERROR_NO_ERROR) return 1;

  const struct mgp_type *string_type = NULL;
  const enum mgp_error string_type_err = mgp_type_string(&string_type);
  if (string_type_err != MGP_ERROR_NO_ERROR) return 1;
  if (mgp_proc_add_result(proc, "result", string_type) != MGP_ERROR_NO_ERROR) return 1;

  return 0;
}

int mgp_shutdown_module() {
  if (gVal) mgp_global_free(gVal);
  return 0;
}
