#include "mg_procedure.h"

int *gVal = NULL;

void set_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Something went wrong"); }

static void procedure(const struct mgp_list *args, const struct mgp_graph *graph, struct mgp_result *result,
                      struct mgp_memory *memory) {
  struct mgp_result_record *record = mgp_result_new_record(result);
  if (record == NULL) return set_error(result);

  struct mgp_value *result_msg = mgp_value_make_string("mgp_init_module allocation works", memory);
  if (result_msg == NULL) return set_error(result);

  int result_inserted = mgp_result_record_insert(record, "result", result_msg);
  mgp_value_destroy(result_msg);
  if (!result_inserted) return set_error(result);
}

int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  const size_t one_gb = 1 << 30;
  gVal = mgp_global_alloc(one_gb);
  if (!gVal) return 1;

  struct mgp_proc *proc = mgp_module_add_read_procedure(module, "procedure", procedure);
  if (!proc) return 1;

  if (!mgp_proc_add_result(proc, "result", mgp_type_string())) return 1;

  return 0;
}

int mgp_shutdown_module() {
  if (gVal) mgp_global_free(gVal);
  return 0;
}
