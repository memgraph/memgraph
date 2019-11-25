#include "mg_procedure.h"

static void communities(const mgp_list *args, const mgp_graph *graph,
                        mgp_result *result, mgp_memory *memory) {
  mgp_result_record *record = mgp_result_new_record(result);
  mgp_value *hello_world_value =
      mgp_value_make_string("Louvain, fuck yeah!", memory);
  mgp_result_record_insert(record, "result", hello_world_value);
  mgp_value_destroy(hello_world_value);
}

extern "C" int mgp_init_module(struct mgp_module *module,
                               struct mgp_memory *memory) {
  struct mgp_proc *proc =
      mgp_module_add_read_procedure(module, "communities", communities);
  if (!mgp_proc_add_result(proc, "result", mgp_type_string())) return 1;
  return 0;
}

extern "C" int mgp_shutdown_module() {
  return 0;
}
