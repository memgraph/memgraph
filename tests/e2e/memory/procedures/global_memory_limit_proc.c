#include "mg_procedure.h"

int *gVal = NULL;

// void set_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Something went wrong"); }

// void set_out_of_memory_error(struct mgp_result *result) { mgp_result_set_error_msg(result, "Out of memory"); }

// static void error(const struct mgp_list *args, const struct mgp_graph *graph, struct mgp_result *result,
//                   struct mgp_memory *memory) {
//   const size_t one_gb = 1 << 30;
//   if (gVal) {
//     mgp_global_free(gVal);
//     gVal = NULL;
//   }
//   if (!gVal) {
//     gVal = mgp_global_alloc(one_gb);
//     if (!gVal) return set_out_of_memory_error(result);
//   }
//   struct mgp_result_record *record = mgp_result_new_record(result);
//   if (record == NULL) return set_error(result);
//   struct mgp_value *error_value = mgp_value_make_string("ERROR", memory);
//   if (error_value == NULL) return set_error(result);
//   int result_inserted = mgp_result_record_insert(record, "error_result", error_value);
//   mgp_value_destroy(error_value);
//   if (!result_inserted) return set_error(result);
// }

// static void success(const struct mgp_list *args, const struct mgp_graph *graph, struct mgp_result *result,
//                     struct mgp_memory *memory) {
//   const size_t bytes = 1024;
//   if (!gVal) {
//     gVal = mgp_global_alloc(bytes);
//     if (!gVal) set_out_of_memory_error(result);
//   }

//   struct mgp_result_record *record = mgp_result_new_record(result);
//   if (record == NULL) return set_error(result);
//   struct mgp_value *success_value = mgp_value_make_string("sucess", memory);
//   if (success_value == NULL) return set_error(result);
//   int result_inserted = mgp_result_record_insert(record, "success_result", success_value);
//   mgp_value_destroy(success_value);
//   if (!result_inserted) return set_error(result);
// }

// int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
//   struct mgp_proc *error_proc = mgp_module_add_read_procedure(module, "error", error);
//   if (!error_proc) return 1;

//   if (!mgp_proc_add_result(error_proc, "error_result", mgp_type_string())) return 1;

//   struct mgp_proc *succ_proc = mgp_module_add_read_procedure(module, "success", success);
//   if (!succ_proc) return 1;

//   if (!mgp_proc_add_result(succ_proc, "success_result", mgp_type_string())) return 1;

//   return 0;
// }

int mgp_shutdown_module() {
  if (gVal) mgp_global_free(gVal);
  return 0;
}
