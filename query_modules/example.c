// Compile with clang or gcc:
// clang -Wall -shared -fPIC -I <path-to-memgraph-include> example.c -o example.so
// <path-to-memgraph-include> for installed Memgraph will usually be something
// like `/usr/include/memgraph` or `/usr/local/include/memgraph`.
// To use the compiled module, you need to run Memgraph configured to load
// modules from the directory where the compiled module can be found.
#include "mg_procedure.h"

// This example procedure returns 2 fields: `args` and `result`.
//   * `args` is a copy of arguments passed to the procedure.
//   * `result` is the result of this procedure, a "Hello World!" string.
// In case of memory errors, this function will report them and finish executing.
//
// The procedure can be invoked in openCypher using the following call:
//   CALL example(1, 2, 3) YIELD args, result;
// Naturally, you may pass in different arguments or yield less fields.
void mgp_main(const struct mgp_list *args, const struct mgp_graph *graph,
              struct mgp_result *result, struct mgp_memory *memory) {
  struct mgp_list *args_copy = mgp_list_make_empty(mgp_list_size(args), memory);
  if (args_copy == NULL) goto error_memory;
  for (size_t i = 0; i < mgp_list_size(args); ++i) {
    int success = mgp_list_append(args_copy, mgp_list_at(args, i));
    if (!success) goto error_free_list;
  }
  struct mgp_result_record *record = mgp_result_new_record(result);
  if (record == NULL) goto error_free_list;
  // Transfer ownership of args_copy to mgp_value.
  struct mgp_value *args_value = mgp_value_make_list(args_copy);
  if (args_value == NULL) goto error_free_list;
  int args_inserted = mgp_result_record_insert(record, "args", args_value);
  // Release `args_value` and contained `args_copy`.
  mgp_value_destroy(args_value);
  if (!args_inserted) goto error_memory;
  struct mgp_value *hello_world_value =
      mgp_value_make_string("Hello World!", memory);
  if (hello_world_value == NULL) goto error_memory;
  int result_inserted =
      mgp_result_record_insert(record, "result", hello_world_value);
  mgp_value_destroy(hello_world_value);
  if (!result_inserted) goto error_memory;
  // We have successfully finished, so return without error reporting.
  return;

error_free_list:
  mgp_list_destroy(args_copy);
error_memory:
  mgp_result_set_error_msg(result, "Not enough memory!");
  return;
}

// This is an optional function if you need to initialize any global state when
// your module is loaded.
int mgp_init_module() {
  // Return 0 to indicate success.
  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
int mgp_shutdown_module() {
  // Return 0 to indicate success.
  return 0;
}
