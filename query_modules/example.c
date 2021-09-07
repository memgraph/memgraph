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
// In case of memory errors, this function will report them and finish
// executing.
//
// The procedure can be invoked in openCypher using the following calls:
//   CALL example.procedure(1, 2) YIELD args, result;
//   CALL example.procedure(1) YIELD args, result;
// Naturally, you may pass in different arguments or yield less fields.
static void procedure(const struct mgp_list *args, const struct mgp_graph *graph, struct mgp_result *result,
                      struct mgp_memory *memory) {
  size_t args_size = 0;
  if (mgp_list_size(args, &args_size) != MGP_ERROR_NO_ERROR) {
    goto error_something_went_wrong;
  }
  struct mgp_list *args_copy = NULL;
  if (mgp_list_make_empty(args_size, memory, &args_copy) != MGP_ERROR_NO_ERROR) {
    goto error_something_went_wrong;
  }
  for (size_t i = 0; i < args_size; ++i) {
    const struct mgp_value *value = NULL;
    if (mgp_list_at(args, i, &value) != MGP_ERROR_NO_ERROR) {
      goto error_free_list;
    }
    if (mgp_list_append(args_copy, value) != MGP_ERROR_NO_ERROR) {
      goto error_free_list;
    }
  }
  struct mgp_result_record *record = NULL;
  if (mgp_result_new_record(result, &record) != MGP_ERROR_NO_ERROR) {
    goto error_free_list;
  }
  // Transfer ownership of args_copy to mgp_value.
  struct mgp_value *args_value = NULL;
  if (mgp_value_make_list(args_copy, &args_value) != MGP_ERROR_NO_ERROR) {
    goto error_free_list;
  }
  // Release `args_value` and contained `args_copy`.
  if (mgp_result_record_insert(record, "args", args_value) != MGP_ERROR_NO_ERROR) {
    mgp_value_destroy(args_value);
    goto error_something_went_wrong;
  }
  mgp_value_destroy(args_value);
  struct mgp_value *hello_world_value = NULL;
  if (mgp_value_make_string("Hello World!", memory, &hello_world_value) != MGP_ERROR_NO_ERROR) {
    goto error_something_went_wrong;
  }
  if (mgp_result_record_insert(record, "result", hello_world_value) != MGP_ERROR_NO_ERROR) {
    mgp_value_destroy(hello_world_value);
    goto error_something_went_wrong;
  }
  mgp_value_destroy(hello_world_value);
  // We have successfully finished, so return without error reporting.
  return;

error_free_list:
  mgp_list_destroy(args_copy);
error_something_went_wrong:
  mgp_result_set_error_msg(result, "Something went wrong!");
  return;
}

static void write_procedure(const struct mgp_list *args, struct mgp_graph *graph, struct mgp_result *result,
                            struct mgp_memory *memory) {
  // TODO(antaljanosbenjamin): Finish this example
}

// Each module needs to define mgp_init_module function.
// Here you can register multiple procedures your module supports.
int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  struct mgp_proc *proc = NULL;
  if (mgp_module_add_read_procedure(module, "procedure", procedure, &proc) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  const struct mgp_type *any_type = NULL;
  if (mgp_type_any(&any_type) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  const struct mgp_type *nullable_any_type = NULL;
  if (mgp_type_nullable(any_type, &nullable_any_type) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  if (mgp_proc_add_arg(proc, "required_arg", nullable_any_type) != MGP_ERROR_NO_ERROR) {
    return 1;
  }

  struct mgp_value *null_value = NULL;
  if (mgp_value_make_null(memory, &null_value) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  if (mgp_proc_add_opt_arg(proc, "optional_arg", nullable_any_type, null_value) != MGP_ERROR_NO_ERROR) {
    mgp_value_destroy(null_value);
    return 1;
  }
  mgp_value_destroy(null_value);
  const struct mgp_type *string = NULL;
  if (mgp_type_string(&string) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  if (mgp_proc_add_result(proc, "result", string) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  const struct mgp_type *list_of_anything = NULL;
  if (mgp_type_list(nullable_any_type, &list_of_anything) != MGP_ERROR_NO_ERROR) {
    return 1;
  }
  if (mgp_proc_add_result(proc, "args", list_of_anything)) {
    return 1;
  }
  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
int mgp_shutdown_module() {
  // Return 0 to indicate success.
  return 0;
}
