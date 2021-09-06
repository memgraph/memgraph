/// @file
/// Provides API for usage in custom openCypher procedures
#ifndef MG_PROCEDURE_H
#define MG_PROCEDURE_H

#ifdef __cplusplus
extern "C" {
#endif

#if __cplusplus >= 201703L
#define MGP_NODISCARD [[nodiscard]]
#else
#define MGP_NODISCARD
#endif

#include <stddef.h>
#include <stdint.h>

/// @name Error Codes
///
///@{

/// All functions return an error code that can be used to figure out whether the API call was successful or not. In
/// case of failure, the specific error code can be used to identify the reason of the failure.
enum MGP_NODISCARD mgp_error {
  MGP_ERROR_NO_ERROR = 0,
  MGP_ERROR_UNKNOWN_ERROR,
  MGP_ERROR_UNABLE_TO_ALLOCATE,
  MGP_ERROR_INSUFFICIENT_BUFFER,
  MGP_ERROR_OUT_OF_RANGE,
  MGP_ERROR_LOGIC_ERROR,
  MGP_ERROR_NON_EXISTENT_OBJECT,
  MGP_ERROR_INVALID_ARGUMENT,
  MGP_ERROR_KEY_ALREADY_EXISTS,
};
///@}

/// @name Memory Allocation
///
/// These should be preferred compared to plain malloc calls as Memgraph's
/// execution will handle allocation and deallocation more efficiently. In
/// addition to efficiency, Memgraph can set the limit on allowed allocations
/// thus providing some safety with regards to memory usage. The allocated
/// memory is only valid during the execution of mgp_main. You must not allocate
/// global resources with these functions and none of the functions are
/// thread-safe, because we provide a single thread of execution when invoking a
/// custom procedure. For allocating global resources, you can use the _global
/// variations of the aforementioned allocators. This allows Memgraph to be
/// more efficient as explained before.
///@{

/// Provides memory managament access and state.
struct mgp_memory;

/// Allocate a block of memory with given size in bytes.
/// Unlike malloc, this function is not thread-safe.
/// `size_in_bytes` must be greater than 0.
/// The resulting pointer must be freed with mgp_free.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_alloc(struct mgp_memory *memory, size_t size_in_bytes, void **result);

/// Allocate an aligned block of memory with given size in bytes.
/// Unlike malloc and aligned_alloc, this function is not thread-safe.
/// `size_in_bytes` must be greater than 0.
/// `alignment` must be a power of 2 value.
/// The resulting pointer must be freed with mgp_free.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_aligned_alloc(struct mgp_memory *memory, size_t size_in_bytes, size_t alignment, void **result);

/// Deallocate an allocation from mgp_alloc or mgp_aligned_alloc.
/// Unlike free, this function is not thread-safe.
/// If `ptr` is NULL, this function does nothing.
/// The behavior is undefined if `ptr` is not a value returned from a prior
/// mgp_alloc or mgp_aligned_alloc call with the corresponding `memory`.
void mgp_free(struct mgp_memory *memory, void *ptr);

/// Allocate a global block of memory with given size in bytes.
/// This function can be used to allocate global memory that persists
/// beyond a single invocation of mgp_main.
/// The resulting pointer must be freed with mgp_global_free.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_global_alloc(size_t size_in_bytes, void **result);

/// Allocate an aligned global block of memory with given size in bytes.
/// This function can be used to allocate global memory that persists
/// beyond a single invocation of mgp_main.
/// The resulting pointer must be freed with mgp_global_free.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_global_aligned_alloc(size_t size_in_bytes, size_t alignment, void **result);

/// Deallocate an allocation from mgp_global_alloc or mgp_global_aligned_alloc.
/// If `ptr` is NULL, this function does nothing.
/// The behavior is undefined if `ptr` is not a value returned from a prior
/// mgp_global_alloc() or mgp_global_aligned_alloc().
void mgp_global_free(void *p);
///@}

/// @name Operations on mgp_value
///
/// struct mgp_value is an immutable container of various values that may appear
/// as arguments and results of a custom procedure. The following functions and
/// types are used to work with mgp_value. Each function returning a non-const
/// mgp_value has allocated a new instance of the result, therefore such
/// mgp_value instances need to be deallocated using mgp_value_destroy.
///@{

/// Immutable container of various values that appear in the query language.
struct mgp_value;

/// List of mgp_value instances
struct mgp_list;

/// Map of character strings to mgp_value instances.
struct mgp_map;

/// Vertex in the graph database.
struct mgp_vertex;

/// Edge in the graph database.
struct mgp_edge;

/// Path containing mgp_vertex and mgp_edge instances.
struct mgp_path;

/// All available types that can be stored in a mgp_value
enum mgp_value_type {
  // NOTE: New types need to be appended, so as not to break ABI.
  MGP_VALUE_TYPE_NULL,
  MGP_VALUE_TYPE_BOOL,
  MGP_VALUE_TYPE_INT,
  MGP_VALUE_TYPE_DOUBLE,
  MGP_VALUE_TYPE_STRING,
  MGP_VALUE_TYPE_LIST,
  MGP_VALUE_TYPE_MAP,
  MGP_VALUE_TYPE_VERTEX,
  MGP_VALUE_TYPE_EDGE,
  MGP_VALUE_TYPE_PATH,
};

/// Free the memory used by the given mgp_value instance.
void mgp_value_destroy(struct mgp_value *val);

/// Construct a value representing `null` in openCypher.
/// You need to free the instance through mgp_value_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_null(struct mgp_memory *memory, struct mgp_value **result);

/// Construct a boolean value.
/// Non-zero values represent `true`, while zero represents `false`.
/// You need to free the instance through mgp_value_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_bool(int val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct an integer value.
/// You need to free the instance through mgp_value_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_int(int64_t val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct a double floating point value.
/// You need to free the instance through mgp_value_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_double(double val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct a character string value from a NULL terminated string.
/// You need to free the instance through mgp_value_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_string(const char *val, struct mgp_memory *memory, struct mgp_value **result);

/// Create a mgp_value storing a mgp_list.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the list is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_list. Therefore, if a mgp_value is successfully created
/// you must not call mgp_list_destroy on the given list.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_list(struct mgp_list *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_map.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the map is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_map. Therefore, if a mgp_value is successfully created
/// you must not call mgp_map_destroy on the given map.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_map(struct mgp_map *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_vertex.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the vertex is given to the created mgp_value and destroying the mgp_value
/// will destroy the mgp_vertex. Therefore, if a mgp_value is successfully
/// created you must not call mgp_vertex_destroy on the given vertex.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_vertex(struct mgp_vertex *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_edge.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the edge is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_edge. Therefore, if a mgp_value is successfully created you
/// must not call mgp_edge_destroy on the given edge.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_edge(struct mgp_edge *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_path.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the path is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_path. Therefore, if a mgp_value is successfully created you
/// must not call mgp_path_destroy on the given path.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_path(struct mgp_path *val, struct mgp_value **result);

/// Get the type of the value contained in mgp_value.
enum mgp_error mgp_value_get_type(const struct mgp_value *val, enum mgp_value_type *result);

/// Result is non-zero if the given mgp_value represents `null`.
enum mgp_error mgp_value_is_null(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a boolean.
enum mgp_error mgp_value_is_bool(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores an integer.
enum mgp_error mgp_value_is_int(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a double floating-point.
enum mgp_error mgp_value_is_double(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a character string.
enum mgp_error mgp_value_is_string(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a list of values.
enum mgp_error mgp_value_is_list(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a map of values.
enum mgp_error mgp_value_is_map(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a vertex.
enum mgp_error mgp_value_is_vertex(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores an edge.
enum mgp_error mgp_value_is_edge(const struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a path.
enum mgp_error mgp_value_is_path(const struct mgp_value *val, int *result);

/// Get the contained boolean value.
/// Non-zero values represent `true`, while zero represents `false`.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_bool(const struct mgp_value *val, int *result);

/// Get the contained integer.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_int(const struct mgp_value *val, int64_t *result);

/// Get the contained double floating-point.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_double(const struct mgp_value *val, double *result);

/// Get the contained character string.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_string(const struct mgp_value *val, const char **result);

/// Get the contained list of values.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_list(const struct mgp_value *val, struct mgp_list **result);

/// Return the contained map of values.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_map(const struct mgp_value *val, struct mgp_map **result);

/// Get the contained vertex.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_vertex(const struct mgp_value *val, struct mgp_vertex **result);

/// Get the contained edge.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_edge(const struct mgp_value *val, struct mgp_edge **result);

/// Get the contained path.
/// Result is undefined if mgp_value does not contain the expected type.
enum mgp_error mgp_value_get_path(const struct mgp_value *val, struct mgp_path **result);

/// Create an empty list with given capacity.
/// You need to free the created instance with mgp_list_destroy.
/// The created list will have allocated enough memory for `capacity` elements
/// of mgp_value, but it will not contain any elements. Therefore,
/// mgp_list_size will return 0.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_list.
enum mgp_error mgp_list_make_empty(size_t capacity, struct mgp_memory *memory, struct mgp_list **result);

/// Free the memory used by the given mgp_list and contained elements.
void mgp_list_destroy(struct mgp_list *list);

/// Append a copy of mgp_value to mgp_list if capacity allows.
/// The list copies the given value and therefore does not take ownership of the
/// original value. You still need to call mgp_value_destroy to free the
/// original value.
/// Return MGP_ERROR_INSUFFICIENT_BUFFER if there's no capacity.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_list_append(struct mgp_list *list, const struct mgp_value *val);

/// Append a copy of mgp_value to mgp_list increasing capacity if needed.
/// The list copies the given value and therefore does not take ownership of the
/// original value. You still need to call mgp_value_destroy to free the
/// original value.
/// In case of a capacity change, the previously contained elements will move in
/// memory and any references to them will be invalid.
/// Return MGP_ERROR_INSUFFICIENT_BUFFER if there's no capacity.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_list_append_extend(struct mgp_list *list, const struct mgp_value *val);

/// Get the number of elements stored in mgp_list.
enum mgp_error mgp_list_size(const struct mgp_list *list, size_t *result);

/// Get the total number of elements for which there's already allocated
/// memoNULLry in mgp_list.
enum mgp_error mgp_list_capacity(const struct mgp_list *list, size_t *result);

/// Get the element in mgp_list at given position.
/// MGP_ERROR_OUT_OF_RANGE is returned if the index is not within mgp_list_size.
enum mgp_error mgp_list_at(struct mgp_list *list, size_t index, struct mgp_value **result);

/// Create an empty map of character strings to mgp_value instances.
/// You need to free the created instance with mgp_map_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_map.
enum mgp_error mgp_map_make_empty(struct mgp_memory *memory, struct mgp_map **result);

/// Free the memory used by the given mgp_map and contained items.
void mgp_map_destroy(struct mgp_map *map);

/// Insert a new mapping from a NULL terminated character string to a value.
/// If a mapping with the same key already exists, it is *not* replaced.
/// In case of insertion, both the string and the value are copied into the map.
/// Therefore, the map does not take ownership of the original key nor value, so
/// you still need to free their memory explicitly.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate for insertion.
/// Return MGP_ERROR_KEY_ALREADY_EXISTS if a previous mapping already exists.
enum mgp_error mgp_map_insert(struct mgp_map *map, const char *key, const struct mgp_value *value);

/// Get the number of items stored in mgp_map.
enum mgp_error mgp_map_size(const struct mgp_map *map, size_t *result);

/// Get the mapped mgp_value to the given character string.
/// Result is NULL if no mapping exists.
enum mgp_error mgp_map_at(struct mgp_map *map, const char *key, struct mgp_value **result);

/// An item in the mgp_map.
struct mgp_map_item;

/// Get the key of the mapped item.
enum mgp_error mgp_map_item_key(const struct mgp_map_item *item, const char **result);

/// Get the value of the mapped item.
enum mgp_error mgp_map_item_value(struct mgp_map_item *item, struct mgp_value **result);

/// An iterator over the items in mgp_map.
struct mgp_map_items_iterator;

/// Start iterating over items contained in the given map.
/// The resulting mgp_map_items_iterator needs to be deallocated with
/// mgp_map_items_iterator_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_map_items_iterator.
enum mgp_error mgp_map_iter_items(const struct mgp_map *map, struct mgp_memory *memory,
                                  struct mgp_map_items_iterator **result);

/// Deallocate memory used by mgp_map_items_iterator.
void mgp_map_items_iterator_destroy(struct mgp_map_items_iterator *it);

/// Get the current item pointed to by the iterator.
/// When the mgp_map_items_iterator_next is invoked, the returned pointer
/// to mgp_map_item becomes invalid. On the other hand, pointers obtained
/// with mgp_map_item_key and mgp_map_item_value remain valid
/// throughout the lifetime of a map. Therefore, you can store the key as well
/// as the value before, and use them after invoking
/// mgp_map_items_iterator_next.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_map_items_iterator_get(const struct mgp_map_items_iterator *it, struct mgp_map_item **result);

/// Advance the iterator to the next item stored in map and return it.
/// The previous pointer obtained through mgp_map_items_iterator_get will
/// be invalidated, but the pointers to key and value will remain valid.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_map_items_iterator_next(struct mgp_map_items_iterator *it, struct mgp_map_item **result);

/// Create a path with the copy of the given starting vertex.
/// You need to free the created instance with mgp_path_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_path.
enum mgp_error mgp_path_make_with_start(const struct mgp_vertex *vertex, struct mgp_memory *memory,
                                        struct mgp_path **result);

/// Copy a mgp_path.
/// Returned pointer must be freed with mgp_path_destroy.
/// MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_path.
enum mgp_error mgp_path_copy(const struct mgp_path *path, struct mgp_memory *memory, struct mgp_path **result);

/// Free the memory used by the given mgp_path and contained vertices and edges.
void mgp_path_destroy(struct mgp_path *path);

/// Append an edge continuing from the last vertex on the path.
/// The edge is copied into the path. Therefore, the path does not take
/// ownership of the original edge, so you still need to free the edge memory
/// explicitly.
/// The last vertex on the path will become the other endpoint of the given
/// edge, as continued from the current last vertex.
/// Return MGP_ERROR_LOGIC_ERROR if the current last vertex in the path is not part of the given edge.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for path extension.
enum mgp_error mgp_path_expand(struct mgp_path *path, struct mgp_edge *edge);

/// Get the number of edges in a mgp_path.
enum mgp_error mgp_path_size(const struct mgp_path *path, size_t *result);

/// Get the vertex from a path at given index.
/// The valid index range is [0, mgp_path_size].
/// MGP_ERROR_OUT_OF_RANGE is returned if index is out of range.
enum mgp_error mgp_path_vertex_at(struct mgp_path *path, size_t index, struct mgp_vertex **result);

/// Get the edge from a path at given index.
/// The valid index range is [0, mgp_path_size - 1].
/// MGP_ERROR_OUT_OF_RANGE is returned if index is out of range.
enum mgp_error mgp_path_edge_at(struct mgp_path *path, size_t index, struct mgp_edge **result);

/// Result is non-zero if given paths are equal, otherwise 0.
enum mgp_error mgp_path_equal(struct mgp_path *p1, struct mgp_path *p2, int *result);

///@}

/// @name Procedure Result
/// These functions and types are used to set the result of your custom
/// procedure.
///@{

/// Stores either an error result or a list of mgp_result_record instances.
struct mgp_result;
/// Represents a record of resulting field values.
struct mgp_result_record;

/// Set the error as the result of the procedure.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE ff there's no memory for copying the error message.
enum mgp_error mgp_result_set_error_msg(struct mgp_result *res, const char *error_msg);

/// Create a new record for results.
/// The previously obtained mgp_result_record pointer is no longer valid, and you must not use it.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_result_record.
enum mgp_error mgp_result_new_record(struct mgp_result *res, struct mgp_result_record **result);

/// Assign a value to a field in the given record.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory to copy the mgp_value to mgp_result_record.
/// Return MGP_ERROR_OUT_OF_RANGE if there is no field named `field_name`.
/// Return MGP_ERROR_LOGIC_ERROR `val` does not satisfy the type of the field name `field_name`.
enum mgp_error mgp_result_record_insert(struct mgp_result_record *record, const char *field_name,
                                        const struct mgp_value *val);
///@}

/// @name Graph Constructs
///@{

/// Label of a vertex.
struct mgp_label {
  /// Name of the label as a NULL terminated character string.
  const char *name;
};

/// Type of an edge.
struct mgp_edge_type {
  /// Name of the type as a NULL terminated character string.
  const char *name;
};

/// Iterator over property values of a vertex or an edge.
struct mgp_properties_iterator;

/// Free the memory used by a mgp_properties_iterator.
void mgp_properties_iterator_destroy(struct mgp_properties_iterator *it);

/// Reference to a named property value.
struct mgp_property {
  /// Name (key) of a property as a NULL terminated string.
  const char *name;
  /// Value of the referenced property.
  const struct mgp_value *value;
};

/// Get the current property pointed to by the iterator.
/// When the mgp_properties_iterator_next is invoked, the previous
/// mgp_property is invalidated and its value must not be used.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_properties_iterator_get(struct mgp_properties_iterator *it, struct mgp_property **result);

/// Advance the iterator to the next property and return it.
/// The previous mgp_property obtained through mgp_properties_iterator_get
/// will be invalidated, and you must not use its value.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_properties_iterator_next(struct mgp_properties_iterator *it, struct mgp_property **result);

/// Iterator over edges of a vertex.
struct mgp_edges_iterator;

/// Free the memory used by a mgp_edges_iterator.
void mgp_edges_iterator_destroy(struct mgp_edges_iterator *it);

/// ID of a vertex; valid during a single query execution.
struct mgp_vertex_id {
  int64_t as_int;
};

/// Get the ID of given vertex.
/// The ID is only valid for a single query execution, you should never store it
/// globally in a query module.
enum mgp_error mgp_vertex_get_id(const struct mgp_vertex *v, struct mgp_vertex_id *result);

/// Copy a mgp_vertex.
/// Resulting pointer must be freed with mgp_vertex_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertex.
enum mgp_error mgp_vertex_copy(const struct mgp_vertex *v, struct mgp_memory *memory, struct mgp_vertex **result);

/// Free the memory used by a mgp_vertex.
void mgp_vertex_destroy(struct mgp_vertex *v);

/// Result is non-zero if given vertices are equal, otherwise 0.
enum mgp_error mgp_vertex_equal(const struct mgp_vertex *v1, const struct mgp_vertex *v2, int *result);

/// Get the number of labels a given vertex has.
enum mgp_error mgp_vertex_labels_count(const struct mgp_vertex *v, size_t *result);

/// Get mgp_label in mgp_vertex at given index.
/// Return MGP_ERROR_OUT_OF_RANGE if the index is out of range.
enum mgp_error mgp_vertex_label_at(const struct mgp_vertex *v, size_t index, struct mgp_label *result);

/// Result is non-zero if the given vertex has the given label.
enum mgp_error mgp_vertex_has_label(const struct mgp_vertex *v, struct mgp_label label, int *result);

/// Result is non-zero if the given vertex has a label with given name.
enum mgp_error mgp_vertex_has_label_named(const struct mgp_vertex *v, const char *label_name, int *result);

/// Get a copy of a vertex property mapped to a given name.
/// Resulting value must be freed with mgp_value_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_vertex_get_property(const struct mgp_vertex *v, const char *property_name, struct mgp_memory *memory,
                                       struct mgp_value **result);

/// Start iterating over properties stored in the given vertex.
/// The resulting mgp_properties_iterator needs to be deallocated with
/// mgp_properties_iterator_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_properties_iterator.
enum mgp_error mgp_vertex_iter_properties(struct mgp_vertex *v, struct mgp_memory *memory,
                                          struct mgp_properties_iterator **result);

/// Start iterating over inbound edges of the given vertex.
/// The resulting mgp_edges_iterator needs to be deallocated with
/// mgp_edges_iterator_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edges_iterator.
enum mgp_error mgp_vertex_iter_in_edges(struct mgp_vertex *v, struct mgp_memory *memory,
                                        struct mgp_edges_iterator **result);

/// Start iterating over outbound edges of the given vertex.
/// The returned mgp_edges_iterator needs to be deallocated with
/// mgp_edges_iterator_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edges_iterator.
enum mgp_error mgp_vertex_iter_out_edges(const struct mgp_vertex *v, struct mgp_memory *memory,
                                         struct mgp_edges_iterator **result);

/// Get the current edge pointed to by the iterator.
/// When the mgp_edges_iterator_next is invoked, the previous
/// mgp_edge is invalidated and its value must not be used.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_edges_iterator_get(struct mgp_edges_iterator *it, struct mgp_edge **result);

/// Advance the iterator to the next edge and return it.
/// The previous mgp_edge obtained through mgp_edges_iterator_get
/// will be invalidated, and you must not use its value.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_edges_iterator_next(struct mgp_edges_iterator *it, struct mgp_edge **result);

/// ID of an edge; valid during a single query execution.
struct mgp_edge_id {
  int64_t as_int;
};

/// Get the ID of given edge.
/// The ID is only valid for a single query execution, you should never store it
/// globally in a query module.
enum mgp_error mgp_edge_get_id(const struct mgp_edge *e, struct mgp_edge_id *result);

/// Copy a mgp_edge.
/// Resulting pointer must be freed with mgp_edge_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edge.
enum mgp_error mgp_edge_copy(const struct mgp_edge *e, struct mgp_memory *memory, struct mgp_edge **result);

/// Free the memory used by a mgp_edge.
void mgp_edge_destroy(struct mgp_edge *e);

/// Result is non-zero if given edges are equal, otherwise 0.
enum mgp_error mgp_edge_equal(const struct mgp_edge *e1, const struct mgp_edge *e2, int *result);

/// Get the type of the given edge.
enum mgp_error mgp_edge_get_type(const struct mgp_edge *e, struct mgp_edge_type *result);

/// Get the source vertex of the given edge.
enum mgp_error mgp_edge_get_from(struct mgp_edge *e, struct mgp_vertex **result);

/// Get the destination vertex of the given edge.
enum mgp_error mgp_edge_get_to(struct mgp_edge *e, struct mgp_vertex **result);

/// Get a copy of a edge property mapped to a given name.
/// Resulting value must be freed with mgp_value_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_edge_get_property(const struct mgp_edge *e, const char *property_name, struct mgp_memory *memory,
                                     struct mgp_value **result);

/// Start iterating over properties stored in the given edge.
/// Resulting mgp_properties_iterator needs to be deallocated with
/// mgp_properties_iterator_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_properties_iterator.
enum mgp_error mgp_edge_iter_properties(const struct mgp_edge *e, struct mgp_memory *memory,
                                        struct mgp_properties_iterator **result);

/// State of the graph database.
struct mgp_graph;

/// Return the vertex corresponding to given ID.
/// Resulting vertex must be freed using mgp_vertex_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the vertex or if ID is not valid.
enum mgp_error mgp_graph_get_vertex_by_id(struct mgp_graph *g, struct mgp_vertex_id id, struct mgp_memory *memory,
                                          struct mgp_vertex **result);

/// Iterator over vertices.
struct mgp_vertices_iterator;

/// Free the memory used by a mgp_vertices_iterator.
void mgp_vertices_iterator_destroy(struct mgp_vertices_iterator *it);

/// Start iterating over vertices of the given graph.
/// Resulting mgp_vertices_iterator needs to be deallocated with mgp_vertices_iterator_destroy.
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertices_iterator.
enum mgp_error mgp_graph_iter_vertices(const struct mgp_graph *g, struct mgp_memory *memory,
                                       struct mgp_vertices_iterator **result);

/// Get the current vertex pointed to by the iterator.
/// When the mgp_vertices_iterator_next is invoked, the previous
/// mgp_vertex is invalidated and its value must not be used.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_vertices_iterator_get(struct mgp_vertices_iterator *it, struct mgp_vertex **result);

/// Advance the iterator to the next vertex and return it.
/// The previous mgp_vertex obtained through mgp_vertices_iterator_get
/// will be invalidated, and you must not use its value.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_vertices_iterator_next(struct mgp_vertices_iterator *it, struct mgp_vertex **result);
///@}

/// @name Type System
///
/// The following structures and functions are used to build a type
/// representation used in openCypher. The primary purpose is to create a
/// procedure signature for use with openCypher. Memgraph will use the built
/// procedure signature to perform various static and dynamic checks when the
/// custom procedure is invoked.
///@{

/// A type for values in openCypher.
struct mgp_type;

/// Get the type representing any value that isn't `null`.
///
/// The ANY type is the parent type of all types.
enum mgp_error mgp_type_any(const struct mgp_type **result);

/// Get the type representing boolean values.
enum mgp_error mgp_type_bool(const struct mgp_type **result);

/// Get the type representing character string values.
enum mgp_error mgp_type_string(const struct mgp_type **result);

/// Get the type representing integer values.
enum mgp_error mgp_type_int(const struct mgp_type **result);

/// Get the type representing floating-point values.
enum mgp_error mgp_type_float(const struct mgp_type **result);

/// Get the type representing any number value.
///
/// This is the parent type for numeric types, i.e. INTEGER and FLOAT.
enum mgp_error mgp_type_number(const struct mgp_type **result);

/// Get the type representing map values.
///
/// Map values are those which map string keys to values of any type. For
/// example `{ database: "Memgraph", version: 1.42 }`. Note that graph nodes
/// contain property maps, so a node value will also satisfy the MAP type. The
/// same applies for graph relationship values.
///
/// @sa mgp_type_node
/// @sa mgp_type_relationship
enum mgp_error mgp_type_map(const struct mgp_type **result);

/// Get the type representing graph node values.
///
/// Since a node contains a map of properties, the node itself is also of MAP
/// type.
enum mgp_error mgp_type_node(const struct mgp_type **result);

/// Get the type representing graph relationship values.
///
/// Since a relationship contains a map of properties, the relationship itself
/// is also of MAP type.
enum mgp_error mgp_type_relationship(const struct mgp_type **result);

/// Get the type representing a graph path (walk) from one node to another.
enum mgp_error mgp_type_path(const struct mgp_type **result);

/// Build a type representing a list of values of given `element_type`.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_list(const struct mgp_type *element_type, const struct mgp_type **result);

/// Build a type representing either a `null` value or a value of given `type`.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_nullable(const struct mgp_type *type, const struct mgp_type **result);
///@}

/// @name Query Module & Procedures
///
/// The following structures and functions are used to build a query module. You
/// will receive an empty instance of mgp_module through your
/// `int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory)`
/// function. Each shared library that wishes to provide a query module needs to
/// have the said function. Inside you can fill the module with procedures,
/// which can then be called through openCypher.
///
/// Arguments to `mgp_init_module` will not live longer than the function's
/// execution, so you must not store them globally. Additionally, you must not
/// use the passed in mgp_memory to allocate global resources.
///@{

/// Stores information on your query module.
struct mgp_module;

/// Describes a procedure of a query module.
struct mgp_proc;

/// Entry-point for a query module procedure, invoked through openCypher.
///
/// Passed in arguments will not live longer than the callback's execution.
/// Therefore, you must not store them globally or use the passed in mgp_memory
/// to allocate global resources.
typedef void (*mgp_proc_cb)(struct mgp_list *, struct mgp_graph *, struct mgp_result *, struct mgp_memory *);

/// Register a read-only procedure with a module.
///
/// The `name` must be a sequence of digits, underscores, lowercase and
/// uppercase Latin letters. The name must begin with a non-digit character.
/// Note that Unicode characters are not allowed. Additionally, names are
/// case-sensitive.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for mgp_proc.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid procedure name.
/// RETURN MGP_ERROR_LOGIC_ERROR if a procedure with the same name was already registered.
enum mgp_error mgp_module_add_read_procedure(struct mgp_module *module, const char *name, mgp_proc_cb cb,
                                             struct mgp_proc **result);

/// Add a required argument to a procedure.
///
/// The order of adding arguments will correspond to the order the procedure
/// must receive them through openCypher. Required arguments will be followed by
/// optional arguments.
///
/// The `name` must be a valid identifier, following the same rules as the
/// procedure`name` in mgp_module_add_read_procedure.
///
/// Passed in `type` describes what kind of values can be used as the argument.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// RETURN MGP_ERROR_LOGIC_ERROR if the procedure already has any optional argument.
enum mgp_error mgp_proc_add_arg(struct mgp_proc *proc, const char *name, const struct mgp_type *type);

/// Add an optional argument with a default value to a procedure.
///
/// The order of adding arguments will correspond to the order the procedure
/// must receive them through openCypher. Optional arguments must follow the
/// required arguments.
///
/// The `name` must be a valid identifier, following the same rules as the
/// procedure `name` in mgp_module_add_read_procedure.
///
/// Passed in `type` describes what kind of values can be used as the argument.
///
/// `default_value` is copied and set as the default value for the argument.
/// Don't forget to call mgp_value_destroy when you are done using
/// `default_value`. When the procedure is called, if this argument is not
/// provided, `default_value` will be used instead. `default_value` must not be
/// a graph element (node, relationship, path) and it must satisfy the given
/// `type`.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// RETURN MGP_ERROR_OUT_OF_RANGE if `default_value` is a graph element (vertex, edge or path).
/// RETURN MGP_ERROR_LOGIC_ERROR if `default_value` does not satisfy `type`.
enum mgp_error mgp_proc_add_opt_arg(struct mgp_proc *proc, const char *name, const struct mgp_type *type,
                                    const struct mgp_value *default_value);

/// Add a result field to a procedure.
///
/// The `name` must be a valid identifier, following the same rules as the
/// procedure `name` in mgp_module_add_read_procedure.
///
/// Passed in `type` describes what kind of values can be returned through the
/// result field.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid result name.
/// RETURN MGP_ERROR_LOGIC_ERROR if a result field with the same name was already added.
enum mgp_error mgp_proc_add_result(struct mgp_proc *proc, const char *name, const struct mgp_type *type);

/// Add a result field to a procedure and mark it as deprecated.
///
/// This is the same as mgp_proc_add_result, but the result field will be marked
/// as deprecated.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid result name.
/// RETURN MGP_ERROR_LOGIC_ERROR if a result field with the same name was already added.
enum mgp_error mgp_proc_add_deprecated_result(struct mgp_proc *proc, const char *name, const struct mgp_type *type);
///@}

/// @name Execution
///
/// The following functions are used to control the execution of the procedure.
///
/// @{

/// Return non-zero if the currently executing procedure should abort as soon as
/// possible.
///
/// Procedures which perform heavyweight processing run the risk of running too
/// long and going over the query execution time limit. To prevent this, such
/// procedures should periodically call this function at critical points in
/// their code in order to determine whether they should abort or not. Note that
/// this mechanism is purely cooperative and depends on the procedure doing the
/// checking and aborting on its own.
enum mgp_error mgp_must_abort(const struct mgp_graph *graph, int *result);

/// @}

/// @name Kafka message API
/// Currently the API below is for kafka only but in the future
/// mgp_message and mgp_messages might be generic to support
/// other streaming systems.
///@{

/// A single Kafka message
struct mgp_message;

/// A list of Kafka messages
struct mgp_messages;

/// Payload is not null terminated and not a string but rather a byte array.
/// You need to call mgp_message_payload_size() first, to read the size of
/// the payload.
enum mgp_error mgp_message_payload(const struct mgp_message *message, const char **result);

/// Return the payload size
enum mgp_error mgp_message_payload_size(const struct mgp_message *message, size_t *result);

/// Return the name of topic
enum mgp_error mgp_message_topic_name(const struct mgp_message *message, const char **result);

/// Return the key of mgp_message as a byte array
enum mgp_error mgp_message_key(const struct mgp_message *message, const char **result);

/// Return the key size of mgp_message
enum mgp_error mgp_message_key_size(const struct mgp_message *message, size_t *result);

/// Return the timestamp of mgp_message as a byte array
enum mgp_error mgp_message_timestamp(const struct mgp_message *message, int64_t *result);

/// Return the number of messages contained in the mgp_messages list
enum mgp_error mgp_messages_size(const struct mgp_messages *message, size_t *result);

/// Return the message from a messages list at given index
enum mgp_error mgp_messages_at(const struct mgp_messages *message, size_t index, const struct mgp_message **result);

/// Entry-point for a module transformation, invoked through a stream transformation.
///
/// Passed in arguments will not live longer than the callback's execution.
/// Therefore, you must not store them globally or use the passed in mgp_memory
/// to allocate global resources.
typedef void (*mgp_trans_cb)(const struct mgp_messages *, struct mgp_graph *, struct mgp_result *, struct mgp_memory *);

/// Register a transformation with a module.
///
/// The `name` must be a sequence of digits, underscores, lowercase and
/// uppercase Latin letters. The name must begin with a non-digit character.
/// Note that Unicode characters are not allowed. Additionally, names are
/// case-sensitive.
///
/// Return MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for transformation.
/// Return MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid transformation name.
/// RETURN MGP_ERROR_LOGIC_ERROR if a transformation with the same name was already registered.
enum mgp_error mgp_module_add_transformation(struct mgp_module *module, const char *name, mgp_trans_cb cb);
/// @}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // MG_PROCEDURE_H
