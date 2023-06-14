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

/// @file
/// Provides API for usage in custom openCypher procedures
#ifndef MG_PROCEDURE_H
#define MG_PROCEDURE_H

#ifdef __cplusplus
extern "C" {
#define MGP_ENUM_CLASS enum class
#else
#define MGP_ENUM_CLASS enum
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
MGP_ENUM_CLASS MGP_NODISCARD mgp_error{
    MGP_ERROR_NO_ERROR,
    MGP_ERROR_UNKNOWN_ERROR,
    MGP_ERROR_UNABLE_TO_ALLOCATE,
    MGP_ERROR_INSUFFICIENT_BUFFER,
    MGP_ERROR_OUT_OF_RANGE,
    MGP_ERROR_LOGIC_ERROR,
    MGP_ERROR_DELETED_OBJECT,
    MGP_ERROR_INVALID_ARGUMENT,
    MGP_ERROR_KEY_ALREADY_EXISTS,
    MGP_ERROR_IMMUTABLE_OBJECT,
    MGP_ERROR_VALUE_CONVERSION,
    MGP_ERROR_SERIALIZATION_ERROR,
    MGP_ERROR_AUTHORIZATION_ERROR,
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

/// Provides memory management access and state.
struct mgp_memory;

/// Allocate a block of memory with given size in bytes.
/// Unlike malloc, this function is not thread-safe.
/// `size_in_bytes` must be greater than 0.
/// The resulting pointer must be freed with mgp_free.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_alloc(struct mgp_memory *memory, size_t size_in_bytes, void **result);

/// Allocate an aligned block of memory with given size in bytes.
/// Unlike malloc and aligned_alloc, this function is not thread-safe.
/// `size_in_bytes` must be greater than 0.
/// `alignment` must be a power of 2 value.
/// The resulting pointer must be freed with mgp_free.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
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
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
enum mgp_error mgp_global_alloc(size_t size_in_bytes, void **result);

/// Allocate an aligned global block of memory with given size in bytes.
/// This function can be used to allocate global memory that persists
/// beyond a single invocation of mgp_main.
/// The resulting pointer must be freed with mgp_global_free.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to serve the requested allocation.
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

/// Date stored in Memgraph.
struct mgp_date;

/// Local time stored in Memgraph.
struct mgp_local_time;

/// Local date-time stored in Memgraph.
struct mgp_local_date_time;

/// Duration stored in Memgraph.
struct mgp_duration;

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
  MGP_VALUE_TYPE_DATE,
  MGP_VALUE_TYPE_LOCAL_TIME,
  MGP_VALUE_TYPE_LOCAL_DATE_TIME,
  MGP_VALUE_TYPE_DURATION,
};

enum mgp_error mgp_value_copy(struct mgp_value *val, struct mgp_memory *memory, struct mgp_value **result);

/// Free the memory used by the given mgp_value instance.
void mgp_value_destroy(struct mgp_value *val);

/// Construct a value representing `null` in openCypher.
/// You need to free the instance through mgp_value_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_null(struct mgp_memory *memory, struct mgp_value **result);

/// Construct a boolean value.
/// Non-zero values represent `true`, while zero represents `false`.
/// You need to free the instance through mgp_value_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_bool(int val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct an integer value.
/// You need to free the instance through mgp_value_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_int(int64_t val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct a double floating point value.
/// You need to free the instance through mgp_value_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_double(double val, struct mgp_memory *memory, struct mgp_value **result);

/// Construct a character string value from a NULL terminated string.
/// You need to free the instance through mgp_value_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_string(const char *val, struct mgp_memory *memory, struct mgp_value **result);

/// Create a mgp_value storing a mgp_list.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the list is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_list. Therefore, if a mgp_value is successfully created
/// you must not call mgp_list_destroy on the given list.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_list(struct mgp_list *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_map.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the map is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_map. Therefore, if a mgp_value is successfully created
/// you must not call mgp_map_destroy on the given map.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_map(struct mgp_map *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_vertex.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the vertex is given to the created mgp_value and destroying the mgp_value
/// will destroy the mgp_vertex. Therefore, if a mgp_value is successfully
/// created you must not call mgp_vertex_destroy on the given vertex.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_vertex(struct mgp_vertex *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_edge.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the edge is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_edge. Therefore, if a mgp_value is successfully created you
/// must not call mgp_edge_destroy on the given edge.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_edge(struct mgp_edge *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_path.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the path is given to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_path. Therefore, if a mgp_value is successfully created you
/// must not call mgp_path_destroy on the given path.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_path(struct mgp_path *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_date.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the date is transferred to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_date. Therefore, if a mgp_value is successfully created you
/// must not call mgp_date_destroy on the given date.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_date(struct mgp_date *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_local_time.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the local time is transferred to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_local_time. Therefore, if a mgp_value is successfully created you
/// must not call mgp_local_time_destroy on the given local time.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_local_time(struct mgp_local_time *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_local_date_time.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the local date-time is transferred to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_local_date_time. Therefore, if a mgp_value is successfully created you
/// must not call mgp_local_date_time_destroy on the given local date-time.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_local_date_time(struct mgp_local_date_time *val, struct mgp_value **result);

/// Create a mgp_value storing a mgp_duration.
/// You need to free the instance through mgp_value_destroy. The ownership of
/// the duration is transferred to the created mgp_value and destroying the mgp_value will
/// destroy the mgp_duration. Therefore, if a mgp_value is successfully created you
/// must not call mgp_duration_destroy on the given duration.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_value.
enum mgp_error mgp_value_make_duration(struct mgp_duration *val, struct mgp_value **result);

/// Get the type of the value contained in mgp_value.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_type(struct mgp_value *val, enum mgp_value_type *result);

/// Result is non-zero if the given mgp_value represents `null`.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_null(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a boolean.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_bool(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores an integer.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_int(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a double floating-point.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_double(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a character string.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_string(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a list of values.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_list(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a map of values.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_map(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a vertex.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_vertex(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores an edge.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_edge(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a path.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_path(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a date.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_date(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a local time.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_local_time(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a local date-time.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_local_date_time(struct mgp_value *val, int *result);

/// Result is non-zero if the given mgp_value stores a duration.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_is_duration(struct mgp_value *val, int *result);

/// Get the contained boolean value.
/// Non-zero values represent `true`, while zero represents `false`.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_bool(struct mgp_value *val, int *result);

/// Get the contained integer.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_int(struct mgp_value *val, int64_t *result);

/// Get the contained double floating-point.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_double(struct mgp_value *val, double *result);

/// Get the contained character string.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_string(struct mgp_value *val, const char **result);

/// Get the contained list of values.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_list(struct mgp_value *val, struct mgp_list **result);

/// Get the contained map of values.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_map(struct mgp_value *val, struct mgp_map **result);

/// Get the contained vertex.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_vertex(struct mgp_value *val, struct mgp_vertex **result);

/// Get the contained edge.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_edge(struct mgp_value *val, struct mgp_edge **result);

/// Get the contained path.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_path(struct mgp_value *val, struct mgp_path **result);

/// Get the contained date.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_date(struct mgp_value *val, struct mgp_date **result);

/// Get the contained local time.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_local_time(struct mgp_value *val, struct mgp_local_time **result);

/// Get the contained local date-time.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_local_date_time(struct mgp_value *val, struct mgp_local_date_time **result);

/// Get the contained duration.
/// Result is undefined if mgp_value does not contain the expected type.
/// Current implementation always returns without errors.
enum mgp_error mgp_value_get_duration(struct mgp_value *val, struct mgp_duration **result);

/// Create an empty list with given capacity.
/// You need to free the created instance with mgp_list_destroy.
/// The created list will have allocated enough memory for `capacity` elements
/// of mgp_value, but it will not contain any elements. Therefore,
/// mgp_list_size will return 0.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_list.
enum mgp_error mgp_list_make_empty(size_t capacity, struct mgp_memory *memory, struct mgp_list **result);

enum mgp_error mgp_list_copy(struct mgp_list *list, struct mgp_memory *memory, struct mgp_list **result);

/// Free the memory used by the given mgp_list and contained elements.
void mgp_list_destroy(struct mgp_list *list);

/// Append a copy of mgp_value to mgp_list if capacity allows.
/// The list copies the given value and therefore does not take ownership of the
/// original value. You still need to call mgp_value_destroy to free the
/// original value.
/// Return mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER if there's no capacity.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_list_append(struct mgp_list *list, struct mgp_value *val);

/// Append a copy of mgp_value to mgp_list increasing capacity if needed.
/// The list copies the given value and therefore does not take ownership of the
/// original value. You still need to call mgp_value_destroy to free the
/// original value.
/// In case of a capacity change, the previously contained elements will move in
/// memory and any references to them will be invalid.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
enum mgp_error mgp_list_append_extend(struct mgp_list *list, struct mgp_value *val);

/// Get the number of elements stored in mgp_list.
/// Current implementation always returns without errors.
enum mgp_error mgp_list_size(struct mgp_list *list, size_t *result);

/// Get the total number of elements for which there's already allocated
/// memory in mgp_list.
/// Current implementation always returns without errors.
enum mgp_error mgp_list_capacity(struct mgp_list *list, size_t *result);

/// Get the element in mgp_list at given position.
/// mgp_error::MGP_ERROR_OUT_OF_RANGE is returned if the index is not within mgp_list_size.
enum mgp_error mgp_list_at(struct mgp_list *list, size_t index, struct mgp_value **result);

/// Create an empty map of character strings to mgp_value instances.
/// You need to free the created instance with mgp_map_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_map.
enum mgp_error mgp_map_make_empty(struct mgp_memory *memory, struct mgp_map **result);

enum mgp_error mgp_map_copy(struct mgp_map *map, struct mgp_memory *memory, struct mgp_map **result);

/// Free the memory used by the given mgp_map and contained items.
void mgp_map_destroy(struct mgp_map *map);

/// Insert a new mapping from a NULL terminated character string to a value.
/// If a mapping with the same key already exists, it is *not* replaced.
/// In case of insertion, both the string and the value are copied into the map.
/// Therefore, the map does not take ownership of the original key nor value, so
/// you still need to free their memory explicitly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate for insertion.
/// Return mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS if a previous mapping already exists.
enum mgp_error mgp_map_insert(struct mgp_map *map, const char *key, struct mgp_value *value);

/// Get the number of items stored in mgp_map.
/// Current implementation always returns without errors.
enum mgp_error mgp_map_size(struct mgp_map *map, size_t *result);

/// Get the mapped mgp_value to the given character string.
/// Result is NULL if no mapping exists.
enum mgp_error mgp_map_at(struct mgp_map *map, const char *key, struct mgp_value **result);

/// An item in the mgp_map.
struct mgp_map_item;

/// Get the key of the mapped item.
enum mgp_error mgp_map_item_key(struct mgp_map_item *item, const char **result);

/// Get the value of the mapped item.
enum mgp_error mgp_map_item_value(struct mgp_map_item *item, struct mgp_value **result);

/// An iterator over the items in mgp_map.
struct mgp_map_items_iterator;

/// Start iterating over items contained in the given map.
/// The resulting mgp_map_items_iterator needs to be deallocated with
/// mgp_map_items_iterator_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_map_items_iterator.
enum mgp_error mgp_map_iter_items(struct mgp_map *map, struct mgp_memory *memory,
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
enum mgp_error mgp_map_items_iterator_get(struct mgp_map_items_iterator *it, struct mgp_map_item **result);

/// Advance the iterator to the next item stored in map and return it.
/// The previous pointer obtained through mgp_map_items_iterator_get will
/// be invalidated, but the pointers to key and value will remain valid.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_map_items_iterator_next(struct mgp_map_items_iterator *it, struct mgp_map_item **result);

/// Create a path with the copy of the given starting vertex.
/// You need to free the created instance with mgp_path_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_path.
enum mgp_error mgp_path_make_with_start(struct mgp_vertex *vertex, struct mgp_memory *memory, struct mgp_path **result);

/// Copy a mgp_path.
/// Returned pointer must be freed with mgp_path_destroy.
/// mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE is returned if unable to allocate a mgp_path.
enum mgp_error mgp_path_copy(struct mgp_path *path, struct mgp_memory *memory, struct mgp_path **result);

/// Free the memory used by the given mgp_path and contained vertices and edges.
void mgp_path_destroy(struct mgp_path *path);

/// Append an edge continuing from the last vertex on the path.
/// The edge is copied into the path. Therefore, the path does not take
/// ownership of the original edge, so you still need to free the edge memory
/// explicitly.
/// The last vertex on the path will become the other endpoint of the given
/// edge, as continued from the current last vertex.
/// Return mgp_error::MGP_ERROR_LOGIC_ERROR if the current last vertex in the path is not part of the given edge.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for path extension.
enum mgp_error mgp_path_expand(struct mgp_path *path, struct mgp_edge *edge);

/// Get the number of edges in a mgp_path.
/// Current implementation always returns without errors.
enum mgp_error mgp_path_size(struct mgp_path *path, size_t *result);

/// Get the vertex from a path at given index.
/// The valid index range is [0, mgp_path_size].
/// mgp_error::MGP_ERROR_OUT_OF_RANGE is returned if index is out of range.
enum mgp_error mgp_path_vertex_at(struct mgp_path *path, size_t index, struct mgp_vertex **result);

/// Get the edge from a path at given index.
/// The valid index range is [0, mgp_path_size - 1].
/// mgp_error::MGP_ERROR_OUT_OF_RANGE is returned if index is out of range.
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
/// Represents a return type for magic functions
struct mgp_func_result;

/// Set the error as the result of the procedure.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE ff there's no memory for copying the error message.
enum mgp_error mgp_result_set_error_msg(struct mgp_result *res, const char *error_msg);

/// Create a new record for results.
/// The previously obtained mgp_result_record pointer is no longer valid, and you must not use it.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_result_record.
enum mgp_error mgp_result_new_record(struct mgp_result *res, struct mgp_result_record **result);

/// Assign a value to a field in the given record.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory to copy the mgp_value to
/// mgp_result_record. Return mgp_error::MGP_ERROR_OUT_OF_RANGE if there is no field named `field_name`. Return
/// mgp_error::MGP_ERROR_LOGIC_ERROR `val` does not satisfy the type of the field name `field_name`.
enum mgp_error mgp_result_record_insert(struct mgp_result_record *record, const char *field_name,
                                        struct mgp_value *val);
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
  struct mgp_value *value;
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
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_property.
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
enum mgp_error mgp_vertex_get_id(struct mgp_vertex *v, struct mgp_vertex_id *result);

/// Result is non-zero if the vertex can be modified.
/// The mutability of the vertex is the same as the graph which it is part of. If a vertex is immutable, then edges
/// cannot be created or deleted, properties and labels cannot be set or removed and all of the returned edges will be
/// immutable also.
/// Current implementation always returns without errors.
enum mgp_error mgp_vertex_underlying_graph_is_mutable(struct mgp_vertex *v, int *result);

/// Set the value of a property on a vertex.
/// When the value is `null`, then the property is removed from the vertex.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for storing the property.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `v` is immutable.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `v` has been modified by another transaction.
/// Return mgp_error::MGP_ERROR_VALUE_CONVERSION if `property_value` is vertex, edge or path.
enum mgp_error mgp_vertex_set_property(struct mgp_vertex *v, const char *property_name,
                                       struct mgp_value *property_value);

/// Add the label to the vertex.
/// If the vertex already has the label, this function does nothing.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for storing the label.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `v` is immutable.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `v` has been modified by another transaction.
enum mgp_error mgp_vertex_add_label(struct mgp_vertex *v, struct mgp_label label);

/// Remove the label from the vertex.
/// If the vertex doesn't have the label, this function does nothing.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `v` is immutable.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `v` has been modified by another transaction.
enum mgp_error mgp_vertex_remove_label(struct mgp_vertex *v, struct mgp_label label);

/// Copy a mgp_vertex.
/// Resulting pointer must be freed with mgp_vertex_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertex.
enum mgp_error mgp_vertex_copy(struct mgp_vertex *v, struct mgp_memory *memory, struct mgp_vertex **result);

/// Free the memory used by a mgp_vertex.
void mgp_vertex_destroy(struct mgp_vertex *v);

/// Result is non-zero if given vertices are equal, otherwise 0.
enum mgp_error mgp_vertex_equal(struct mgp_vertex *v1, struct mgp_vertex *v2, int *result);

/// Get the number of labels a given vertex has.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_labels_count(struct mgp_vertex *v, size_t *result);

/// Get mgp_label in mgp_vertex at given index.
/// Return mgp_error::MGP_ERROR_OUT_OF_RANGE if the index is out of range.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_label_at(struct mgp_vertex *v, size_t index, struct mgp_label *result);

/// Result is non-zero if the given vertex has the given label.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_has_label(struct mgp_vertex *v, struct mgp_label label, int *result);

/// Result is non-zero if the given vertex has a label with given name.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_has_label_named(struct mgp_vertex *v, const char *label_name, int *result);

/// Get a copy of a vertex property mapped to a given name.
/// Resulting value must be freed with mgp_value_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_get_property(struct mgp_vertex *v, const char *property_name, struct mgp_memory *memory,
                                       struct mgp_value **result);

/// Start iterating over properties stored in the given vertex.
/// The properties of the vertex are copied when the iterator is created, therefore later changes won't affect them.
/// The resulting mgp_properties_iterator needs to be deallocated with
/// mgp_properties_iterator_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_properties_iterator.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_iter_properties(struct mgp_vertex *v, struct mgp_memory *memory,
                                          struct mgp_properties_iterator **result);

/// Start iterating over inbound edges of the given vertex.
/// The connection information of the vertex is copied when the iterator is created, therefore later creation or
/// deletion of edges won't affect the iterated edges, however the property changes on the edges will be visible.
/// The resulting mgp_edges_iterator needs to be deallocated with mgp_edges_iterator_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edges_iterator.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_iter_in_edges(struct mgp_vertex *v, struct mgp_memory *memory,
                                        struct mgp_edges_iterator **result);

/// Start iterating over outbound edges of the given vertex.
/// The connection information of the vertex is copied when the iterator is created, therefore later creation or
/// deletion of edges won't affect the iterated edges, however the property changes on the edges will be visible.
/// The resulting mgp_edges_iterator needs to be deallocated with mgp_edges_iterator_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edges_iterator.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `v` has been deleted.
enum mgp_error mgp_vertex_iter_out_edges(struct mgp_vertex *v, struct mgp_memory *memory,
                                         struct mgp_edges_iterator **result);

/// Result is non-zero if the edges returned by this iterator can be modified.
/// The mutability of the mgp_edges_iterator is the same as the graph which it belongs to.
/// Current implementation always returns without errors.
enum mgp_error mgp_edges_iterator_underlying_graph_is_mutable(struct mgp_edges_iterator *it, int *result);

/// Get the current edge pointed to by the iterator.
/// When the mgp_edges_iterator_next is invoked, the previous
/// mgp_edge is invalidated and its value must not be used.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_edges_iterator_get(struct mgp_edges_iterator *it, struct mgp_edge **result);

/// Advance the iterator to the next edge and return it.
/// The previous mgp_edge obtained through mgp_edges_iterator_get
/// will be invalidated, and you must not use its value.
/// Result is NULL if the end of the iteration has been reached.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edge.
enum mgp_error mgp_edges_iterator_next(struct mgp_edges_iterator *it, struct mgp_edge **result);

/// ID of an edge; valid during a single query execution.
struct mgp_edge_id {
  int64_t as_int;
};

/// Get the ID of given edge.
enum mgp_error mgp_edge_get_id(struct mgp_edge *e, struct mgp_edge_id *result);

/// Result is non-zero if the edge can be modified.
/// The mutability of the edge is the same as the graph which it is part of. If an edge is immutable, properties cannot
/// be set or removed and all of the returned vertices will be immutable also.
/// Current implementation always returns without errors.
enum mgp_error mgp_edge_underlying_graph_is_mutable(struct mgp_edge *e, int *result);

/// Copy a mgp_edge.
/// Resulting pointer must be freed with mgp_edge_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edge.
enum mgp_error mgp_edge_copy(struct mgp_edge *e, struct mgp_memory *memory, struct mgp_edge **result);

/// Free the memory used by a mgp_edge.
void mgp_edge_destroy(struct mgp_edge *e);

/// Result is non-zero if given edges are equal, otherwise 0.
enum mgp_error mgp_edge_equal(struct mgp_edge *e1, struct mgp_edge *e2, int *result);

/// Get the type of the given edge.
enum mgp_error mgp_edge_get_type(struct mgp_edge *e, struct mgp_edge_type *result);

/// Get the source vertex of the given edge.
/// Resulting vertex is valid until the edge is valid and it must not be used afterwards.
/// Current implementation always returns without errors.
enum mgp_error mgp_edge_get_from(struct mgp_edge *e, struct mgp_vertex **result);

/// Get the destination vertex of the given edge.
/// Resulting vertex is valid until the edge is valid and it must not be used afterwards.
/// Current implementation always returns without errors.
enum mgp_error mgp_edge_get_to(struct mgp_edge *e, struct mgp_vertex **result);

/// Get a copy of a edge property mapped to a given name.
/// Resulting value must be freed with mgp_value_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_value.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `e` has been deleted.
enum mgp_error mgp_edge_get_property(struct mgp_edge *e, const char *property_name, struct mgp_memory *memory,
                                     struct mgp_value **result);

/// Set the value of a property on an edge.
/// When the value is `null`, then the property is removed from the edge.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for storing the property.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `e` is immutable.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `e` has been deleted.
/// Return mgp_error::MGP_ERROR_LOGIC_ERROR if properties on edges are disabled.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `e` has been modified by another transaction.
/// Return mgp_error::MGP_ERROR_VALUE_CONVERSION if `property_value` is vertex, edge or path.
enum mgp_error mgp_edge_set_property(struct mgp_edge *e, const char *property_name, struct mgp_value *property_value);

/// Start iterating over properties stored in the given edge.
/// The properties of the edge are copied when the iterator is created, therefore later changes won't affect them.
/// Resulting mgp_properties_iterator needs to be deallocated with
/// mgp_properties_iterator_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_properties_iterator.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `e` has been deleted.
enum mgp_error mgp_edge_iter_properties(struct mgp_edge *e, struct mgp_memory *memory,
                                        struct mgp_properties_iterator **result);

/// State of the graph database.
struct mgp_graph;

/// Get the vertex corresponding to given ID, or NULL if no such vertex exists.
/// Resulting vertex must be freed using mgp_vertex_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the vertex.
enum mgp_error mgp_graph_get_vertex_by_id(struct mgp_graph *g, struct mgp_vertex_id id, struct mgp_memory *memory,
                                          struct mgp_vertex **result);

/// Result is non-zero if the graph can be modified.
/// If a graph is immutable, then vertices cannot be created or deleted, and all of the returned vertices will be
/// immutable also. The same applies for edges.
/// Current implementation always returns without errors.
enum mgp_error mgp_graph_is_mutable(struct mgp_graph *graph, int *result);

/// Add a new vertex to the graph.
/// Resulting vertex must be freed using mgp_vertex_destroy.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `graph` is immutable.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertex.
enum mgp_error mgp_graph_create_vertex(struct mgp_graph *graph, struct mgp_memory *memory, struct mgp_vertex **result);

/// Delete a vertex from the graph.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `graph` is immutable.
/// Return mgp_error::MGP_ERROR_LOGIC_ERROR if `vertex` has edges.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `vertex` has been modified by another transaction.
enum mgp_error mgp_graph_delete_vertex(struct mgp_graph *graph, struct mgp_vertex *vertex);

/// Delete a vertex and all of its edges from the graph.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `graph` is immutable.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `vertex` has been modified by another transaction.
enum mgp_error mgp_graph_detach_delete_vertex(struct mgp_graph *graph, struct mgp_vertex *vertex);

/// Add a new directed edge between the two vertices with the specified label.
/// Resulting edge must be freed using mgp_edge_destroy.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `graph` is immutable.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_edge.
/// Return mgp_error::MGP_ERROR_DELETED_OBJECT if `from` or `to` has been deleted.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `from` or `to` has been modified by another transaction.
enum mgp_error mgp_graph_create_edge(struct mgp_graph *graph, struct mgp_vertex *from, struct mgp_vertex *to,
                                     struct mgp_edge_type type, struct mgp_memory *memory, struct mgp_edge **result);

/// Delete an edge from the graph.
/// Return mgp_error::MGP_ERROR_IMMUTABLE_OBJECT if `graph` is immutable.
/// Return mgp_error::MGP_ERROR_SERIALIZATION_ERROR if `edge`, its source or destination vertex has been modified by
/// another transaction.
enum mgp_error mgp_graph_delete_edge(struct mgp_graph *graph, struct mgp_edge *edge);

/// Iterator over vertices.
struct mgp_vertices_iterator;

/// Free the memory used by a mgp_vertices_iterator.
void mgp_vertices_iterator_destroy(struct mgp_vertices_iterator *it);

/// Start iterating over vertices of the given graph.
/// Resulting mgp_vertices_iterator needs to be deallocated with mgp_vertices_iterator_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertices_iterator.
enum mgp_error mgp_graph_iter_vertices(struct mgp_graph *g, struct mgp_memory *memory,
                                       struct mgp_vertices_iterator **result);

/// Result is non-zero if the vertices returned by this iterator can be modified.
/// The mutability of the mgp_vertices_iterator is the same as the graph which it belongs to.
/// Current implementation always returns without errors.
enum mgp_error mgp_vertices_iterator_underlying_graph_is_mutable(struct mgp_vertices_iterator *it, int *result);

/// Get the current vertex pointed to by the iterator.
/// When the mgp_vertices_iterator_next is invoked, the previous
/// mgp_vertex is invalidated and its value must not be used.
/// Result is NULL if the end of the iteration has been reached.
enum mgp_error mgp_vertices_iterator_get(struct mgp_vertices_iterator *it, struct mgp_vertex **result);

/// @name Temporal Types
///
///@{

struct mgp_date_parameters {
  int year;
  int month;
  int day;
};

/// Create a date from a string following the ISO 8601 format.
/// Resulting date must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the string cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_from_string(const char *string, struct mgp_memory *memory, struct mgp_date **date);

/// Create a date from mgp_date_parameter.
/// Resulting date must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the parameters cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_from_parameters(struct mgp_date_parameters *parameters, struct mgp_memory *memory,
                                        struct mgp_date **date);

/// Copy a mgp_date.
/// Resulting pointer must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_copy(struct mgp_date *date, struct mgp_memory *memory, struct mgp_date **result);

/// Free the memory used by a mgp_date.
void mgp_date_destroy(struct mgp_date *date);

/// Result is non-zero if given dates are equal, otherwise 0.
enum mgp_error mgp_date_equal(struct mgp_date *first, struct mgp_date *second, int *result);

/// Get the year property of the date.
enum mgp_error mgp_date_get_year(struct mgp_date *date, int *year);

/// Get the month property of the date.
enum mgp_error mgp_date_get_month(struct mgp_date *date, int *month);

/// Get the day property of the date.
enum mgp_error mgp_date_get_day(struct mgp_date *date, int *day);

/// Get the date as microseconds from Unix epoch.
enum mgp_error mgp_date_timestamp(struct mgp_date *date, int64_t *timestamp);

/// Get the date representing current date.
/// Resulting date must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_now(struct mgp_memory *memory, struct mgp_date **date);

/// Add a duration to the date.
/// Resulting date must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid date.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_add_duration(struct mgp_date *date, struct mgp_duration *dur, struct mgp_memory *memory,
                                     struct mgp_date **result);

/// Subtract a duration from the date.
/// Resulting date must be freed with mgp_date_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid date.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_sub_duration(struct mgp_date *date, struct mgp_duration *dur, struct mgp_memory *memory,
                                     struct mgp_date **result);

/// Get a duration between two dates.
/// Resulting duration must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_date_diff(struct mgp_date *first, struct mgp_date *second, struct mgp_memory *memory,
                             struct mgp_duration **result);

struct mgp_local_time_parameters {
  int hour;
  int minute;
  int second;
  int millisecond;
  int microsecond;
};

/// Create a local time from a string following the ISO 8601 format.
/// Resulting local time must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the string cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_from_string(const char *string, struct mgp_memory *memory,
                                          struct mgp_local_time **local_time);

/// Create a local time from mgp_local_time_parameters.
/// Resulting local time must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the parameters cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_from_parameters(struct mgp_local_time_parameters *parameters, struct mgp_memory *memory,
                                              struct mgp_local_time **local_time);

/// Copy a mgp_local_time.
/// Resulting pointer must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_copy(struct mgp_local_time *local_time, struct mgp_memory *memory,
                                   struct mgp_local_time **result);

/// Free the memory used by a mgp_local_time.
void mgp_local_time_destroy(struct mgp_local_time *local_time);

/// Result is non-zero if given local times are equal, otherwise 0.
enum mgp_error mgp_local_time_equal(struct mgp_local_time *first, struct mgp_local_time *second, int *result);

/// Get the hour property of the local time.
enum mgp_error mgp_local_time_get_hour(struct mgp_local_time *local_time, int *hour);

/// Get the minute property of the local time.
enum mgp_error mgp_local_time_get_minute(struct mgp_local_time *local_time, int *minute);

/// Get the second property of the local time.
enum mgp_error mgp_local_time_get_second(struct mgp_local_time *local_time, int *second);

/// Get the millisecond property of the local time.
enum mgp_error mgp_local_time_get_millisecond(struct mgp_local_time *local_time, int *millisecond);

/// Get the microsecond property of the local time.
enum mgp_error mgp_local_time_get_microsecond(struct mgp_local_time *local_time, int *microsecond);

/// Get the local time as microseconds from midnight.
enum mgp_error mgp_local_time_timestamp(struct mgp_local_time *local_time, int64_t *timestamp);

/// Get the local time representing current time.
/// Resulting pointer must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_now(struct mgp_memory *memory, struct mgp_local_time **local_time);

/// Add a duration to the local time.
/// Resulting pointer must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid local time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_add_duration(struct mgp_local_time *local_time, struct mgp_duration *dur,
                                           struct mgp_memory *memory, struct mgp_local_time **result);

/// Subtract a duration from the local time.
/// Resulting pointer must be freed with mgp_local_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid local time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_sub_duration(struct mgp_local_time *local_time, struct mgp_duration *dur,
                                           struct mgp_memory *memory, struct mgp_local_time **result);

/// Get a duration between two local times.
/// Resulting pointer must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_date.
enum mgp_error mgp_local_time_diff(struct mgp_local_time *first, struct mgp_local_time *second,
                                   struct mgp_memory *memory, struct mgp_duration **result);

struct mgp_local_date_time_parameters {
  struct mgp_date_parameters *date_parameters;
  struct mgp_local_time_parameters *local_time_parameters;
};

/// Create a local date-time from a string following the ISO 8601 format.
/// Resulting local date-time must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the string cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_from_string(const char *string, struct mgp_memory *memory,
                                               struct mgp_local_date_time **local_date_time);

/// Create a local date-time from mgp_local_date_time_parameters.
/// Resulting local date-time must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the parameters cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_from_parameters(struct mgp_local_date_time_parameters *parameters,
                                                   struct mgp_memory *memory,
                                                   struct mgp_local_date_time **local_date_time);

/// Copy a mgp_local_date_time.
/// Resulting pointer must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_copy(struct mgp_local_date_time *local_date_time, struct mgp_memory *memory,
                                        struct mgp_local_date_time **result);

/// Free the memory used by a mgp_local_date_time.
void mgp_local_date_time_destroy(struct mgp_local_date_time *local_date_time);

/// Result is non-zero if given local date-times are equal, otherwise 0.
enum mgp_error mgp_local_date_time_equal(struct mgp_local_date_time *first, struct mgp_local_date_time *second,
                                         int *result);

/// Get the year property of the local date-time.
enum mgp_error mgp_local_date_time_get_year(struct mgp_local_date_time *local_date_time, int *year);

/// Get the month property of the local date-time.
enum mgp_error mgp_local_date_time_get_month(struct mgp_local_date_time *local_date_time, int *month);

/// Get the day property of the local date-time.
enum mgp_error mgp_local_date_time_get_day(struct mgp_local_date_time *local_date_time, int *day);

/// Get the hour property of the local date-time.
enum mgp_error mgp_local_date_time_get_hour(struct mgp_local_date_time *local_date_time, int *hour);

/// Get the minute property of the local date-time.
enum mgp_error mgp_local_date_time_get_minute(struct mgp_local_date_time *local_date_time, int *minute);

/// Get the second property of the local date-time.
enum mgp_error mgp_local_date_time_get_second(struct mgp_local_date_time *local_date_time, int *second);

/// Get the milisecond property of the local date-time.
enum mgp_error mgp_local_date_time_get_millisecond(struct mgp_local_date_time *local_date_time, int *millisecond);

/// Get the microsecond property of the local date-time.
enum mgp_error mgp_local_date_time_get_microsecond(struct mgp_local_date_time *local_date_time, int *microsecond);

/// Get the local date-time as microseconds from Unix epoch.
enum mgp_error mgp_local_date_time_timestamp(struct mgp_local_date_time *local_date_time, int64_t *timestamp);

/// Get the local date-time representing current date and time.
/// Resulting local date-time must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_now(struct mgp_memory *memory, struct mgp_local_date_time **local_date_time);

/// Add a duration to the local date-time.
/// Resulting local date-time must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid local date-time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_add_duration(struct mgp_local_date_time *local_date_time, struct mgp_duration *dur,
                                                struct mgp_memory *memory, struct mgp_local_date_time **result);

/// Subtract a duration from the local date-time.
/// Resulting local date-time must be freed with mgp_local_date_time_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid local date-time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_sub_duration(struct mgp_local_date_time *local_date_time, struct mgp_duration *dur,
                                                struct mgp_memory *memory, struct mgp_local_date_time **result);

/// Get a duration between two local date-times.
/// Resulting duration must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_local_date_time.
enum mgp_error mgp_local_date_time_diff(struct mgp_local_date_time *first, struct mgp_local_date_time *second,
                                        struct mgp_memory *memory, struct mgp_duration **result);

struct mgp_duration_parameters {
  double day;
  double hour;
  double minute;
  double second;
  double millisecond;
  double microsecond;
};

/// Create a duration from a string following the ISO 8601 format.
/// Resulting duration must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the string cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_from_string(const char *string, struct mgp_memory *memory, struct mgp_duration **duration);

/// Create a duration from mgp_duration_parameters.
/// Resulting duration must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the parameters cannot be parsed correctly.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_from_parameters(struct mgp_duration_parameters *parameters, struct mgp_memory *memory,
                                            struct mgp_duration **duration);

/// Create a duration from microseconds.
/// Resulting duration must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_from_microseconds(int64_t microseconds, struct mgp_memory *memory,
                                              struct mgp_duration **duration);

/// Copy a mgp_duration.
/// Resulting pointer must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_copy(struct mgp_duration *duration, struct mgp_memory *memory,
                                 struct mgp_duration **result);

/// Free the memory used by a mgp_duration.
void mgp_duration_destroy(struct mgp_duration *duration);

/// Result is non-zero if given durations are equal, otherwise 0.
enum mgp_error mgp_duration_equal(struct mgp_duration *first, struct mgp_duration *second, int *result);

/// Get the duration as microseconds.
enum mgp_error mgp_duration_get_microseconds(struct mgp_duration *duration, int64_t *microseconds);

/// Apply unary minus operator to the duration.
/// Resulting pointer must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_neg(struct mgp_duration *dur, struct mgp_memory *memory, struct mgp_duration **result);

/// Add two durations.
/// Resulting pointer must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid duration.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_add(struct mgp_duration *first, struct mgp_duration *second, struct mgp_memory *memory,
                                struct mgp_duration **result);

/// Subtract two durations.
/// Resulting pointer must be freed with mgp_duration_destroy.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the operation results in an invalid duration.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_duration.
enum mgp_error mgp_duration_sub(struct mgp_duration *first, struct mgp_duration *second, struct mgp_memory *memory,
                                struct mgp_duration **result);
///@}

/// Advance the iterator to the next vertex and return it.
/// The previous mgp_vertex obtained through mgp_vertices_iterator_get
/// will be invalidated, and you must not use its value.
/// Result is NULL if the end of the iteration has been reached.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate a mgp_vertex.
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
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_any(struct mgp_type **result);

/// Get the type representing boolean values.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_bool(struct mgp_type **result);

/// Get the type representing character string values.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_string(struct mgp_type **result);

/// Get the type representing integer values.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_int(struct mgp_type **result);

/// Get the type representing floating-point values.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_float(struct mgp_type **result);

/// Get the type representing any number value.
///
/// This is the parent type for numeric types, i.e. INTEGER and FLOAT.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_number(struct mgp_type **result);

/// Get the type representing map values.
///
/// Map values are those which map string keys to values of any type. For
/// example `{ database: "Memgraph", version: 1.42 }`. Note that graph nodes
/// contain property maps, so a node value will also satisfy the MAP type. The
/// same applies for graph relationship values.
///
/// @sa mgp_type_node
/// @sa mgp_type_relationship
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_map(struct mgp_type **result);

/// Get the type representing graph node values.
///
/// Since a node contains a map of properties, the node itself is also of MAP
/// type.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_node(struct mgp_type **result);

/// Get the type representing graph relationship values.
///
/// Since a relationship contains a map of properties, the relationship itself
/// is also of MAP type.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_relationship(struct mgp_type **result);

/// Get the type representing a graph path (walk) from one node to another.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_path(struct mgp_type **result);

/// Build a type representing a list of values of given `element_type`.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_list(struct mgp_type *element_type, struct mgp_type **result);

/// Get the type representing a date.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_date(struct mgp_type **result);

/// Get the type representing a local time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_local_time(struct mgp_type **result);

/// Get the type representing a local date-time.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_local_date_time(struct mgp_type **result);

/// Get the type representing a duration.
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_duration(struct mgp_type **result);

/// Build a type representing either a `null` value or a value of given `type`.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate the new type.
enum mgp_error mgp_type_nullable(struct mgp_type *type, struct mgp_type **result);
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

/// Describes a Memgraph magic function.
struct mgp_func;

/// All available log levels that can be used in mgp_log function
MGP_ENUM_CLASS mgp_log_level{
    MGP_LOG_LEVEL_TRACE, MGP_LOG_LEVEL_DEBUG, MGP_LOG_LEVEL_INFO,
    MGP_LOG_LEVEL_WARN,  MGP_LOG_LEVEL_ERROR, MGP_LOG_LEVEL_CRITICAL,
};

/// Entry-point for a query module read procedure, invoked through openCypher.
///
/// Passed in arguments will not live longer than the callback's execution.
/// Therefore, you must not store them globally or use the passed in mgp_memory
/// to allocate global resources.
typedef void (*mgp_proc_cb)(struct mgp_list *, struct mgp_graph *, struct mgp_result *, struct mgp_memory *);

/// Register a read-only procedure to a module.
///
/// The `name` must be a sequence of digits, underscores, lowercase and
/// uppercase Latin letters. The name must begin with a non-digit character.
/// Note that Unicode characters are not allowed. Additionally, names are
/// case-sensitive.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for mgp_proc.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid procedure name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a procedure with the same name was already registered.
enum mgp_error mgp_module_add_read_procedure(struct mgp_module *module, const char *name, mgp_proc_cb cb,
                                             struct mgp_proc **result);

/// Register a writeable procedure to a module.
///
/// The `name` must be a valid identifier, following the same rules as the
/// procedure`name` in mgp_module_add_read_procedure.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for mgp_proc.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid procedure name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a procedure with the same name was already registered.
enum mgp_error mgp_module_add_write_procedure(struct mgp_module *module, const char *name, mgp_proc_cb cb,
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
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if the procedure already has any optional argument.
enum mgp_error mgp_proc_add_arg(struct mgp_proc *proc, const char *name, struct mgp_type *type);

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
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// RETURN mgp_error::MGP_ERROR_VALUE_CONVERSION if `default_value` is a graph element (vertex, edge or path).
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if `default_value` does not satisfy `type`.
enum mgp_error mgp_proc_add_opt_arg(struct mgp_proc *proc, const char *name, struct mgp_type *type,
                                    struct mgp_value *default_value);

/// Add a result field to a procedure.
///
/// The `name` must be a valid identifier, following the same rules as the
/// procedure `name` in mgp_module_add_read_procedure.
///
/// Passed in `type` describes what kind of values can be returned through the
/// result field.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid result name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a result field with the same name was already added.
enum mgp_error mgp_proc_add_result(struct mgp_proc *proc, const char *name, struct mgp_type *type);

/// Add a result field to a procedure and mark it as deprecated.
///
/// This is the same as mgp_proc_add_result, but the result field will be marked
/// as deprecated.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid result name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a result field with the same name was already added.
enum mgp_error mgp_proc_add_deprecated_result(struct mgp_proc *proc, const char *name, struct mgp_type *type);

/// Log a message on a certain level.
enum mgp_error mgp_log(enum mgp_log_level log_level, const char *output);
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
int mgp_must_abort(struct mgp_graph *graph);

/// @}

/// @name Stream Source message API
/// API for accessing specific data contained in a mgp_message
/// used for defining transformation procedures.
/// Not all methods are available for all stream sources
/// so make sure that your transformation procedure can be used
/// for a specific source, i.e. only valid methods are used.
///@{

/// A single Stream source message
struct mgp_message;

/// A list of Stream source messages
struct mgp_messages;

/// Stream source type.
enum mgp_source_type {
  KAFKA,
  PULSAR,
};

/// Get the type of the stream source that produced the message.
enum mgp_error mgp_message_source_type(struct mgp_message *message, enum mgp_source_type *result);

/// Payload is not null terminated and not a string but rather a byte array.
/// You need to call mgp_message_payload_size() first, to read the size of
/// the payload.
/// Supported stream sources:
///   - Kafka
///   - Pulsar
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_payload(struct mgp_message *message, const char **result);

/// Get the payload size
/// Supported stream sources:
///   - Kafka
///   - Pulsar
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_payload_size(struct mgp_message *message, size_t *result);

/// Get the name of topic
/// Supported stream sources:
///   - Kafka
///   - Pulsar
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_topic_name(struct mgp_message *message, const char **result);

/// Get the key of mgp_message as a byte array
/// Supported stream sources:
///   - Kafka
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_key(struct mgp_message *message, const char **result);

/// Get the key size of mgp_message
/// Supported stream sources:
///   - Kafka
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_key_size(struct mgp_message *message, size_t *result);

/// Get the timestamp of mgp_message as a byte array
/// Supported stream sources:
///   - Kafka
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_timestamp(struct mgp_message *message, int64_t *result);

/// Get the message offset from a message.
/// Supported stream sources:
///   - Kafka
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if the message is from an unsupported stream source.
enum mgp_error mgp_message_offset(struct mgp_message *message, int64_t *result);

/// Get the number of messages contained in the mgp_messages list
/// Current implementation always returns without errors.
enum mgp_error mgp_messages_size(struct mgp_messages *message, size_t *result);

/// Get the message from a messages list at given index
enum mgp_error mgp_messages_at(struct mgp_messages *message, size_t index, struct mgp_message **result);

/// Entry-point for a module transformation, invoked through a stream transformation.
///
/// Passed in arguments will not live longer than the callback's execution.
/// Therefore, you must not store them globally or use the passed in mgp_memory
/// to allocate global resources.
typedef void (*mgp_trans_cb)(struct mgp_messages *, struct mgp_graph *, struct mgp_result *, struct mgp_memory *);

/// Register a transformation with a module.
///
/// The `name` must be a sequence of digits, underscores, lowercase and
/// uppercase Latin letters. The name must begin with a non-digit character.
/// Note that Unicode characters are not allowed. Additionally, names are
/// case-sensitive.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for transformation.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid transformation name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a transformation with the same name was already registered.
enum mgp_error mgp_module_add_transformation(struct mgp_module *module, const char *name, mgp_trans_cb cb);
/// @}

/// @name Memgraph Magic Functions API
///
/// API for creating the Memgraph magic functions. It is used to create external-source stateless methods which can
/// be called by using openCypher query language. These methods should not modify the original graph and should use only
/// the values provided as arguments to the method.
///
///@{

/// State of the database that is exposed to magic functions. Currently it is unused, but it enables extending the
/// functionalities of magic functions in future without breaking the API.
struct mgp_func_context;

/// Add a required argument to a function.
///
/// The order of the added arguments corresponds to the signature of the openCypher function.
/// Note, that required arguments are followed by optional arguments.
///
/// The `name` must be a valid identifier, following the same rules as the
/// function `name` in mgp_module_add_function.
///
/// Passed in `type` describes what kind of values can be used as the argument.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// Return mgp_error::MGP_ERROR_LOGIC_ERROR if the function already has any optional argument.
enum mgp_error mgp_func_add_arg(struct mgp_func *func, const char *name, struct mgp_type *type);

/// Add an optional argument with a default value to a function.
///
/// The order of the added arguments corresponds to the signature of the openCypher function.
/// Note, that required arguments are followed by optional arguments.
///
/// The `name` must be a valid identifier, following the same rules as the
/// function `name` in mgp_module_add_function.
///
/// Passed in `type` describes what kind of values can be used as the argument.
///
/// `default_value` is copied and set as the default value for the argument.
/// Don't forget to call mgp_value_destroy when you are done using
/// `default_value`. When the function is called, if this argument is not
/// provided, `default_value` will be used instead. `default_value` must not be
/// a graph element (node, relationship, path) and it must satisfy the given
/// `type`.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for an argument.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid argument name.
/// Return mgp_error::MGP_ERROR_VALUE_CONVERSION if `default_value` is a graph element (vertex, edge or path).
/// Return mgp_error::MGP_ERROR_LOGIC_ERROR if `default_value` does not satisfy `type`.
enum mgp_error mgp_func_add_opt_arg(struct mgp_func *func, const char *name, struct mgp_type *type,
                                    struct mgp_value *default_value);

/// Entry-point for a custom Memgraph awesome function.
///
/// Passed in arguments will not live longer than the callback's execution.
/// Therefore, you must not store them globally or use the passed in mgp_memory
/// to allocate global resources.
typedef void (*mgp_func_cb)(struct mgp_list *, struct mgp_func_context *, struct mgp_func_result *,
                            struct mgp_memory *);

/// Register a Memgraph magic function.
///
/// The `name` must be a sequence of digits, underscores, lowercase and
/// uppercase Latin letters. The name must begin with a non-digit character.
/// Note that Unicode characters are not allowed.
///
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory for mgp_func.
/// Return mgp_error::MGP_ERROR_INVALID_ARGUMENT if `name` is not a valid function name.
/// RETURN mgp_error::MGP_ERROR_LOGIC_ERROR if a function with the same name was already registered.
enum mgp_error mgp_module_add_function(struct mgp_module *module, const char *name, mgp_func_cb cb,
                                       struct mgp_func **result);

/// Set an error message as an output to the Magic function
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if there's no memory for copying the error message.
enum mgp_error mgp_func_result_set_error_msg(struct mgp_func_result *result, const char *error_msg,
                                             struct mgp_memory *memory);

/// Set an output value for the Magic function
/// Return mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE if unable to allocate memory to copy the mgp_value to
/// mgp_func_result.
enum mgp_error mgp_func_result_set_value(struct mgp_func_result *result, struct mgp_value *value,
                                         struct mgp_memory *memory);
/// @}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // MG_PROCEDURE_H
