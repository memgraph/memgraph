// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file _mgp.hpp
///
/// The file contains methods that connect mg procedures and the outside code
/// Methods like mapping a graph into memory or assigning new mg results or
/// their properties are implemented.
#pragma once

#include "mg_exceptions.hpp"
#include "mg_procedure.h"

namespace mgp {

namespace {
inline void MgExceptionHandle(mgp_error result_code) {
  switch (result_code) {
    case mgp_error::MGP_ERROR_UNKNOWN_ERROR:
      throw mg_exception::UnknownException();
    case mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE:
      throw mg_exception::AllocationException();
    case mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER:
      throw mg_exception::InsufficientBufferException();
    case mgp_error::MGP_ERROR_OUT_OF_RANGE:
      throw mg_exception::OutOfRangeException();
    case mgp_error::MGP_ERROR_LOGIC_ERROR:
      throw mg_exception::LogicException();
    case mgp_error::MGP_ERROR_DELETED_OBJECT:
      throw mg_exception::DeletedObjectException();
    case mgp_error::MGP_ERROR_INVALID_ARGUMENT:
      throw mg_exception::InvalidArgumentException();
    case mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS:
      throw mg_exception::KeyAlreadyExistsException();
    case mgp_error::MGP_ERROR_IMMUTABLE_OBJECT:
      throw mg_exception::ImmutableObjectException();
    case mgp_error::MGP_ERROR_VALUE_CONVERSION:
      throw mg_exception::ValueConversionException();
    case mgp_error::MGP_ERROR_SERIALIZATION_ERROR:
      throw mg_exception::SerializationException();
    default:
      return;
  }
}

template <typename TResult, typename TFunc, typename... TArgs>
TResult MgInvoke(TFunc func, TArgs... args) {
  TResult result{};

  auto result_code = func(args..., &result);
  MgExceptionHandle(result_code);

  return result;
}

template <typename TFunc, typename... TArgs>
inline void MgInvokeVoid(TFunc func, TArgs... args) {
  auto result_code = func(args...);
  MgExceptionHandle(result_code);
}
}  // namespace

// mgp_value

// Make value

inline mgp_value *value_make_null(mgp_memory *memory) { return MgInvoke<mgp_value *>(mgp_value_make_null, memory); }

inline mgp_value *value_make_bool(int val, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_value_make_bool, val, memory);
}

inline mgp_value *value_make_int(int64_t val, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_value_make_int, val, memory);
}

inline mgp_value *value_make_double(double val, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_value_make_double, val, memory);
}

inline mgp_value *value_make_string(const char *val, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_value_make_string, val, memory);
}

inline mgp_value *value_make_list(mgp_list *val) { return MgInvoke<mgp_value *>(mgp_value_make_list, val); }

inline mgp_value *value_make_map(mgp_map *val) { return MgInvoke<mgp_value *>(mgp_value_make_map, val); }

inline mgp_value *value_make_vertex(mgp_vertex *val) { return MgInvoke<mgp_value *>(mgp_value_make_vertex, val); }

inline mgp_value *value_make_edge(mgp_edge *val) { return MgInvoke<mgp_value *>(mgp_value_make_edge, val); }

inline mgp_value *value_make_path(mgp_path *val) { return MgInvoke<mgp_value *>(mgp_value_make_path, val); }

inline mgp_value *value_make_date(mgp_date *val) { return MgInvoke<mgp_value *>(mgp_value_make_date, val); }

inline mgp_value *value_make_local_time(mgp_local_time *val) {
  return MgInvoke<mgp_value *>(mgp_value_make_local_time, val);
}

inline mgp_value *value_make_local_date_time(mgp_local_date_time *val) {
  return MgInvoke<mgp_value *>(mgp_value_make_local_date_time, val);
}

inline mgp_value *value_make_duration(mgp_duration *val) { return MgInvoke<mgp_value *>(mgp_value_make_duration, val); }

// Copy value

// TODO: implement within MGP API
// with primitive types ({bool, int, double, string}), create a new identical value
// otherwise call mgp_##TYPE_copy and convert type
inline mgp_value *value_copy(mgp_value *val, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_value_copy, val, memory);
}

// Destroy value

inline void value_destroy(mgp_value *val) { mgp_value_destroy(val); }

// Get value of type

inline mgp_value_type value_get_type(mgp_value *val) { return MgInvoke<mgp_value_type>(mgp_value_get_type, val); }

inline bool value_get_bool(mgp_value *val) { return MgInvoke<int>(mgp_value_get_bool, val); }

inline int64_t value_get_int(mgp_value *val) { return MgInvoke<int64_t>(mgp_value_get_int, val); }

inline double value_get_double(mgp_value *val) { return MgInvoke<double>(mgp_value_get_double, val); }

inline const char *value_get_string(mgp_value *val) { return MgInvoke<const char *>(mgp_value_get_string, val); }

inline mgp_list *value_get_list(mgp_value *val) { return MgInvoke<mgp_list *>(mgp_value_get_list, val); }

inline mgp_map *value_get_map(mgp_value *val) { return MgInvoke<mgp_map *>(mgp_value_get_map, val); }

inline mgp_vertex *value_get_vertex(mgp_value *val) { return MgInvoke<mgp_vertex *>(mgp_value_get_vertex, val); }

inline mgp_edge *value_get_edge(mgp_value *val) { return MgInvoke<mgp_edge *>(mgp_value_get_edge, val); }

inline mgp_path *value_get_path(mgp_value *val) { return MgInvoke<mgp_path *>(mgp_value_get_path, val); }

inline mgp_date *value_get_date(mgp_value *val) { return MgInvoke<mgp_date *>(mgp_value_get_date, val); }

inline mgp_local_time *value_get_local_time(mgp_value *val) {
  return MgInvoke<mgp_local_time *>(mgp_value_get_local_time, val);
}

inline mgp_local_date_time *value_get_local_date_time(mgp_value *val) {
  return MgInvoke<mgp_local_date_time *>(mgp_value_get_local_date_time, val);
}

inline mgp_duration *value_get_duration(mgp_value *val) {
  return MgInvoke<mgp_duration *>(mgp_value_get_duration, val);
}

// Check type of value

inline bool value_is_null(mgp_value *val) { return MgInvoke<int>(mgp_value_is_null, val); }

inline bool value_is_bool(mgp_value *val) { return MgInvoke<int>(mgp_value_is_bool, val); }

inline bool value_is_int(mgp_value *val) { return MgInvoke<int>(mgp_value_is_int, val); }

inline bool value_is_double(mgp_value *val) { return MgInvoke<int>(mgp_value_is_double, val); }

inline bool value_is_string(mgp_value *val) { return MgInvoke<int>(mgp_value_is_string, val); }

inline bool value_is_list(mgp_value *val) { return MgInvoke<int>(mgp_value_is_list, val); }

inline bool value_is_map(mgp_value *val) { return MgInvoke<int>(mgp_value_is_map, val); }

inline bool value_is_vertex(mgp_value *val) { return MgInvoke<int>(mgp_value_is_vertex, val); }

inline bool value_is_edge(mgp_value *val) { return MgInvoke<int>(mgp_value_is_edge, val); }

inline bool value_is_path(mgp_value *val) { return MgInvoke<int>(mgp_value_is_path, val); }

inline bool value_is_date(mgp_value *val) { return MgInvoke<int>(mgp_value_is_date, val); }

inline bool value_is_local_time(mgp_value *val) { return MgInvoke<int>(mgp_value_is_local_time, val); }

inline bool value_is_local_date_time(mgp_value *val) { return MgInvoke<int>(mgp_value_is_local_date_time, val); }

inline bool value_is_duration(mgp_value *val) { return MgInvoke<int>(mgp_value_is_duration, val); }

// Get type

inline mgp_type *type_any() { return MgInvoke<mgp_type *>(mgp_type_any); }

inline mgp_type *type_bool() { return MgInvoke<mgp_type *>(mgp_type_bool); }

inline mgp_type *type_string() { return MgInvoke<mgp_type *>(mgp_type_string); }

inline mgp_type *type_int() { return MgInvoke<mgp_type *>(mgp_type_int); }

inline mgp_type *type_float() { return MgInvoke<mgp_type *>(mgp_type_float); }

inline mgp_type *type_number() { return MgInvoke<mgp_type *>(mgp_type_number); }

inline mgp_type *type_list(mgp_type *element_type) { return MgInvoke<mgp_type *>(mgp_type_list, element_type); }

inline mgp_type *type_map() { return MgInvoke<mgp_type *>(mgp_type_map); }

inline mgp_type *type_node() { return MgInvoke<mgp_type *>(mgp_type_node); }

inline mgp_type *type_relationship() { return MgInvoke<mgp_type *>(mgp_type_relationship); }

inline mgp_type *type_path() { return MgInvoke<mgp_type *>(mgp_type_path); }

inline mgp_type *type_date() { return MgInvoke<mgp_type *>(mgp_type_date); }

inline mgp_type *type_local_time() { return MgInvoke<mgp_type *>(mgp_type_local_time); }

inline mgp_type *type_local_date_time() { return MgInvoke<mgp_type *>(mgp_type_local_date_time); }

inline mgp_type *type_duration() { return MgInvoke<mgp_type *>(mgp_type_duration); }

inline mgp_type *type_nullable(mgp_type *type) { return MgInvoke<mgp_type *>(mgp_type_nullable, type); }

// mgp_graph

inline bool graph_is_mutable(mgp_graph *graph) { return MgInvoke<int>(mgp_graph_is_mutable, graph); }

inline mgp_vertex *graph_create_vertex(mgp_graph *graph, mgp_memory *memory) {
  return MgInvoke<mgp_vertex *>(mgp_graph_create_vertex, graph, memory);
}

inline void graph_delete_vertex(mgp_graph *graph, mgp_vertex *vertex) {
  MgInvokeVoid(mgp_graph_delete_vertex, graph, vertex);
}

inline void graph_detach_delete_vertex(mgp_graph *graph, mgp_vertex *vertex) {
  MgInvokeVoid(mgp_graph_detach_delete_vertex, graph, vertex);
}

inline mgp_edge *graph_create_edge(mgp_graph *graph, mgp_vertex *from, mgp_vertex *to, mgp_edge_type type,
                                   mgp_memory *memory) {
  return MgInvoke<mgp_edge *>(mgp_graph_create_edge, graph, from, to, type, memory);
}

inline void graph_delete_edge(mgp_graph *graph, mgp_edge *edge) { MgInvokeVoid(mgp_graph_delete_edge, graph, edge); }

inline mgp_vertex *graph_get_vertex_by_id(mgp_graph *g, mgp_vertex_id id, mgp_memory *memory) {
  return MgInvoke<mgp_vertex *>(mgp_graph_get_vertex_by_id, g, id, memory);
}

inline mgp_vertices_iterator *graph_iter_vertices(mgp_graph *g, mgp_memory *memory) {
  return MgInvoke<mgp_vertices_iterator *>(mgp_graph_iter_vertices, g, memory);
}

// mgp_vertices_iterator

inline void vertices_iterator_destroy(mgp_vertices_iterator *it) { mgp_vertices_iterator_destroy(it); }

inline mgp_vertex *vertices_iterator_get(mgp_vertices_iterator *it) {
  return MgInvoke<mgp_vertex *>(mgp_vertices_iterator_get, it);
}

inline mgp_vertex *vertices_iterator_next(mgp_vertices_iterator *it) {
  return MgInvoke<mgp_vertex *>(mgp_vertices_iterator_next, it);
}

// mgp_edges_iterator

inline void edges_iterator_destroy(mgp_edges_iterator *it) { mgp_edges_iterator_destroy(it); }

inline mgp_edge *edges_iterator_get(mgp_edges_iterator *it) { return MgInvoke<mgp_edge *>(mgp_edges_iterator_get, it); }

inline mgp_edge *edges_iterator_next(mgp_edges_iterator *it) {
  return MgInvoke<mgp_edge *>(mgp_edges_iterator_next, it);
}

// mgp_properties_iterator

inline void properties_iterator_destroy(mgp_properties_iterator *it) { mgp_properties_iterator_destroy(it); }

inline mgp_property *properties_iterator_get(mgp_properties_iterator *it) {
  return MgInvoke<mgp_property *>(mgp_properties_iterator_get, it);
}

inline mgp_property *properties_iterator_next(mgp_properties_iterator *it) {
  return MgInvoke<mgp_property *>(mgp_properties_iterator_next, it);
}

// Container {mgp_list, mgp_map} methods

// mgp_list

inline mgp_list *list_make_empty(size_t capacity, mgp_memory *memory) {
  return MgInvoke<mgp_list *>(mgp_list_make_empty, capacity, memory);
}

inline mgp_list *list_copy(mgp_list *list, mgp_memory *memory) {
  return MgInvoke<mgp_list *>(mgp_list_copy, list, memory);
}

inline void list_destroy(mgp_list *list) { mgp_list_destroy(list); }

inline void list_append(mgp_list *list, mgp_value *val) { MgInvokeVoid(mgp_list_append, list, val); }

inline void list_append_extend(mgp_list *list, mgp_value *val) { MgInvokeVoid(mgp_list_append_extend, list, val); }

inline size_t list_size(mgp_list *list) { return MgInvoke<size_t>(mgp_list_size, list); }

inline size_t list_capacity(mgp_list *list) { return MgInvoke<size_t>(mgp_list_capacity, list); }

inline mgp_value *list_at(mgp_list *list, size_t index) { return MgInvoke<mgp_value *>(mgp_list_at, list, index); }

// mgp_map

inline mgp_map *map_make_empty(mgp_memory *memory) { return MgInvoke<mgp_map *>(mgp_map_make_empty, memory); }

inline mgp_map *map_copy(mgp_map *map, mgp_memory *memory) { return MgInvoke<mgp_map *>(mgp_map_copy, map, memory); }

inline void map_destroy(mgp_map *map) { mgp_map_destroy(map); }

inline void map_insert(mgp_map *map, const char *key, mgp_value *value) {
  MgInvokeVoid(mgp_map_insert, map, key, value);
}

inline size_t map_size(mgp_map *map) { return MgInvoke<size_t>(mgp_map_size, map); }

inline mgp_value *map_at(mgp_map *map, const char *key) { return MgInvoke<mgp_value *>(mgp_map_at, map, key); }

inline const char *map_item_key(mgp_map_item *item) { return MgInvoke<const char *>(mgp_map_item_key, item); }

inline mgp_value *map_item_value(mgp_map_item *item) { return MgInvoke<mgp_value *>(mgp_map_item_value, item); }

inline mgp_map_items_iterator *map_iter_items(mgp_map *map, mgp_memory *memory) {
  return MgInvoke<mgp_map_items_iterator *>(mgp_map_iter_items, map, memory);
}

inline void map_items_iterator_destroy(mgp_map_items_iterator *it) { mgp_map_items_iterator_destroy(it); }

inline mgp_map_item *map_items_iterator_get(mgp_map_items_iterator *it) {
  return MgInvoke<mgp_map_item *>(mgp_map_items_iterator_get, it);
}

inline mgp_map_item *map_items_iterator_next(mgp_map_items_iterator *it) {
  return MgInvoke<mgp_map_item *>(mgp_map_items_iterator_next, it);
}

// mgp_vertex

inline mgp_vertex_id vertex_get_id(mgp_vertex *v) { return MgInvoke<mgp_vertex_id>(mgp_vertex_get_id, v); }

inline mgp_vertex *vertex_copy(mgp_vertex *v, mgp_memory *memory) {
  return MgInvoke<mgp_vertex *>(mgp_vertex_copy, v, memory);
}

inline void vertex_destroy(mgp_vertex *v) { mgp_vertex_destroy(v); }

inline bool vertex_equal(mgp_vertex *v1, mgp_vertex *v2) { return MgInvoke<int>(mgp_vertex_equal, v1, v2); }

inline size_t vertex_labels_count(mgp_vertex *v) { return MgInvoke<size_t>(mgp_vertex_labels_count, v); }

inline mgp_label vertex_label_at(mgp_vertex *v, size_t index) {
  return MgInvoke<mgp_label>(mgp_vertex_label_at, v, index);
}

inline bool vertex_has_label(mgp_vertex *v, mgp_label label) { return MgInvoke<int>(mgp_vertex_has_label, v, label); }

inline bool vertex_has_label_named(mgp_vertex *v, const char *label_name) {
  return MgInvoke<int>(mgp_vertex_has_label_named, v, label_name);
}

inline void vertex_add_label(mgp_vertex *vertex, mgp_label label) { MgInvokeVoid(mgp_vertex_add_label, vertex, label); }

inline mgp_value *vertex_get_property(mgp_vertex *v, const char *property_name, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_vertex_get_property, v, property_name, memory);
}

inline void vertex_set_property(mgp_vertex *v, const char *property_name, mgp_value *property_value) {
  MgInvokeVoid(mgp_vertex_set_property, v, property_name, property_value);
}

inline mgp_properties_iterator *vertex_iter_properties(mgp_vertex *v, mgp_memory *memory) {
  return MgInvoke<mgp_properties_iterator *>(mgp_vertex_iter_properties, v, memory);
}

inline mgp_edges_iterator *vertex_iter_in_edges(mgp_vertex *v, mgp_memory *memory) {
  return MgInvoke<mgp_edges_iterator *>(mgp_vertex_iter_in_edges, v, memory);
}

inline mgp_edges_iterator *vertex_iter_out_edges(mgp_vertex *v, mgp_memory *memory) {
  return MgInvoke<mgp_edges_iterator *>(mgp_vertex_iter_out_edges, v, memory);
}

// mgp_edge

inline mgp_edge_id edge_get_id(mgp_edge *e) { return MgInvoke<mgp_edge_id>(mgp_edge_get_id, e); }

inline mgp_edge *edge_copy(mgp_edge *e, mgp_memory *memory) { return MgInvoke<mgp_edge *>(mgp_edge_copy, e, memory); }

inline void edge_destroy(mgp_edge *e) { mgp_edge_destroy(e); }

inline bool edge_equal(mgp_edge *e1, mgp_edge *e2) { return MgInvoke<int>(mgp_edge_equal, e1, e2); }

inline mgp_edge_type edge_get_type(mgp_edge *e) { return MgInvoke<mgp_edge_type>(mgp_edge_get_type, e); }

inline mgp_vertex *edge_get_from(mgp_edge *e) { return MgInvoke<mgp_vertex *>(mgp_edge_get_from, e); }

inline mgp_vertex *edge_get_to(mgp_edge *e) { return MgInvoke<mgp_vertex *>(mgp_edge_get_to, e); }

inline mgp_value *edge_get_property(mgp_edge *e, const char *property_name, mgp_memory *memory) {
  return MgInvoke<mgp_value *>(mgp_edge_get_property, e, property_name, memory);
}

inline void edge_set_property(mgp_edge *e, const char *property_name, mgp_value *property_value) {
  MgInvokeVoid(mgp_edge_set_property, e, property_name, property_value);
}

inline mgp_properties_iterator *edge_iter_properties(mgp_edge *e, mgp_memory *memory) {
  return MgInvoke<mgp_properties_iterator *>(mgp_edge_iter_properties, e, memory);
}

// mgp_path

inline mgp_path *path_make_with_start(mgp_vertex *vertex, mgp_memory *memory) {
  return MgInvoke<mgp_path *>(mgp_path_make_with_start, vertex, memory);
}

inline mgp_path *path_copy(mgp_path *path, mgp_memory *memory) {
  return MgInvoke<mgp_path *>(mgp_path_copy, path, memory);
}

inline void path_destroy(mgp_path *path) { mgp_path_destroy(path); }

inline void path_expand(mgp_path *path, mgp_edge *edge) { MgInvokeVoid(mgp_path_expand, path, edge); }

inline size_t path_size(mgp_path *path) { return MgInvoke<size_t>(mgp_path_size, path); }

inline mgp_vertex *path_vertex_at(mgp_path *path, size_t index) {
  return MgInvoke<mgp_vertex *>(mgp_path_vertex_at, path, index);
}

inline mgp_edge *path_edge_at(mgp_path *path, size_t index) {
  return MgInvoke<mgp_edge *>(mgp_path_edge_at, path, index);
}

inline bool path_equal(mgp_path *p1, mgp_path *p2) { return MgInvoke<int>(mgp_path_equal, p1, p2); }

// Temporal type {mgp_date, mgp_local_time, mgp_local_date_time, mgp_duration} methods

// mgp_date

inline mgp_date *date_from_string(const char *string, mgp_memory *memory) {
  return MgInvoke<mgp_date *>(mgp_date_from_string, string, memory);
}

inline mgp_date *date_from_parameters(mgp_date_parameters *parameters, mgp_memory *memory) {
  return MgInvoke<mgp_date *>(mgp_date_from_parameters, parameters, memory);
}

inline mgp_date *date_copy(mgp_date *date, mgp_memory *memory) {
  return MgInvoke<mgp_date *>(mgp_date_copy, date, memory);
}

inline void date_destroy(mgp_date *date) { mgp_date_destroy(date); }

inline bool date_equal(mgp_date *first, mgp_date *second) { return MgInvoke<int>(mgp_date_equal, first, second); }

inline int date_get_year(mgp_date *date) { return MgInvoke<int>(mgp_date_get_year, date); }

inline int date_get_month(mgp_date *date) { return MgInvoke<int>(mgp_date_get_month, date); }

inline int date_get_day(mgp_date *date) { return MgInvoke<int>(mgp_date_get_day, date); }

inline int64_t date_timestamp(mgp_date *date) { return MgInvoke<int64_t>(mgp_date_timestamp, date); }

inline mgp_date *date_now(mgp_memory *memory) { return MgInvoke<mgp_date *>(mgp_date_now, memory); }

inline mgp_date *date_add_duration(mgp_date *date, mgp_duration *dur, mgp_memory *memory) {
  return MgInvoke<mgp_date *>(mgp_date_add_duration, date, dur, memory);
}

inline mgp_date *date_sub_duration(mgp_date *date, mgp_duration *dur, mgp_memory *memory) {
  return MgInvoke<mgp_date *>(mgp_date_sub_duration, date, dur, memory);
}

inline mgp_duration *date_diff(mgp_date *first, mgp_date *second, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_date_diff, first, second, memory);
}

// mgp_local_time

inline mgp_local_time *local_time_from_string(const char *string, mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_from_string, string, memory);
}

inline mgp_local_time *local_time_from_parameters(mgp_local_time_parameters *parameters, mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_from_parameters, parameters, memory);
}

inline mgp_local_time *local_time_copy(mgp_local_time *local_time, mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_copy, local_time, memory);
}

inline void local_time_destroy(mgp_local_time *local_time) { mgp_local_time_destroy(local_time); }

inline bool local_time_equal(mgp_local_time *first, mgp_local_time *second) {
  return MgInvoke<int>(mgp_local_time_equal, first, second);
}

inline int local_time_get_hour(mgp_local_time *local_time) {
  return MgInvoke<int>(mgp_local_time_get_hour, local_time);
}

inline int local_time_get_minute(mgp_local_time *local_time) {
  return MgInvoke<int>(mgp_local_time_get_minute, local_time);
}

inline int local_time_get_second(mgp_local_time *local_time) {
  return MgInvoke<int>(mgp_local_time_get_second, local_time);
}

inline int local_time_get_millisecond(mgp_local_time *local_time) {
  return MgInvoke<int>(mgp_local_time_get_millisecond, local_time);
}

inline int local_time_get_microsecond(mgp_local_time *local_time) {
  return MgInvoke<int>(mgp_local_time_get_microsecond, local_time);
}

inline int64_t local_time_timestamp(mgp_local_time *local_time) {
  return MgInvoke<int64_t>(mgp_local_time_timestamp, local_time);
}

inline mgp_local_time *local_time_now(mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_now, memory);
}

inline mgp_local_time *local_time_add_duration(mgp_local_time *local_time, mgp_duration *dur, mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_add_duration, local_time, dur, memory);
}

inline mgp_local_time *local_time_sub_duration(mgp_local_time *local_time, mgp_duration *dur, mgp_memory *memory) {
  return MgInvoke<mgp_local_time *>(mgp_local_time_sub_duration, local_time, dur, memory);
}

inline mgp_duration *local_time_diff(mgp_local_time *first, mgp_local_time *second, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_local_time_diff, first, second, memory);
}

// mgp_local_date_time

inline mgp_local_date_time *local_date_time_from_string(const char *string, mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_from_string, string, memory);
}

inline mgp_local_date_time *local_date_time_from_parameters(mgp_local_date_time_parameters *parameters,
                                                            mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_from_parameters, parameters, memory);
}

inline mgp_local_date_time *local_date_time_copy(mgp_local_date_time *local_date_time, mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_copy, local_date_time, memory);
}

inline void local_date_time_destroy(mgp_local_date_time *local_date_time) {
  mgp_local_date_time_destroy(local_date_time);
}

inline bool local_date_time_equal(mgp_local_date_time *first, mgp_local_date_time *second) {
  return MgInvoke<int>(mgp_local_date_time_equal, first, second);
}

inline int local_date_time_get_year(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_year, local_date_time);
}

inline int local_date_time_get_month(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_month, local_date_time);
}

inline int local_date_time_get_day(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_day, local_date_time);
}

inline int local_date_time_get_hour(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_hour, local_date_time);
}

inline int local_date_time_get_minute(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_minute, local_date_time);
}

inline int local_date_time_get_second(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_second, local_date_time);
}

inline int local_date_time_get_millisecond(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_millisecond, local_date_time);
}

inline int local_date_time_get_microsecond(mgp_local_date_time *local_date_time) {
  return MgInvoke<int>(mgp_local_date_time_get_microsecond, local_date_time);
}

inline int64_t local_date_time_timestamp(mgp_local_date_time *local_date_time) {
  return MgInvoke<int64_t>(mgp_local_date_time_timestamp, local_date_time);
}

inline mgp_local_date_time *local_date_time_now(mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_now, memory);
}

inline mgp_local_date_time *local_date_time_add_duration(mgp_local_date_time *local_date_time, mgp_duration *dur,
                                                         mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_add_duration, local_date_time, dur, memory);
}

inline mgp_local_date_time *local_date_time_sub_duration(mgp_local_date_time *local_date_time, mgp_duration *dur,
                                                         mgp_memory *memory) {
  return MgInvoke<mgp_local_date_time *>(mgp_local_date_time_sub_duration, local_date_time, dur, memory);
}

inline mgp_duration *local_date_time_diff(mgp_local_date_time *first, mgp_local_date_time *second, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_local_date_time_diff, first, second, memory);
}

// mgp_duration

inline mgp_duration *duration_from_string(const char *string, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_from_string, string, memory);
}

inline mgp_duration *duration_from_parameters(mgp_duration_parameters *parameters, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_from_parameters, parameters, memory);
}

inline mgp_duration *duration_from_microseconds(int64_t microseconds, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_from_microseconds, microseconds, memory);
}

inline mgp_duration *duration_copy(mgp_duration *duration, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_copy, duration, memory);
}

inline void duration_destroy(mgp_duration *duration) { mgp_duration_destroy(duration); }

inline int64_t duration_get_microseconds(mgp_duration *duration) {
  return MgInvoke<int64_t>(mgp_duration_get_microseconds, duration);
}

inline bool duration_equal(mgp_duration *first, mgp_duration *second) {
  return MgInvoke<int>(mgp_duration_equal, first, second);
}

inline mgp_duration *duration_neg(mgp_duration *duration, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_neg, duration, memory);
}

inline mgp_duration *duration_add(mgp_duration *first, mgp_duration *second, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_add, first, second, memory);
}

inline mgp_duration *duration_sub(mgp_duration *first, mgp_duration *second, mgp_memory *memory) {
  return MgInvoke<mgp_duration *>(mgp_duration_sub, first, second, memory);
}

// Procedure

inline mgp_proc *module_add_read_procedure(mgp_module *module, const char *name, mgp_proc_cb cb) {
  return MgInvoke<mgp_proc *>(mgp_module_add_read_procedure, module, name, cb);
}

inline mgp_proc *module_add_write_procedure(mgp_module *module, const char *name, mgp_proc_cb cb) {
  return MgInvoke<mgp_proc *>(mgp_module_add_write_procedure, module, name, cb);
}

inline void proc_add_arg(mgp_proc *proc, const char *name, mgp_type *type) {
  MgInvokeVoid(mgp_proc_add_arg, proc, name, type);
}

inline void proc_add_opt_arg(mgp_proc *proc, const char *name, mgp_type *type, mgp_value *default_value) {
  MgInvokeVoid(mgp_proc_add_opt_arg, proc, name, type, default_value);
}

inline void proc_add_result(mgp_proc *proc, const char *name, mgp_type *type) {
  MgInvokeVoid(mgp_proc_add_result, proc, name, type);
}

inline void proc_add_deprecated_result(mgp_proc *proc, const char *name, mgp_type *type) {
  MgInvokeVoid(mgp_proc_add_deprecated_result, proc, name, type);
}

inline bool must_abort(mgp_graph *graph) { return mgp_must_abort(graph); }

// mgp_result

inline void result_set_error_msg(mgp_result *res, const char *error_msg) {
  MgInvokeVoid(mgp_result_set_error_msg, res, error_msg);
}

inline mgp_result_record *result_new_record(mgp_result *res) {
  return MgInvoke<mgp_result_record *>(mgp_result_new_record, res);
}

inline void result_record_insert(mgp_result_record *record, const char *field_name, mgp_value *val) {
  MgInvokeVoid(mgp_result_record_insert, record, field_name, val);
}

// Function

inline mgp_func *module_add_function(mgp_module *module, const char *name, mgp_func_cb cb) {
  return MgInvoke<mgp_func *>(mgp_module_add_function, module, name, cb);
}

inline void func_add_arg(mgp_func *func, const char *name, mgp_type *type) {
  MgInvokeVoid(mgp_func_add_arg, func, name, type);
}

inline void func_add_opt_arg(mgp_func *func, const char *name, mgp_type *type, mgp_value *default_value) {
  MgInvokeVoid(mgp_func_add_opt_arg, func, name, type, default_value);
}

inline void func_result_set_error_msg(mgp_func_result *res, const char *msg, mgp_memory *memory) {
  MgInvokeVoid(mgp_func_result_set_error_msg, res, msg, memory);
}

inline void func_result_set_value(mgp_func_result *res, mgp_value *value, mgp_memory *memory) {
  MgInvokeVoid(mgp_func_result_set_value, res, value, memory);
}

}  // namespace mgp
