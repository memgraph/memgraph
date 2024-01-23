// Copyright 2024 Memgraph Ltd.
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
#pragma once

#include <cstdint>

namespace memgraph::utils {

enum class TypeId : uint64_t {
  // Operators
  UNKNOWN,
  LOGICAL_OPERATOR,
  ONCE,
  NODE_CREATION_INFO,
  CREATE_NODE,
  EDGE_CREATION_INFO,
  CREATE_EXPAND,
  SCAN_ALL,
  SCAN_ALL_BY_LABEL,
  SCAN_ALL_BY_LABEL_PROPERTY_RANGE,
  SCAN_ALL_BY_LABEL_PROPERTY_VALUE,
  SCAN_ALL_BY_LABEL_PROPERTY,
  SCAN_ALL_BY_ID,
  EXPAND_COMMON,
  EXPAND,
  EXPANSION_LAMBDA,
  EXPAND_VARIABLE,
  CONSTRUCT_NAMED_PATH,
  FILTER,
  PRODUCE,
  DELETE,
  SET_PROPERTY,
  SET_PROPERTIES,
  SET_LABELS,
  REMOVE_PROPERTY,
  REMOVE_LABELS,
  EDGE_UNIQUENESS_FILTER,
  EMPTY_RESULT,
  ACCUMULATE,
  AGGREGATE,
  AGGREGATE_ELEMENT,
  SKIP,
  EVALUATE_PATTERN_FILTER,
  LIMIT,
  ORDERBY,
  MERGE,
  OPTIONAL,
  UNWIND,
  DISTINCT,
  UNION,
  CARTESIAN,
  OUTPUT_TABLE,
  OUTPUT_TABLE_STREAM,
  CALL_PROCEDURE,
  LOAD_CSV,
  FOREACH,
  APPLY,
  INDEXED_JOIN,
  HASH_JOIN,

  // Replication
  REP_APPEND_DELTAS_REQ,
  REP_APPEND_DELTAS_RES,
  REP_HEARTBEAT_REQ,
  REP_HEARTBEAT_RES,
  REP_FREQUENT_HEARTBEAT_REQ,
  REP_FREQUENT_HEARTBEAT_RES,
  REP_SNAPSHOT_REQ,
  REP_SNAPSHOT_RES,
  REP_WALFILES_REQ,
  REP_WALFILES_RES,
  REP_CURRENT_WAL_REQ,
  REP_CURRENT_WAL_RES,
  REP_TIMESTAMP_REQ,
  REP_TIMESTAMP_RES,

  // Coordinator
  COORD_FAILOVER_REQ,
  COORD_FAILOVER_RES,
  COORD_SET_REPL_MAIN_REQ,
  COORD_SET_REPL_MAIN_RES,

  // AST
  AST_LABELIX,
  AST_PROPERTYIX,
  AST_EDGETYPEIX,
  AST_TREE,
  AST_EXPRESSION,
  AST_WHERE,
  AST_BINARY_OPERATOR,
  AST_UNARY_OPERATOR,
  AST_OR_OPERATOR,
  AST_XOR_OPERATOR,
  AST_AND_OPERATOR,
  AST_ADDITION_OPERATOR,
  AST_SUBTRACTION_OPERATOR,
  AST_MULTIPLICATION_OPERATOR,
  AST_DIVISION_OPERATOR,
  AST_MOD_OPERATOR,
  AST_NOT_EQUAL_OPERATOR,
  AST_EQUAL_OPERATOR,
  AST_LESS_OPERATOR,
  AST_GREATER_OPERATOR,
  AST_LESS_EQUAL_OPERATOR,
  AST_GREATER_EQUAL_OPERATOR,
  AST_IN_LIST_OPERATOR,
  AST_SUBSCRIPT_OPERATOR,
  AST_NOT_OPERATOR,
  AST_UNARY_PLUS_OPERATOR,
  AST_UNARY_MINUS_OPERATOR,
  AST_IS_NULL_OPERATOR,
  AST_AGGREGATION,
  AST_LIST_SLICING_OPERATOR,
  AST_IF_OPERATOR,
  AST_BASE_LITERAL,
  AST_PRIMITIVE_LITERAL,
  AST_LIST_LITERAL,
  AST_MAP_LITERAL,
  AST_MAP_PROJECTION_LITERAL,
  AST_IDENTIFIER,
  AST_PROPERTY_LOOKUP,
  AST_ALL_PROPERTIES_LOOKUP,
  AST_LABELS_TEST,
  AST_FUNCTION,
  AST_REDUCE,
  AST_COALESCE,
  AST_EXTRACT,
  AST_ALL,
  AST_SINGLE,
  AST_ANY,
  AST_NONE,
  AST_PARAMETER_LOOKUP,
  AST_REGEX_MATCH,
  AST_NAMED_EXPRESSION,
  AST_PATTERN_ATOM,
  AST_NODE_ATOM,
  AST_EDGE_ATOM_LAMBDA,
  AST_EDGE_ATOM,
  AST_PATTERN,
  AST_CLAUSE,
  AST_SINGLE_QUERY,
  AST_CYPHER_UNION,
  AST_QUERY,
  AST_CYPHER_QUERY,
  AST_EXPLAIN_QUERY,
  AST_PROFILE_QUERY,
  AST_INDEX_QUERY,
  AST_CREATE,
  AST_CALL_PROCEDURE,
  AST_MATCH,
  AST_SORT_ITEM,
  AST_RETURN_BODY,
  AST_RETURN,
  AST_WITH,
  AST_DELETE,
  AST_SET_PROPERTY,
  AST_SET_PROPERTIES,
  AST_SET_LABELS,
  AST_REMOVE_PROPERTY,
  AST_REMOVE_LABELS,
  AST_MERGE,
  AST_UNWIND,
  AST_AUTH_QUERY,
  AST_DATABASE_INFO_QUERY,
  AST_SYSTEM_INFO_QUERY,
  AST_CONSTRAINT,
  AST_CONSTRAINT_QUERY,
  AST_DUMP_QUERY,
  AST_REPLICATION_QUERY,
  AST_LOCK_PATH_QUERY,
  AST_LOAD_CSV,
  AST_FREE_MEMORY_QUERY,
  AST_TRIGGER_QUERY,
  AST_ISOLATION_LEVEL_QUERY,
  AST_STORAGE_MODE_QUERY,
  AST_CREATE_SNAPSHOT_QUERY,
  AST_STREAM_QUERY,
  AST_SETTING_QUERY,
  AST_VERSION_QUERY,
  AST_FOREACH,
  AST_SHOW_CONFIG_QUERY,
  AST_ANALYZE_GRAPH_QUERY,
  AST_TRANSACTION_QUEUE_QUERY,
  AST_EXISTS,
  AST_CALL_SUBQUERY,
  AST_MULTI_DATABASE_QUERY,
  AST_SHOW_DATABASES,
  AST_EDGE_IMPORT_MODE_QUERY,
  AST_PATTERN_COMPREHENSION,
  AST_COORDINATOR_QUERY,
  // Symbol
  SYMBOL,
};

/// Type information on a C++ type.
///
/// You should embed this structure as a static constant member `kType` and make
/// sure you generate a unique ID for it. Also, if your type has inheritance,
/// you may want to add a `virtual utils::TypeInfo GetType();` method to get the
/// runtime type.
struct TypeInfo {
  /// Unique ID for the type.
  TypeId id;
  /// Pretty name of the type.
  const char *name;
  /// `TypeInfo *` for superclass of this type.
  /// Multiple inheritance is not supported.
  const TypeInfo *superclass{nullptr};
};

inline bool operator==(const TypeInfo &a, const TypeInfo &b) { return a.id == b.id; }
inline bool operator!=(const TypeInfo &a, const TypeInfo &b) { return a.id != b.id; }
inline bool operator<(const TypeInfo &a, const TypeInfo &b) { return a.id < b.id; }
inline bool operator<=(const TypeInfo &a, const TypeInfo &b) { return a.id <= b.id; }
inline bool operator>(const TypeInfo &a, const TypeInfo &b) { return a.id > b.id; }
inline bool operator>=(const TypeInfo &a, const TypeInfo &b) { return a.id >= b.id; }

/// Return true if `a` is subtype or the same type as `b`.
inline bool IsSubtype(const TypeInfo &a, const TypeInfo &b) {
  if (a == b) return true;
  const TypeInfo *super_a = a.superclass;
  while (super_a) {
    if (*super_a == b) return true;
    super_a = super_a->superclass;
  }
  return false;
}

template <class T>
bool IsSubtype(const T &a, const TypeInfo &b) {
  return IsSubtype(a.GetTypeInfo(), b);
}

/// Downcast `a` to `TDerived` using static_cast.
///
/// If `a` is `nullptr` or `TBase` is not a subtype of `TDerived`, then a
/// `nullptr` is returned.
///
/// This downcast is ill-formed if TBase is ambiguous, inaccessible, or virtual
/// base (or a base of a virtual base) of TDerived.
template <class TDerived, class TBase>
TDerived *Downcast(TBase *a) {
  if (!a) return nullptr;
  if (IsSubtype(*a, TDerived::kType)) return static_cast<TDerived *>(a);
  return nullptr;
}

}  // namespace memgraph::utils
