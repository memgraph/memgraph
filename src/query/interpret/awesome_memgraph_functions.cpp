// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/interpret/awesome_memgraph_functions.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <optional>
#include <random>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "query/auth_checker.hpp"
#include "query/common.hpp"
#include "query/exceptions.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/query_user.hpp"
#include "query/string_helpers.hpp"
#include "query/subgraph_graph_view.hpp"
#include "query/typed_value.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_graph_view.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/point_functions.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/case_insensitve_set.hpp"
#include "utils/pmr/string.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"
#include "utils/uuid.hpp"

#include "absl/container/flat_hash_map.h"

namespace memgraph::query {
namespace {

////////////////////////////////////////////////////////////////////////////////
// eDSL using template magic for describing a type of an awesome memgraph
// function and checking if the passed in arguments match the description.
//
// To use the type checking eDSL, you should put a `FType` invocation in the
// body of your awesome Memgraph function. `FType` takes type arguments as the
// description of the function type signature. Each runtime argument will be
// checked in order corresponding to given compile time type arguments. These
// type arguments can come in two forms:
//
//   * final, primitive type descriptor and
//   * combinator type descriptor.
//
// The primitive type descriptors are defined as empty structs, they are right
// below this documentation.
//
// Combinator type descriptors are defined as structs taking additional type
// parameters, you can find these further below in the implementation. Of
// primary interest are `Or` and `Optional` type combinators.
//
// With `Or` you can describe that an argument can be any of the types listed in
// `Or`. For example, `Or<Null, Bool, Integer>` allows an argument to be either
// `Null` or a boolean or an integer.
//
// The `Optional` combinator is used to define optional arguments to a function.
// These must come as the last positional arguments. Naturally, you can use `Or`
// inside `Optional`. So for example, `Optional<Or<Null, Bool>, Integer>`
// describes that a function takes 2 optional arguments. The 1st one must be
// either a `Null` or a boolean, while the 2nd one must be an integer. The type
// signature check will succeed in the following cases.
//
//   * No optional arguments were supplied.
//   * One argument was supplied and it passes `Or<Null, Bool>` check.
//   * Two arguments were supplied, the 1st one passes `Or<Null, Bool>` check
//     and the 2nd one passes `Integer` check.
//
// Runtime arguments to `FType` are: function name, pointer to arguments and the
// number of received arguments.
//
// Full example.
//
//     FType<Or<Null, String>, NonNegativeInteger,
//           Optional<NonNegativeInteger>>("substring", args, nargs);
//
// The above will check that `substring` function received the 2 required
// arguments. Optionally, the function may take a 3rd argument. The 1st argument
// must be either a `Null` or a character string. The 2nd argument is required
// to be a non-negative integer. If the 3rd argument was supplied, it will also
// be checked that it is a non-negative integer. If any of these checks fail,
// `FType` will throw a `QueryRuntimeException` with an appropriate error
// message.
////////////////////////////////////////////////////////////////////////////////

struct Null {};

struct Bool {};

struct Integer {};

struct PositiveInteger {};

struct NonZeroInteger {};

struct NonNegativeInteger {};

struct Double {};

struct Number {};

struct List {};

struct String {};

struct Map {};

struct Edge {};

struct Vertex {};

struct Path {};

struct Date {};

struct LocalTime {};

struct LocalDateTime {};

struct Duration {};

struct ZonedDateTime {};

struct Graph {};

struct Enum {};

struct Point2d {};

struct Point3d {};

template <class ArgType>
bool ArgIsType(const TypedValue &arg) {
  if constexpr (std::is_same_v<ArgType, Null>) {
    return arg.IsNull();
  } else if constexpr (std::is_same_v<ArgType, Bool>) {
    return arg.IsBool();
  } else if constexpr (std::is_same_v<ArgType, Integer>) {
    return arg.IsInt();
  } else if constexpr (std::is_same_v<ArgType, PositiveInteger>) {
    return arg.IsInt() && arg.ValueInt() > 0;
  } else if constexpr (std::is_same_v<ArgType, NonZeroInteger>) {
    return arg.IsInt() && arg.ValueInt() != 0;
  } else if constexpr (std::is_same_v<ArgType, NonNegativeInteger>) {
    return arg.IsInt() && arg.ValueInt() >= 0;
  } else if constexpr (std::is_same_v<ArgType, Double>) {
    return arg.IsDouble();
  } else if constexpr (std::is_same_v<ArgType, Number>) {
    return arg.IsNumeric();
  } else if constexpr (std::is_same_v<ArgType, List>) {
    return arg.IsList();
  } else if constexpr (std::is_same_v<ArgType, String>) {
    return arg.IsString();
  } else if constexpr (std::is_same_v<ArgType, Map>) {
    return arg.IsMap();
  } else if constexpr (std::is_same_v<ArgType, Vertex>) {
    return arg.IsVertex() || arg.IsVirtualNode();
  } else if constexpr (std::is_same_v<ArgType, Edge>) {
    return arg.IsEdge() || arg.IsVirtualEdge();
  } else if constexpr (std::is_same_v<ArgType, Path>) {
    return arg.IsPath();
  } else if constexpr (std::is_same_v<ArgType, Date>) {
    return arg.IsDate();
  } else if constexpr (std::is_same_v<ArgType, LocalTime>) {
    return arg.IsLocalTime();
  } else if constexpr (std::is_same_v<ArgType, LocalDateTime>) {
    return arg.IsLocalDateTime();
  } else if constexpr (std::is_same_v<ArgType, Duration>) {
    return arg.IsDuration();
  } else if constexpr (std::is_same_v<ArgType, ZonedDateTime>) {
    return arg.IsZonedDateTime();
  } else if constexpr (std::is_same_v<ArgType, Graph>) {
    return arg.IsGraph();
  } else if constexpr (std::is_same_v<ArgType, Enum>) {
    return arg.IsEnum();
  } else if constexpr (std::is_same_v<ArgType, Point2d>) {
    return arg.IsPoint2d();
  } else if constexpr (std::is_same_v<ArgType, Point3d>) {
    return arg.IsPoint3d();
  } else if constexpr (std::is_same_v<ArgType, void>) {
    return true;
  } else {
    static_assert(std::is_same_v<ArgType, Null>, "Unknown ArgType");
  }
  return false;
}

template <class ArgType>
constexpr const char *ArgTypeName() {
  // The type names returned should be standardized openCypher type names.
  // https://github.com/opencypher/openCypher/blob/master/docs/openCypher9.pdf
  if constexpr (std::is_same_v<ArgType, Null>) {
    return "null";
  } else if constexpr (std::is_same_v<ArgType, Bool>) {
    return "boolean";
  } else if constexpr (std::is_same_v<ArgType, Integer>) {
    return "integer";
  } else if constexpr (std::is_same_v<ArgType, PositiveInteger>) {
    return "positive integer";
  } else if constexpr (std::is_same_v<ArgType, NonZeroInteger>) {
    return "non-zero integer";
  } else if constexpr (std::is_same_v<ArgType, NonNegativeInteger>) {
    return "non-negative integer";
  } else if constexpr (std::is_same_v<ArgType, Double>) {
    return "float";
  } else if constexpr (std::is_same_v<ArgType, Number>) {
    return "number";
  } else if constexpr (std::is_same_v<ArgType, List>) {
    return "list";
  } else if constexpr (std::is_same_v<ArgType, String>) {
    return "string";
  } else if constexpr (std::is_same_v<ArgType, Map>) {
    return "map";
  } else if constexpr (std::is_same_v<ArgType, Vertex>) {
    return "node";
  } else if constexpr (std::is_same_v<ArgType, Edge>) {
    return "relationship";
  } else if constexpr (std::is_same_v<ArgType, Path>) {
    return "path";
  } else if constexpr (std::is_same_v<ArgType, void>) {
    return "void";
  } else if constexpr (std::is_same_v<ArgType, Date>) {
    return "Date";
  } else if constexpr (std::is_same_v<ArgType, LocalTime>) {
    return "LocalTime";
  } else if constexpr (std::is_same_v<ArgType, LocalDateTime>) {
    return "LocalDateTime";
  } else if constexpr (std::is_same_v<ArgType, Duration>) {
    return "Duration";
  } else if constexpr (std::is_same_v<ArgType, ZonedDateTime>) {
    return "ZonedDateTime";
  } else if constexpr (std::is_same_v<ArgType, Graph>) {
    return "graph";
  } else if constexpr (std::is_same_v<ArgType, Enum>) {
    return "Enum";
  } else if constexpr (std::is_same_v<ArgType, Point2d> || std::is_same_v<ArgType, Point3d>) {
    return "Point";
  } else {
    static_assert(std::is_same_v<ArgType, Null>, "Unknown ArgType");
  }
  return "<unknown-type>";
}

template <class... ArgType>
struct Or;

template <class ArgType>
struct Or<ArgType> {
  static bool Check(const TypedValue &arg) { return ArgIsType<ArgType>(arg); }

  static std::string TypeNames() { return ArgTypeName<ArgType>(); }
};

template <class ArgType, class... ArgTypes>
struct Or<ArgType, ArgTypes...> {
  static bool Check(const TypedValue &arg) {
    if (ArgIsType<ArgType>(arg)) return true;
    return Or<ArgTypes...>::Check(arg);
  }

  static std::string TypeNames() {
    if constexpr (sizeof...(ArgTypes) > 1) {
      return fmt::format("'{}', {}", ArgTypeName<ArgType>(), Or<ArgTypes...>::TypeNames());
    } else {
      return fmt::format("'{}' or '{}'", ArgTypeName<ArgType>(), Or<ArgTypes...>::TypeNames());
    }
  }
};

template <class T>
struct IsOrType {
  static constexpr bool value = false;
};

template <class... ArgTypes>
struct IsOrType<Or<ArgTypes...>> {
  static constexpr bool value = true;
};

template <class... ArgTypes>
struct Optional;

template <class ArgType>
struct Optional<ArgType> {
  static constexpr size_t size = 1;

  static void Check(const char *name, const TypedValue *args, int64_t nargs, int64_t pos) {
    if (nargs == 0) return;
    const TypedValue &arg = args[0];
    if constexpr (IsOrType<ArgType>::value) {
      if (!ArgType::Check(arg)) {
        throw QueryRuntimeException(
            "Optional '{}' argument at position {} must be either {}.", name, pos, ArgType::TypeNames());
      }
    } else {
      if (!ArgIsType<ArgType>(arg))
        throw QueryRuntimeException(
            "Optional '{}' argument at position {} must be '{}'.", name, pos, ArgTypeName<ArgType>());
    }
  }
};

template <class ArgType, class... ArgTypes>
struct Optional<ArgType, ArgTypes...> {
  static constexpr size_t size = 1 + sizeof...(ArgTypes);

  static void Check(const char *name, const TypedValue *args, int64_t nargs, int64_t pos) {
    if (nargs == 0) return;
    Optional<ArgType>::Check(name, args, nargs, pos);
    Optional<ArgTypes...>::Check(name, args + 1, nargs - 1, pos + 1);
  }
};

template <class T>
struct IsOptional {
  static constexpr bool value = false;
};

template <class... ArgTypes>
struct IsOptional<Optional<ArgTypes...>> {
  static constexpr bool value = true;
};

template <class ArgType, class... ArgTypes>
constexpr size_t FTypeRequiredArgs() {
  if constexpr (IsOptional<ArgType>::value) {
    static_assert(sizeof...(ArgTypes) == 0, "Optional arguments must be last!");
    return 0;
  } else if constexpr (sizeof...(ArgTypes) == 0) {
    return 1;
  } else {
    return 1U + FTypeRequiredArgs<ArgTypes...>();
  }
}

template <class ArgType, class... ArgTypes>
constexpr size_t FTypeOptionalArgs() {
  if constexpr (IsOptional<ArgType>::value) {
    static_assert(sizeof...(ArgTypes) == 0, "Optional arguments must be last!");
    return ArgType::size;
  } else if constexpr (sizeof...(ArgTypes) == 0) {
    return 0;
  } else {
    return FTypeOptionalArgs<ArgTypes...>();
  }
}

template <class ArgType, class... ArgTypes>
void FType(const char *name, const TypedValue *args, int64_t nargs, int64_t pos = 1) {
  if constexpr (std::is_same_v<ArgType, void>) {
    if (nargs != 0) {
      throw QueryRuntimeException("'{}' requires no arguments.", name);
    }
    return;
  }
  static constexpr int64_t required_args = FTypeRequiredArgs<ArgType, ArgTypes...>();
  static constexpr int64_t optional_args = FTypeOptionalArgs<ArgType, ArgTypes...>();
  static constexpr int64_t total_args = required_args + optional_args;
  if constexpr (optional_args > 0) {
    if (nargs < required_args || nargs > total_args) {
      throw QueryRuntimeException("'{}' requires between {} and {} arguments.", name, required_args, total_args);
    }
  } else {
    if (nargs != required_args) {
      throw QueryRuntimeException(
          "'{}' requires exactly {} {}.", name, required_args, required_args == 1 ? "argument" : "arguments");
    }
  }
  const TypedValue &arg = args[0];
  if constexpr (IsOrType<ArgType>::value) {
    if (!ArgType::Check(arg)) {
      throw QueryRuntimeException("'{}' argument at position {} must be either {}.", name, pos, ArgType::TypeNames());
    }
  } else if constexpr (IsOptional<ArgType>::value) {
    static_assert(sizeof...(ArgTypes) == 0, "Optional arguments must be last!");
    ArgType::Check(name, args, nargs, pos);
  } else {
    if (!ArgIsType<ArgType>(arg)) {
      throw QueryRuntimeException("'{}' argument at position {} must be '{}'", name, pos, ArgTypeName<ArgType>());
    }
  }
  if constexpr (sizeof...(ArgTypes) > 0) {
    FType<ArgTypes...>(name, args + 1, nargs - 1, pos + 1);
  }
}

////////////////////////////////////////////////////////////////////////////////
// END function type description eDSL
////////////////////////////////////////////////////////////////////////////////

// Predicate functions.
// Neo4j has all, any, exists, none, single
// Those functions are a little bit different since they take a filterExpression
// as an argument.
// There is all, any, none and single productions in opencypher grammar, but it
// will be trivial to also add exists.
// TODO: Implement this.

// Scalar functions.
// We don't have a way to implement id function since we don't store any. If it
// is really neccessary we could probably map vlist* to id.
// TODO: Implement length (it works on a path, but we didn't define path
// structure yet).
// TODO: Implement size(pattern), for example size((a)-[:X]-()) should return
// number of results of this pattern. I don't think we will ever do this.
// TODO: Implement rest of the list functions.
// TODO: Implement degrees, haversin, radians
// TODO: Implement spatial functions

TypedValue EndNode(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Edge>>("endNode", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  if (args[0].IsVirtualEdge()) return TypedValue(args[0].ValueVirtualEdge().To(), ctx.memory);
  return TypedValue(args[0].ValueEdge().To(), ctx.memory);
}

TypedValue Head(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("head", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValue(ctx.memory);
  return TypedValue(list[0], ctx.memory);
}

TypedValue Last(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("last", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValue(ctx.memory);
  return TypedValue(list.back(), ctx.memory);
}

constexpr auto allow_all_properties = [](storage::PropertyId) { return true; };

// One binding-aware, view-honoring, auth-filtered pass over a graph element's properties, shared by
// properties()/keys()/values(). Invokes `f(PropertyId, const PropertyValue &, bool allowed)` for each
// property, where `allowed` is the fine-grained READ permission for that property (always true when
// no auth checker is bound, and for virtual nodes/edges, which mint no real-graph privileges of their
// own). `count_hint(size_t)` is invoked once with the property count so a list-building caller can
// reserve (a map-building caller passes a no-op). A real accessor is iterated in its stored PropertyId
// order at `view`; a virtual node merges its origin (lazy read-through at `view`) with its overlay,
// hidden keys omitted; a virtual edge yields its overlay (no origin, so `view` is irrelevant). `fn`
// names the caller for error wording. Beyond the accessor's own result the helper makes no further
// copy of the property set, so the stored order is preserved and keys() never copies a value it
// discards. (propertySize() reads a single key lazily instead so an unread embedding is never
// materialized; see VirtualPropertySize.)
template <typename CountFn, typename F>
void ForEachElementProperty(const TypedValue &value, storage::View view, std::string_view fn,
                            FineGrainedAuthChecker const *checker, CountFn &&count_hint, F &&f) {
  auto emit = [&](const auto &props, auto &&is_allowed) {
    count_hint(props.size());
    for (const auto &[id, property_value] : props) f(id, property_value, is_allowed(id));
  };
  auto absorb = [&](auto &&maybe_props, auto &&is_allowed) {
    if (!maybe_props) {
      switch (maybe_props.error()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get {} from a deleted object.", fn);
        case storage::Error::NONEXISTENT_OBJECT:
          throw QueryRuntimeException("Trying to get {} from an object that doesn't exist.", fn);
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting {}.", fn);
      }
    }
    emit(*maybe_props, is_allowed);
  };
  if (value.IsVertex()) {
    auto const &vertex = value.ValueVertex();
    if (!checker) {
      absorb(vertex.Properties(view), allow_all_properties);
      return;
    }
    auto maybe_labels = vertex.Labels(view);
    if (!maybe_labels) ThrowVertexLabelsReadFailure(maybe_labels.error());
    absorb(vertex.Properties(view), [&](storage::PropertyId prop) {
      return checker->HasPropertyPermission(*maybe_labels, prop, AuthQuery::PropertyPermissionType::READ);
    });
  } else if (value.IsEdge()) {
    auto const &edge = value.ValueEdge();
    if (!checker) {
      absorb(edge.Properties(view), allow_all_properties);
      return;
    }
    absorb(edge.Properties(view), [&](storage::PropertyId prop) {
      return checker->HasPropertyPermission(edge.EdgeType(), prop, AuthQuery::PropertyPermissionType::READ);
    });
  } else if (value.IsVirtualNode()) {
    auto const &vnode = value.ValueVirtualNode();
    auto maybe_props = vnode.Properties(view);
    if (!maybe_props) throw QueryRuntimeException("Reading {} of a projected node's origin failed.", fn);
    auto const &origin = vnode.Origin();
    if (!checker || !origin) {
      // A synthetic node (no origin) mints no real-graph data; without a checker nothing is filtered.
      emit(*maybe_props, allow_all_properties);
    } else {
      // An overlay node reads through to its origin, so origin-backed properties get the origin's
      // per-property READ permission (over the origin's labels), matching a real vertex. An
      // overlay-bound override is the author's own value and is exempt.
      auto maybe_labels = origin->Labels(view);
      if (!maybe_labels) ThrowVertexLabelsReadFailure(maybe_labels.error());
      emit(*maybe_props, [&](storage::PropertyId prop) {
        return vnode.IsOverlayBound(prop) ||
               checker->HasPropertyPermission(*maybe_labels, prop, AuthQuery::PropertyPermissionType::READ);
      });
    }
  } else {
    emit(value.ValueVirtualEdge().Properties(), allow_all_properties);
  }
}

// NOTE: Denied properties appear as keys with null values. This differs from
// keys() and values(), which omit denied properties entirely. This divergence
// is by design.
TypedValue Properties(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("properties", args, nargs);
  auto *dba = ctx.db_accessor;
  const auto &value = args[0];
  if (value.IsNull()) return TypedValue(ctx.memory);
  TypedValue::TMap properties(ctx.memory);
  // Denied properties appear as keys with null values (see the NOTE above); keys()/values() omit them.
  ForEachElementProperty(
      value,
      ctx.view,
      "properties",
      ctx.auth_checker,
      [](size_t) {},
      [&](storage::PropertyId id, const storage::PropertyValue &pv, bool allowed) {
        auto key = TypedValue::TString(dba->PropertyToName(id), ctx.memory);
        if (allowed) {
          properties.emplace(std::move(key), TypedValue(pv, dba->GetStorageAccessor()->GetNameIdMapper(), ctx.memory));
        } else {
          properties.emplace(std::move(key), TypedValue(ctx.memory));
        }
      });
  return TypedValue(std::move(properties));
}

TypedValue RandomUuid(const TypedValue * /*args*/, int64_t /*nargs*/, const FunctionContext &ctx) {
  return TypedValue(utils::GenerateUUID(), ctx.memory);
}

TypedValue Size(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List, String, Map, Path>>("size", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsList()) {
    return TypedValue(static_cast<int64_t>(value.ValueList().size()), ctx.memory);
  } else if (value.IsString()) {
    return TypedValue(static_cast<int64_t>(value.ValueString().size()), ctx.memory);
  } else if (value.IsMap()) {
    // neo4j doesn't implement size for map, but I don't see a good reason not
    // to do it.
    return TypedValue(static_cast<int64_t>(value.ValueMap().size()), ctx.memory);
  } else {
    return TypedValue(static_cast<int64_t>(value.ValuePath().edges().size()), ctx.memory);
  }
}

namespace {

// The encoded byte size of a virtual element's property value, matching the metric
// storage::*::GetPropertySize reports for a real accessor: the value's footprint once encoded
// into a PropertyStore. The binding-aware read is the seam - an overlay node reads through to
// its origin for an origin-bound key, a hidden or absent key returns null and so contributes 0.
template <typename VirtualElement>
uint64_t VirtualPropertySize(const VirtualElement &element, storage::PropertyId key, storage::View view) {
  auto maybe_value = element.GetProperty(view, key);
  if (!maybe_value) {
    throw QueryRuntimeException("Reading a property of a projected element's origin failed.");
  }
  if (maybe_value->IsNull()) return 0;
  storage::PropertyStore store;
  store.SetProperty(key, *maybe_value);
  return store.PropertySize(key);
}

}  // namespace

TypedValue PropertySize(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>, Or<String>>("propertySize", args, nargs);

  auto *dba = ctx.db_accessor;

  const auto &property_name = args[1].ValueString();
  const auto maybe_property_id = dba->NameToPropertyIfExists(property_name);

  if (!maybe_property_id) {
    return TypedValue(0, ctx.memory);
  }

  uint64_t property_size = 0;
  const auto &graph_entity = args[0];
  if (graph_entity.IsVertex()) {
    property_size = graph_entity.ValueVertex().GetPropertySize(*maybe_property_id, ctx.view).value();
  } else if (graph_entity.IsEdge()) {
    property_size = graph_entity.ValueEdge().GetPropertySize(*maybe_property_id, ctx.view).value();
  } else if (graph_entity.IsVirtualNode()) {
    property_size = VirtualPropertySize(graph_entity.ValueVirtualNode(), *maybe_property_id, ctx.view);
  } else if (graph_entity.IsVirtualEdge()) {
    property_size = VirtualPropertySize(graph_entity.ValueVirtualEdge(), *maybe_property_id, ctx.view);
  }

  return TypedValue(static_cast<int64_t>(property_size), ctx.memory);
}

TypedValue StartNode(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Edge>>("startNode", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  if (args[0].IsVirtualEdge()) return TypedValue(args[0].ValueVirtualEdge().From(), ctx.memory);
  return TypedValue(args[0].ValueEdge().From(), ctx.memory);
}

namespace {

size_t UnwrapDegreeResult(storage::Result<size_t> maybe_degree) {
  if (!maybe_degree) {
    switch (maybe_degree.error()) {
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get degree of a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get degree of a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when getting node degree.");
    }
  }
  return *maybe_degree;
}

}  // namespace

TypedValue IsEmpty(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List, Map, String>>("isempty", args, nargs);
  auto const &arg = args[0];
  if (arg.IsNull()) return TypedValue(ctx.memory);
  switch (arg.type()) {
    using enum TypedValue::Type;
    case List:
      return TypedValue(arg.UnsafeValueList().empty(), ctx.memory);
    case Map:
      return TypedValue(arg.UnsafeValueMap().empty(), ctx.memory);
    case String:
      return TypedValue(arg.UnsafeValueString().empty(), ctx.memory);
    default:
      return TypedValue(ctx.memory);
  }
}

// Resolves a node's (in, out) degree over the ambient graph view: a projection
// node counts the projection's edges, a subgraph member counts only its member
// edges, and a real vertex on the identity view (or with no view bound) counts
// the real graph's edges.
std::pair<int64_t, int64_t> AmbientInOutDegree(const TypedValue &arg, const FunctionContext &ctx) {
  if (arg.IsVirtualNode()) {
    const auto gid = arg.ValueVirtualNode().Gid();
    if (auto *projection = dynamic_cast<VirtualGraphView *>(ctx.graph_view)) {
      return {static_cast<int64_t>(projection->InEdges(gid).size()),
              static_cast<int64_t>(projection->OutEdges(gid).size())};
    }
    // A virtual node outside a projection scope (a literal virtualNode(), no
    // VirtualGraphView bound) has no ambient topology.
    return {0, 0};
  }
  const auto &vertex = arg.ValueVertex();
  if (auto *subgraph = dynamic_cast<SubgraphGraphView *>(ctx.graph_view)) {
    const auto count_members = [&](auto maybe_edges) -> int64_t {
      if (!maybe_edges) throw QueryRuntimeException("Trying to get degree of a node that doesn't exist.");
      int64_t count = 0;
      for (const auto &edge : maybe_edges->edges) {
        if (subgraph->ContainsEdge(edge)) ++count;
      }
      return count;
    };
    return {count_members(vertex.InEdges(ctx.view)), count_members(vertex.OutEdges(ctx.view))};
  }
  if (dynamic_cast<VirtualGraphView *>(ctx.graph_view) != nullptr) {
    // A real vertex is not a node of a projection, so it has no edges in the
    // ambient projection graph. Reached when a real vertex is imported into a
    // CALL { USE <projection> ... } scope; the scope must not report the
    // vertex's real-graph degree.
    return {0, 0};
  }
  return {static_cast<int64_t>(UnwrapDegreeResult(vertex.InDegree(ctx.view))),
          static_cast<int64_t>(UnwrapDegreeResult(vertex.OutDegree(ctx.view)))};
}

TypedValue Degree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("degree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto [in_degree, out_degree] = AmbientInOutDegree(args[0], ctx);
  return TypedValue(in_degree + out_degree, ctx.memory);
}

TypedValue InDegree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("inDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(AmbientInOutDegree(args[0], ctx).first, ctx.memory);
}

TypedValue OutDegree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("outDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(AmbientInOutDegree(args[0], ctx).second, ctx.memory);
}

// Type-set shared by each strict to* and its *OrNull variant: strict throws on a rejected type, *OrNull
// returns null; a parse failure on an accepted type returns null in both.
using ToBooleanTypes = Or<Null, Bool, Integer, String>;  // Integer, not Number: toBoolean rejects floats.
using ToNumericTypes = Or<Null, Bool, Number, String>;   // shared by toFloat and toInteger.
using ToStringTypes =
    Or<Null, String, Number, Date, LocalTime, LocalDateTime, Duration, ZonedDateTime, Bool, Enum, Point2d, Point3d>;

TypedValue ToBoolean(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<ToBooleanTypes>("toBoolean", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsBool()) {
    return TypedValue(value.ValueBool(), ctx.memory);
  } else if (value.IsInt()) {
    return TypedValue(value.ValueInt() != 0L, ctx.memory);
  } else {
    auto s = utils::ToUpperCase(utils::Trim(value.ValueString()));
    if (s == "TRUE" || s == "T") return TypedValue(true, ctx.memory);
    if (s == "FALSE" || s == "F") return TypedValue(false, ctx.memory);
    // I think this is just stupid and that exception should be thrown, but
    // neo4j does it this way...
    return TypedValue(ctx.memory);
  }
}

TypedValue ToFloat(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<ToNumericTypes>("toFloat", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsInt()) {
    return TypedValue(static_cast<double>(value.ValueInt()), ctx.memory);
  } else if (value.IsBool()) {
    return TypedValue(value.ValueBool() ? 1.0 : 0.0, ctx.memory);
  } else if (value.IsDouble()) {
    return TypedValue(value, ctx.memory);
  } else {
    try {
      return TypedValue(utils::ParseDouble(utils::Trim(value.ValueString())), ctx.memory);
    } catch (const utils::BasicException &) {
      return TypedValue(ctx.memory);
    }
  }
}

TypedValue ToInteger(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<ToNumericTypes>("toInteger", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsBool()) {
    return TypedValue(value.ValueBool() ? 1L : 0L, ctx.memory);
  } else if (value.IsInt()) {
    return TypedValue(value, ctx.memory);
  } else if (value.IsDouble()) {
    return TypedValue(static_cast<int64_t>(value.ValueDouble()), ctx.memory);
  } else {
    try {
      // Yup, this is correct. String is valid if it has floating point
      // number, then it is parsed and converted to int.
      return TypedValue(static_cast<int64_t>(utils::ParseDouble(utils::Trim(value.ValueString()))), ctx.memory);
    } catch (const utils::BasicException &) {
      return TypedValue(ctx.memory);
    }
  }
}

// Null-on-rejected-type wrapper: accepted types delegate to the strict fn (still null on parse failure), rest -> null.
template <typename Types, TypedValue (*StrictFn)(const TypedValue *, int64_t, const FunctionContext &)>
TypedValue ConvertOrNull(const char *name, const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  if (nargs != 1) throw QueryRuntimeException("'{}' requires exactly 1 argument.", name);
  if (!Types::Check(args[0])) return TypedValue(ctx.memory);
  return StrictFn(args, nargs, ctx);
}

TypedValue ToBooleanOrNull(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return ConvertOrNull<ToBooleanTypes, ToBoolean>("toBooleanOrNull", args, nargs, ctx);
}

TypedValue ToFloatOrNull(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return ConvertOrNull<ToNumericTypes, ToFloat>("toFloatOrNull", args, nargs, ctx);
}

TypedValue ToIntegerOrNull(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return ConvertOrNull<ToNumericTypes, ToInteger>("toIntegerOrNull", args, nargs, ctx);
}

TypedValue ToBooleanList(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("toBooleanList", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  const auto &list = value.ValueList();
  TypedValue::TVector values(ctx.memory);
  values.reserve(list.size());
  for (const auto &element : list) values.emplace_back(ToBooleanOrNull(&element, 1, ctx));
  return TypedValue(std::move(values));
}

TypedValue ToFloatList(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("toFloatList", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  const auto &list = value.ValueList();
  TypedValue::TVector values(ctx.memory);
  values.reserve(list.size());
  for (const auto &element : list) values.emplace_back(ToFloatOrNull(&element, 1, ctx));
  return TypedValue(std::move(values));
}

TypedValue ToIntegerList(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("toIntegerList", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  const auto &list = value.ValueList();
  TypedValue::TVector values(ctx.memory);
  values.reserve(list.size());
  for (const auto &element : list) values.emplace_back(ToIntegerOrNull(&element, 1, ctx));
  return TypedValue(std::move(values));
}

TypedValue Type(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Edge>>("type", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  if (args[0].IsVirtualEdge()) return {args[0].ValueVirtualEdge().EdgeTypeName(), ctx.memory};
  return TypedValue(dba->EdgeTypeToName(args[0].ValueEdge().EdgeType()), ctx.memory);
}

TypedValue ValueType(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null,
           Bool,
           Integer,
           Double,
           String,
           List,
           Map,
           Vertex,
           Edge,
           Path,
           Date,
           LocalTime,
           LocalDateTime,
           ZonedDateTime,
           Duration,
           Graph,
           Enum,
           Point2d,
           Point3d>>("type", args, nargs);
  // The type names returned should be standardized openCypher type names.
  // https://github.com/opencypher/openCypher/blob/master/docs/openCypher9.pdf
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue("NULL", ctx.memory);
    case TypedValue::Type::Bool:
      return TypedValue("BOOLEAN", ctx.memory);
    case TypedValue::Type::Int:
      return TypedValue("INTEGER", ctx.memory);
    case TypedValue::Type::Double:
      return TypedValue("FLOAT", ctx.memory);
    case TypedValue::Type::String:
      return TypedValue("STRING", ctx.memory);
    case TypedValue::Type::List:
      return TypedValue("LIST", ctx.memory);
    case TypedValue::Type::Map:
      return TypedValue("MAP", ctx.memory);
    case TypedValue::Type::Vertex:
      return TypedValue("NODE", ctx.memory);
    case TypedValue::Type::Edge:
      return TypedValue("RELATIONSHIP", ctx.memory);
    case TypedValue::Type::VirtualEdge:
      return TypedValue("VIRTUAL_RELATIONSHIP", ctx.memory);
    case TypedValue::Type::VirtualNode:
      return TypedValue("VIRTUAL_NODE", ctx.memory);
    case TypedValue::Type::Path:
      return TypedValue("PATH", ctx.memory);
    case TypedValue::Type::Date:
      return TypedValue("DATE", ctx.memory);
    case TypedValue::Type::LocalTime:
      return TypedValue("LOCAL_TIME", ctx.memory);
    case TypedValue::Type::LocalDateTime:
      return TypedValue("LOCAL_DATE_TIME", ctx.memory);
    case TypedValue::Type::Duration:
      return TypedValue("DURATION", ctx.memory);
    case TypedValue::Type::Enum:
      return TypedValue("ENUM", ctx.memory);
    case TypedValue::Type::Point2d:
    case TypedValue::Type::Point3d:
      return TypedValue("POINT", ctx.memory);
    case TypedValue::Type::ZonedDateTime:
      return TypedValue("ZONED_DATE_TIME", ctx.memory);
    case TypedValue::Type::Graph:
      return TypedValue("GRAPH", ctx.memory);
    case TypedValue::Type::VirtualGraph:
      return TypedValue("VIRTUAL_GRAPH", ctx.memory);
    case TypedValue::Type::Function:
      throw QueryRuntimeException("Unknown value type! Please report an issue!");
  }
}

TypedValue Keys(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge, Map>>("keys", args, nargs);
  auto *dba = ctx.db_accessor;
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TVector keys(ctx.memory);
  if (value.IsMap()) {
    for (const auto &[string_key, map_value] : value.ValueMap()) keys.emplace_back(string_key);
    return TypedValue(std::move(keys));
  }
  ForEachElementProperty(
      value,
      ctx.view,
      "keys",
      ctx.auth_checker,
      [&](size_t n) { keys.reserve(n); },
      [&](storage::PropertyId id, const storage::PropertyValue &, bool allowed) {
        if (allowed) keys.emplace_back(TypedValue(dba->PropertyToName(id), ctx.memory));
      });
  return TypedValue(std::move(keys));
}

TypedValue Values(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge, Map>>("values", args, nargs);
  auto *dba = ctx.db_accessor;
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TVector values(ctx.memory);
  if (value.IsMap()) {
    for (const auto &[string_key, map_value] : value.ValueMap()) values.emplace_back(map_value);
    return TypedValue(std::move(values));
  }
  ForEachElementProperty(
      value,
      ctx.view,
      "values",
      ctx.auth_checker,
      [&](size_t n) { values.reserve(n); },
      [&](storage::PropertyId, const storage::PropertyValue &pv, bool allowed) {
        if (allowed) values.emplace_back(TypedValue(pv, dba->GetStorageAccessor()->GetNameIdMapper(), ctx.memory));
      });
  return TypedValue(std::move(values));
}

TypedValue Labels(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("labels", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  if (args[0].IsVirtualNode()) {
    const auto &node_labels = args[0].ValueVirtualNode().Labels();
    TypedValue::TVector labels(ctx.memory);
    labels.reserve(node_labels.size());
    std::ranges::transform(
        node_labels, std::back_inserter(labels), [&](const auto &label) { return TypedValue(label, ctx.memory); });
    return TypedValue(std::move(labels));
  }
  TypedValue::TVector labels(ctx.memory);
  auto maybe_labels = args[0].ValueVertex().Labels(ctx.view);
  if (!maybe_labels) {
    ThrowVertexLabelsReadFailure(maybe_labels.error());
  }
  for (const auto &label : *maybe_labels) {
    labels.emplace_back(dba->LabelToName(label));
  }
  return TypedValue(std::move(labels));
}

TypedValue Nodes(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Path>>("nodes", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertices = args[0].ValuePath().vertices();
  TypedValue::TVector values(ctx.memory);
  values.reserve(vertices.size());
  for (const auto &v : vertices) values.emplace_back(v);
  return TypedValue(std::move(values));
}

TypedValue Relationships(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Path>>("relationships", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &edges = args[0].ValuePath().edges();
  TypedValue::TVector values(ctx.memory);
  values.reserve(edges.size());
  for (const auto &e : edges) values.emplace_back(e);
  return TypedValue(std::move(values));
}

TypedValue Range(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Integer>, Or<Null, Integer>, Optional<Or<Null, NonZeroInteger>>>("range", args, nargs);
  for (int64_t i = 0; i < nargs; ++i)
    if (args[i].IsNull()) return TypedValue(ctx.memory);
  auto lbound = args[0].ValueInt();
  auto rbound = args[1].ValueInt();
  int64_t step = nargs == 3 ? args[2].ValueInt() : 1;
  TypedValue::TVector list(ctx.memory);
  if (lbound <= rbound && step > 0) {
    int64_t n = ((rbound - lbound + 1) + (step - 1)) / step;
    list.reserve(n);
    for (auto i = lbound; i <= rbound; i += step) {
      list.emplace_back(i);
    }
    MG_ASSERT(list.size() == n);
  } else if (lbound >= rbound && step < 0) {
    int64_t n = ((lbound - rbound + 1) + (-step - 1)) / -step;
    list.reserve(n);
    for (auto i = lbound; i >= rbound; i += step) {
      list.emplace_back(i);
    }
    MG_ASSERT(list.size() == n);
  }
  return TypedValue(std::move(list));
}

TypedValue Tail(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("tail", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  TypedValue::TVector list(args[0].ValueList(), ctx.memory);
  if (list.empty()) return TypedValue(std::move(list));
  list.erase(list.begin());
  return TypedValue(std::move(list));
}

TypedValue ToSet(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("toSet", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  const auto &elements = value.ValueList();
  using unique_collection = utils::pmr::unordered_set<TypedValue, TypedValue::Hash, TypedValue::BoolEqual>;
  auto unique_elements = unique_collection(
      elements.cbegin(), elements.cend(), elements.size() * 2, TypedValue::Hash{}, TypedValue::BoolEqual{}, ctx.memory);
  return TypedValue{TypedValue::TVector(
      std::make_move_iterator(unique_elements.begin()), std::make_move_iterator(unique_elements.end()), ctx.memory)};
}

TypedValue UniformSample(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>, Or<Null, NonNegativeInteger>>("uniformSample", args, nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &population = args[0].ValueList();
  auto population_size = population.size();
  if (population_size == 0) return TypedValue(ctx.memory);
  auto desired_length = args[1].ValueInt();
  std::uniform_int_distribution<uint64_t> rand_dist{0, population_size - 1};
  TypedValue::TVector sampled(ctx.memory);
  sampled.reserve(desired_length);
  for (int64_t i = 0; i < desired_length; ++i) {
    sampled.emplace_back(population[rand_dist(pseudo_rand_gen_)]);
  }
  return TypedValue(std::move(sampled));
}

TypedValue Abs(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Number>>("abs", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsInt()) {
    return TypedValue(std::abs(value.ValueInt()), ctx.memory);
  } else {
    return TypedValue(std::abs(value.ValueDouble()), ctx.memory);
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                               \
  TypedValue name(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) { \
    FType<Or<Null, Number>>(#lowercased_name, args, nargs);                            \
    const auto &value = args[0];                                                       \
    if (value.IsNull()) {                                                              \
      return TypedValue(ctx.memory);                                                   \
    } else if (value.IsInt()) {                                                        \
      return TypedValue(lowercased_name(value.ValueInt()), ctx.memory);                \
    } else {                                                                           \
      return TypedValue(lowercased_name(value.ValueDouble()), ctx.memory);             \
    }                                                                                  \
  }

WRAP_CMATH_FLOAT_FUNCTION(Ceil, ceil)
WRAP_CMATH_FLOAT_FUNCTION(Floor, floor)
// We are not completely compatible with neoj4 in this function because,
// neo4j rounds -0.5, -1.5, -2.5... to 0, -1, -2...
WRAP_CMATH_FLOAT_FUNCTION(Round, round)
WRAP_CMATH_FLOAT_FUNCTION(Exp, exp)
WRAP_CMATH_FLOAT_FUNCTION(Log, log)
WRAP_CMATH_FLOAT_FUNCTION(Log10, log10)
WRAP_CMATH_FLOAT_FUNCTION(Sqrt, sqrt)
WRAP_CMATH_FLOAT_FUNCTION(Acos, acos)
WRAP_CMATH_FLOAT_FUNCTION(Asin, asin)
WRAP_CMATH_FLOAT_FUNCTION(Atan, atan)
WRAP_CMATH_FLOAT_FUNCTION(Cos, cos)
WRAP_CMATH_FLOAT_FUNCTION(Sin, sin)
WRAP_CMATH_FLOAT_FUNCTION(Tan, tan)

#undef WRAP_CMATH_FLOAT_FUNCTION

TypedValue Atan2(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Number>, Or<Null, Number>>("atan2", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  auto to_double = [](const TypedValue &t) -> double {
    if (t.IsInt()) {
      return t.ValueInt();
    } else {
      return t.ValueDouble();
    }
  };
  double y = to_double(args[0]);
  double x = to_double(args[1]);
  return TypedValue(atan2(y, x), ctx.memory);
}

TypedValue Sign(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Number>>("sign", args, nargs);
  auto sign = [&](auto x) { return TypedValue((0 < x) - (x < 0), ctx.memory); };
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsInt()) {
    return sign(value.ValueInt());
  } else {
    return sign(value.ValueDouble());
  }
}

TypedValue E(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<void>("e", args, nargs);
  return TypedValue(M_E, ctx.memory);
}

TypedValue Pi(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<void>("pi", args, nargs);
  return TypedValue(M_PI, ctx.memory);
}

TypedValue Rand(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<void>("rand", args, nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  return TypedValue(rand_dist_(pseudo_rand_gen_), ctx.memory);
}

template <class TPredicate>
TypedValue StringMatchOperator(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, Or<Null, String>>(TPredicate::name, args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &s1 = args[0].ValueString();
  const auto &s2 = args[1].ValueString();
  return TypedValue(TPredicate{}(s1, s2), ctx.memory);
}

// Check if s1 starts with s2.
struct StartsWithPredicate {
  static constexpr const char *name = "startsWith";

  bool operator()(const TypedValue::TString &s1, const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.begin(), s2.end(), s1.begin());
  }
};

auto StartsWith = StringMatchOperator<StartsWithPredicate>;

// Check if s1 ends with s2.
struct EndsWithPredicate {
  static constexpr const char *name = "endsWith";

  bool operator()(const TypedValue::TString &s1, const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
  }
};

auto EndsWith = StringMatchOperator<EndsWithPredicate>;

// Check if s1 contains s2.
struct ContainsPredicate {
  static constexpr const char *name = "contains";

  bool operator()(const TypedValue::TString &s1, const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return s1.contains(s2);
  }
};

auto Contains = StringMatchOperator<ContainsPredicate>;

TypedValue Assert(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Bool, Optional<String>>("assert", args, nargs);
  if (!args[0].ValueBool()) {
    std::string message("Assertion failed");
    if (nargs == 2) {
      message += ": ";
      message += args[1].ValueString();
    }
    message += ".";
    throw QueryRuntimeException(message);
  }
  return TypedValue(args[0], ctx.memory);
}

TypedValue Counter(const TypedValue *args, int64_t nargs, const FunctionContext &context) {
  FType<String, Integer, Optional<NonZeroInteger>>("counter", args, nargs);
  int64_t step = 1;
  if (nargs == 3) {
    step = args[2].ValueInt();
  }

  auto [it, inserted] = context.counters->emplace(args[0].ValueString(), args[1].ValueInt());
  auto value = it->second;
  it->second += step;

  return TypedValue(value, context.memory);
}

// The query-local external id for a synthetic Gid, or the raw synthetic id when there is no mapper
// (a path with no query context).
int64_t ExternalSyntheticId(storage::Gid gid, const FunctionContext &ctx) {
  if (ctx.synthetic_id_mapper) return ctx.synthetic_id_mapper->ExternalId(gid);
  return gid.AsInt();
}

TypedValue IdOf(const TypedValue &arg, const FunctionContext &ctx) {
  if (arg.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (arg.IsVirtualNode()) {
    const auto &vnode = arg.ValueVirtualNode();
    // An overlay node (one derived over a real vertex) reports its origin's real id, so id() is the
    // entity's identity in the real graph. A synthetic node has no origin and reports its query-local
    // external id.
    if (vnode.HasOrigin()) return TypedValue(vnode.Origin()->CypherId(), ctx.memory);
    return TypedValue(ExternalSyntheticId(vnode.Gid(), ctx), ctx.memory);
  } else if (arg.IsVertex()) {
    return TypedValue(arg.ValueVertex().CypherId(), ctx.memory);
  } else if (arg.IsVirtualEdge()) {
    return TypedValue(ExternalSyntheticId(arg.ValueVirtualEdge().Gid(), ctx), ctx.memory);
  } else {
    return TypedValue(arg.ValueEdge().CypherId(), ctx.memory);
  }
}

TypedValue Id(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("id", args, nargs);
  return IdOf(args[0], ctx);
}

// The overlay-local id: the query-local external id for a virtual node or edge (whether synthetic or
// an overlay over a real entity), and null for a real entity that has no overlay identity.
TypedValue VirtualIdOf(const TypedValue &arg, const FunctionContext &ctx) {
  if (arg.IsVirtualNode()) {
    return TypedValue(ExternalSyntheticId(arg.ValueVirtualNode().Gid(), ctx), ctx.memory);
  } else if (arg.IsVirtualEdge()) {
    return TypedValue(ExternalSyntheticId(arg.ValueVirtualEdge().Gid(), ctx), ctx.memory);
  }
  return TypedValue(ctx.memory);
}

TypedValue VirtualId(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("virtual_id", args, nargs);
  return VirtualIdOf(args[0], ctx);
}

// Returns the id as a string for compatibility with external integrations.
TypedValue ElementId(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("elementId", args, nargs);
  auto id = IdOf(args[0], ctx);
  if (id.IsNull()) return id;
  return TypedValue(std::to_string(id.ValueInt()), ctx.memory);
}

// Conversion core shared by toString and toStringOrNull; nullopt iff the enum can't be resolved to a name.
std::optional<TypedValue> TryToString(const TypedValue &arg, const FunctionContext &ctx) {
  using enum TypedValue::Type;
  switch (arg.type()) {
    case Null: {
      return TypedValue(ctx.memory);
    }

    case String: {
      return TypedValue(arg, ctx.memory);
    }

    case Int: {
      // TODO: This is making a pointless copy of std::string, we may want to
      // use a different conversion to string
      return TypedValue(std::to_string(arg.ValueInt()), ctx.memory);
    }

    case Double: {
      return TypedValue(memgraph::utils::DoubleToString(arg.ValueDouble()), ctx.memory);
    }

    case Date: {
      return TypedValue(arg.ValueDate().ToString(), ctx.memory);
    }

    case LocalTime: {
      return TypedValue(arg.ValueLocalTime().ToString(), ctx.memory);
    }

    case LocalDateTime: {
      return TypedValue(arg.ValueLocalDateTime().ToString(), ctx.memory);
    }

    case Duration: {
      return TypedValue(arg.ValueDuration().ToString(), ctx.memory);
    }

    case ZonedDateTime: {
      return TypedValue(arg.ValueZonedDateTime().ToString(), ctx.memory);
    }

    case Enum: {
      auto opt_str = ctx.db_accessor->EnumToName(arg.ValueEnum());
      if (!opt_str) return std::nullopt;
      return TypedValue(*opt_str, ctx.memory);
    }

    case Bool: {
      return TypedValue(arg.ValueBool() ? "true" : "false", ctx.memory);
    }

    case Point2d: {
      return TypedValue(CypherConstructionFor(arg.ValuePoint2d()), ctx.memory);
    }

    case Point3d: {
      return TypedValue(CypherConstructionFor(arg.ValuePoint3d()), ctx.memory);
    }

    case List:
    case Map:
    case Vertex:
    case Edge:
    case VirtualEdge:
    case VirtualNode:
    case Path:
    case Graph:
    case VirtualGraph:
    case Function: {
      MG_ASSERT(false, "unexpected TypedValue::Type");
    }
  }
}

TypedValue ToString(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<ToStringTypes>("toString", args, nargs);
  auto converted = TryToString(args[0], ctx);
  if (!converted) throw QueryRuntimeException("'toString' the given enum can't be converted to a string");
  return *std::move(converted);
}

TypedValue ToStringOrNull(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  if (nargs != 1) {
    throw QueryRuntimeException("'toStringOrNull' requires exactly 1 argument.");
  }
  // Rejected type or unconvertible value -> null.
  if (!ToStringTypes::Check(args[0])) return TypedValue(ctx.memory);
  auto converted = TryToString(args[0], ctx);
  return converted ? *std::move(converted) : TypedValue(ctx.memory);
}

TypedValue ToStringList(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, List>>("toStringList", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  const auto &list = value.ValueList();
  TypedValue::TVector values(ctx.memory);
  values.reserve(list.size());
  // Per-element via ToStringOrNull (not ToString): non-stringifiable elements become null.
  for (const auto &element : list) values.emplace_back(ToStringOrNull(&element, 1, ctx));
  return TypedValue(std::move(values));
}

TypedValue Timestamp(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Date, LocalTime, LocalDateTime, ZonedDateTime, Duration>>>("timestamp", args, nargs);

  if (nargs == 0) {
    return TypedValue(ctx.timestamp, ctx.memory);
  }

  const auto &arg = *args;
  if (arg.IsDate()) {
    return TypedValue(arg.ValueDate().MicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsLocalTime()) {
    return TypedValue(arg.ValueLocalTime().MicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsLocalDateTime()) {
    // Timestamps need to be in system time (UTC)
    return TypedValue(arg.ValueLocalDateTime().SysMicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsDuration()) {
    return TypedValue(arg.ValueDuration().microseconds, ctx.memory);
  }
  if (arg.IsZonedDateTime()) {
    return TypedValue(arg.ValueZonedDateTime().SysMicrosecondsSinceEpoch().count(), ctx.memory);
  }
  return TypedValue(ctx.timestamp, ctx.memory);
}

TypedValue Left(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, Or<Null, NonNegativeInteger>>("left", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(utils::Substr(args[0].ValueString(), 0, args[1].ValueInt()), ctx.memory);
}

TypedValue Right(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, Or<Null, NonNegativeInteger>>("right", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &str = args[0].ValueString();
  auto len = args[1].ValueInt();
  return len <= str.size() ? TypedValue(utils::Substr(str, str.size() - len, len), ctx.memory)
                           : TypedValue(str, ctx.memory);
}

TypedValue CallStringFunction(const TypedValue *args, int64_t nargs, utils::MemoryResource *memory, const char *name,
                              std::function<TypedValue::TString(const TypedValue::TString &)> fun) {
  FType<Or<Null, String>>(name, args, nargs);
  if (args[0].IsNull()) return TypedValue(memory);
  return TypedValue(fun(args[0].ValueString()), memory);
}

TypedValue LTrim(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "lTrim", [&](const auto &str) {
    return TypedValue::TString(utils::LTrim(str), ctx.memory);
  });
}

TypedValue RTrim(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "rTrim", [&](const auto &str) {
    return TypedValue::TString(utils::RTrim(str), ctx.memory);
  });
}

TypedValue Trim(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "trim", [&](const auto &str) {
    return TypedValue::TString(utils::Trim(str), ctx.memory);
  });
}

TypedValue Reverse(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(
      args, nargs, ctx.memory, "reverse", [&](const auto &str) { return utils::Reversed(str, ctx.memory); });
}

TypedValue ToLower(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "toLower", [&](const auto &str) {
    TypedValue::TString res(ctx.memory);
    utils::ToLowerCase(&res, str);
    return res;
  });
}

TypedValue ToUpper(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "toUpper", [&](const auto &str) {
    TypedValue::TString res(ctx.memory);
    utils::ToUpperCase(&res, str);
    return res;
  });
}

TypedValue Replace(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, Or<Null, String>, Or<Null, String>>("replace", args, nargs);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TString replaced(ctx.memory);
  utils::Replace(&replaced, args[0].ValueString(), args[1].ValueString(), args[2].ValueString());
  return TypedValue(std::move(replaced));
}

TypedValue Split(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, Or<Null, String>>("split", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TVector result(ctx.memory);
  utils::Split(&result, args[0].ValueString(), args[1].ValueString());
  return TypedValue(std::move(result));
}

TypedValue Substring(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String>, NonNegativeInteger, Optional<NonNegativeInteger>>("substring", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &str = args[0].ValueString();
  auto start = args[1].ValueInt();
  if (nargs == 2) return TypedValue(utils::Substr(str, start), ctx.memory);
  auto len = args[2].ValueInt();
  return TypedValue(utils::Substr(str, start, len), ctx.memory);
}

TypedValue ToByteString(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<String>("toByteString", args, nargs);
  const auto &str = args[0].ValueString();
  if (str.empty()) return TypedValue("", ctx.memory);
  if (!utils::StartsWith(str, "0x") && !utils::StartsWith(str, "0X")) {
    throw QueryRuntimeException("'toByteString' argument must start with '0x'");
  }
  const auto &hex_str = utils::Substr(str, 2);
  auto read_hex = [](const char ch) -> unsigned char {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    throw QueryRuntimeException("'toByteString' argument has an invalid character '{}'", ch);
  };
  utils::pmr::string bytes(ctx.memory);
  bytes.reserve((1 + hex_str.size()) / 2);
  size_t i = 0;
  // Treat odd length hex string as having a leading zero.
  if (hex_str.size() % 2) bytes.append(1, read_hex(hex_str[i++]));
  for (; i < hex_str.size(); i += 2) {
    unsigned char byte = read_hex(hex_str[i]) * 16U + read_hex(hex_str[i + 1]);
    // MemcpyCast in case we are converting to a signed value, so as to avoid
    // undefined behaviour.
    bytes.append(1, std::bit_cast<decltype(bytes)::value_type>(byte));
  }
  return TypedValue(std::move(bytes));
}

TypedValue FromByteString(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<String, Optional<PositiveInteger>>("fromByteString", args, nargs);
  const auto &bytes = args[0].ValueString();
  if (bytes.empty()) return TypedValue("", ctx.memory);
  size_t min_length = bytes.size();
  if (nargs == 2) min_length = std::max(min_length, static_cast<size_t>(args[1].ValueInt()));
  utils::pmr::string str(ctx.memory);
  str.reserve(min_length * 2 + 2);
  str.append("0x");
  for (size_t pad = 0; pad < min_length - bytes.size(); ++pad) str.append(2, '0');
  // Convert the bytes to a character string in hex representation.
  // Unfortunately, we don't know whether the default `char` is signed or
  // unsigned, so we have to work around any potential undefined behaviour when
  // conversions between the 2 occur. That's why this function is more
  // complicated than it should be.
  auto to_hex = [](const unsigned char val) -> char {
    unsigned char ch = val < 10U ? static_cast<unsigned char>('0') + val : static_cast<unsigned char>('a') + val - 10U;
    return std::bit_cast<char>(ch);
  };
  for (unsigned char byte : bytes) {
    str.append(1, to_hex(byte / 16U));
    str.append(1, to_hex(byte % 16U));
  }
  return TypedValue(std::move(str));
}

template <typename T>
concept IsNumberOrInteger = utils::SameAsAnyOf<T, Number, Integer>;

template <IsNumberOrInteger ArgType>
bool MapNumericParameters(auto &parameter_mappings, const auto &input_parameters) {
  bool has_mapped_any_field{false};
  for (const auto &[key, value] : input_parameters) {
    if (auto it = parameter_mappings.find(key); it != parameter_mappings.end()) {
      has_mapped_any_field = true;
      if (value.IsInt()) {
        *it->second = value.ValueInt();
      } else if (std::is_same_v<ArgType, Number> && value.IsDouble()) {
        *it->second = value.ValueDouble();
      } else {
        std::string_view error = std::is_same_v<ArgType, Integer> ? "an integer." : "a numeric value.";
        throw QueryRuntimeException("Invalid value for key '{}'. Expected {}", key, error);
      }
    } else {
      throw QueryRuntimeException("Unknown key '{}'.", key);
    }
  }

  return has_mapped_any_field;
}

TypedValue Date(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Null, String, Map, struct Date, LocalDateTime, ZonedDateTime>>>("date", args, nargs);
  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp).date(), ctx.memory);
  }

  if (args[0].IsNull()) {
    return TypedValue(ctx.memory);
  }

  if (args[0].IsDate()) {
    return args[0];
  }

  if (args[0].IsLocalDateTime()) {
    return TypedValue(utils::Date{args[0].ValueLocalDateTime().date()}, ctx.memory);
  }

  if (args[0].IsZonedDateTime()) {
    auto const &zdt{args[0].ValueZonedDateTime()};
    return TypedValue(
        utils::Date{{zdt.LocalYear(), static_cast<int64_t>(zdt.LocalMonth()), static_cast<int64_t>(zdt.LocalDay())}},
        ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[date_parameters, is_extended] = utils::ParseDateParameters(args[0].ValueString());
    return TypedValue(utils::Date(date_parameters), ctx.memory);
  }

  utils::DateParameters date_parameters;

  using namespace std::literals;
  std::unordered_map parameter_mappings = {std::pair{"year"sv, &date_parameters.year},
                                           std::pair{"month"sv, &date_parameters.month},
                                           std::pair{"day"sv, &date_parameters.day}};

  MapNumericParameters<Integer>(parameter_mappings, args[0].ValueMap());
  return TypedValue(utils::Date(date_parameters), ctx.memory);
}

TypedValue LocalTime(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Null, String, Map, struct LocalTime, LocalDateTime, ZonedDateTime>>>("localtime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp).local_time(), ctx.memory);
  }

  if (args[0].IsNull()) {
    return TypedValue(ctx.memory);
  }

  if (args[0].IsLocalTime()) {
    return args[0];
  }

  if (args[0].IsLocalDateTime()) {
    return TypedValue(utils::LocalTime{args[0].ValueLocalDateTime().local_time()}, ctx.memory);
  }

  if (args[0].IsZonedDateTime()) {
    auto const &zdt{args[0].ValueZonedDateTime()};
    return TypedValue(utils::LocalTime{{.hour = zdt.LocalHour(),
                                        .minute = zdt.LocalMinute(),
                                        .second = zdt.LocalSecond(),
                                        .millisecond = zdt.LocalMillisecond(),
                                        .microsecond = zdt.LocalMicrosecond()}},
                      ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[local_time_parameters, is_extended] = utils::ParseLocalTimeParameters(args[0].ValueString());
    return TypedValue(utils::LocalTime(local_time_parameters), ctx.memory);
  }

  utils::LocalTimeParameters local_time_parameters;

  using namespace std::literals;
  std::unordered_map parameter_mappings{
      std::pair{"hour"sv, &local_time_parameters.hour},
      std::pair{"minute"sv, &local_time_parameters.minute},
      std::pair{"second"sv, &local_time_parameters.second},
      std::pair{"millisecond"sv, &local_time_parameters.millisecond},
      std::pair{"microsecond"sv, &local_time_parameters.microsecond},
  };

  MapNumericParameters<Integer>(parameter_mappings, args[0].ValueMap());
  return TypedValue(utils::LocalTime(local_time_parameters), ctx.memory);
}

TypedValue LocalDateTime(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Null, String, Map, struct LocalDateTime, ZonedDateTime>>>("localdatetime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp), ctx.memory);
  }

  if (args[0].IsNull()) {
    return TypedValue(ctx.memory);
  }

  if (args[0].IsLocalDateTime()) {
    return args[0];
  }

  if (args[0].IsZonedDateTime()) {
    auto const &zdt{args[0].ValueZonedDateTime()};
    return TypedValue(
        utils::LocalDateTime{
            {zdt.LocalYear(), static_cast<int64_t>(zdt.LocalMonth()), static_cast<int64_t>(zdt.LocalDay())},
            {zdt.LocalHour(), zdt.LocalMinute(), zdt.LocalSecond(), zdt.LocalMillisecond(), zdt.LocalMicrosecond()}},
        ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[date_parameters, local_time_parameters] = utils::ParseLocalDateTimeParameters(args[0].ValueString());
    return TypedValue(utils::LocalDateTime(date_parameters, local_time_parameters), ctx.memory);
  }

  utils::DateParameters date_parameters;
  utils::LocalTimeParameters local_time_parameters;
  using namespace std::literals;
  std::unordered_map parameter_mappings{
      std::pair{"year"sv, &date_parameters.year},
      std::pair{"month"sv, &date_parameters.month},
      std::pair{"day"sv, &date_parameters.day},
      std::pair{"hour"sv, &local_time_parameters.hour},
      std::pair{"minute"sv, &local_time_parameters.minute},
      std::pair{"second"sv, &local_time_parameters.second},
      std::pair{"millisecond"sv, &local_time_parameters.millisecond},
      std::pair{"microsecond"sv, &local_time_parameters.microsecond},
  };

  MapNumericParameters<Integer>(parameter_mappings, args[0].ValueMap());
  return TypedValue(utils::LocalDateTime(date_parameters, local_time_parameters), ctx.memory);
}

TypedValue Duration(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String, Map>>("duration", args, nargs);

  if (args[0].IsNull()) {
    return TypedValue(ctx.memory);
  }

  if (args[0].IsString()) {
    return TypedValue(utils::Duration(utils::ParseDurationParameters(args[0].ValueString())), ctx.memory);
  }

  utils::DurationParameters duration_parameters;
  using namespace std::literals;
  std::unordered_map parameter_mappings{std::pair{"day"sv, &duration_parameters.day},
                                        std::pair{"hour"sv, &duration_parameters.hour},
                                        std::pair{"minute"sv, &duration_parameters.minute},
                                        std::pair{"second"sv, &duration_parameters.second},
                                        std::pair{"millisecond"sv, &duration_parameters.millisecond},
                                        std::pair{"microsecond"sv, &duration_parameters.microsecond}};
  MapNumericParameters<Number>(parameter_mappings, args[0].ValueMap());
  return TypedValue(utils::Duration(duration_parameters), ctx.memory);
}

utils::Timezone GetTimezone(const memgraph::query::TypedValue::TMap &input_parameters, const FunctionContext &ctx) {
  const utils::pmr::string timezone("timezone", ctx.memory);
  if (!input_parameters.contains(timezone)) {
    return utils::DefaultTimezone();
  }
  const auto &value = input_parameters.at(timezone);
  if (value.IsString()) {
    return utils::Timezone(value.ValueString());
  }
  if (value.IsInt()) {
    return utils::Timezone(std::chrono::minutes{value.ValueInt()});
  }
  throw QueryRuntimeException("Invalid value for key 'timezone'. Expected an integer or a string");
}

// Refers to ZonedDateTime; called DateTime for compatibility with Cypher
TypedValue DateTime(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Null, String, Map, ZonedDateTime>>>("datetime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::ZonedDateTime(utils::AsSysTime(ctx.timestamp), utils::DefaultTimezone()), ctx.memory);
  }

  if (args[0].IsNull()) {
    return TypedValue(ctx.memory);
  }

  if (args[0].IsZonedDateTime()) {
    return args[0];
  }

  if (args[0].IsString()) {
    const auto &zoned_date_time_parameters = utils::ParseZonedDateTimeParameters(args[0].ValueString());
    return TypedValue(utils::ZonedDateTime(zoned_date_time_parameters), ctx.memory);
  }

  utils::DateParameters date_parameters{};
  utils::LocalTimeParameters time_parameters{};
  using namespace std::literals;
  std::unordered_map date_parameter_mappings{
      std::pair{"year"sv, &date_parameters.year},
      std::pair{"month"sv, &date_parameters.month},
      std::pair{"day"sv, &date_parameters.day},
      std::pair{"hour"sv, &time_parameters.hour},
      std::pair{"minute"sv, &time_parameters.minute},
      std::pair{"second"sv, &time_parameters.second},
      std::pair{"millisecond"sv, &time_parameters.millisecond},
      std::pair{"microsecond"sv, &time_parameters.microsecond},
  };

  auto fields = args[0].ValueMap();
  const auto timezone = GetTimezone(fields, ctx);
  const utils::pmr::string timezone_key("timezone", ctx.memory);
  fields.erase(timezone_key);

  bool const has_mapped_numeric_fields = MapNumericParameters<Integer>(date_parameter_mappings, fields);
  if (!has_mapped_numeric_fields) {
    return TypedValue(utils::ZonedDateTime(utils::AsSysTime(ctx.timestamp), timezone), ctx.memory);
  }
  auto zoned_date_time_parameters = utils::ZonedDateTimeParameters{date_parameters, time_parameters, timezone};
  return TypedValue(utils::ZonedDateTime(zoned_date_time_parameters), ctx.memory);
}

TypedValue ToEnum(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<String, Optional<String>>("toEnum", args, nargs);

  auto const &s1 = args[0].ValueString();
  if (nargs == 1) {
    auto enum_val = ctx.db_accessor->GetEnumValue(s1);
    if (!enum_val) throw QueryRuntimeException("Invalid enum '{}'", s1);
    return TypedValue(*enum_val, ctx.memory);
  }
  auto const &s2 = args[1].ValueString();
  auto enum_val = ctx.db_accessor->GetEnumValue(s1, s2);
  if (!enum_val) throw QueryRuntimeException("Invalid enum '{}::{}'", s1, s2);
  return TypedValue(*enum_val, ctx.memory);
}

TypedValue Point(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Map>("point", args, nargs);

  auto const &input = args[0].ValueMap();

  for (auto const &[k, v] : input) {
    if (v.IsNull()) {
      return TypedValue(ctx.memory);
    }
  }

  auto numeric_as_double = [](TypedValue const &value, std::string_view arg_name) -> double {
    if (value.IsDouble()) {
      return value.ValueDouble();
    }
    if (value.IsInt()) {
      return static_cast<double>(value.ValueInt());
    }
    throw QueryRuntimeException("Argument {} is not numeric.", arg_name);
  };

  auto [x, from_longitude] = std::invoke([&]() -> std::pair<double, bool> {
    auto it_x = input.find("x");
    if (it_x != input.end()) {
      return {numeric_as_double(it_x->second, "longitude/x"), false};
    }
    auto it_longitude = input.find("longitude");
    if (it_longitude != input.end()) {
      return {numeric_as_double(it_longitude->second, "longitude/x"), true};
    }
    throw QueryRuntimeException("Argument longitude/x is missing.");
  });

  auto [y, from_latitude] = std::invoke([&]() -> std::pair<double, bool> {
    auto it_y = input.find("y");
    if (it_y != input.end()) {
      return {numeric_as_double(it_y->second, "latitude/y"), false};
    }
    auto it_latitude = input.find("latitude");
    if (it_latitude != input.end()) {
      return {numeric_as_double(it_latitude->second, "latitude/y"), true};
    }
    throw QueryRuntimeException("Argument latitude/y is missing.");
  });

  using z_type = std::optional<std::pair<double, bool>>;
  auto z_opt = std::invoke([&]() -> z_type {
    auto it_z = input.find("z");
    if (it_z != input.end()) {
      return z_type{std::in_place, numeric_as_double(it_z->second, "height/z"), false};
    }
    auto it_height = input.find("height");
    if (it_height != input.end()) {
      return z_type{std::in_place, numeric_as_double(it_height->second, "height/z"), true};
    }
    return std::nullopt;
  });

  if (from_longitude != from_latitude) {
    throw QueryRuntimeException("Use either x, y, z or longitude, latitude, height.");
  }

  auto crs = std::invoke([&]() -> std::optional<std::string_view> {
    auto value = input.find("crs");
    if (value == input.end() || !value->second.IsString()) {
      return std::nullopt;
    }
    return value->second.ValueString();
  });

  auto srid = std::invoke([&]() -> std::optional<int> {
    auto value = input.find("srid");
    if (value == input.end() || !value->second.IsInt()) {
      return std::nullopt;
    }
    return value->second.ValueInt();
  });

  if (crs.has_value() && srid.has_value()) {
    throw QueryRuntimeException("Cannot specify both CRS and SRID.");
  }

  std::optional<storage::CoordinateReferenceSystem> mg_crs;
  if (crs) {
    mg_crs = storage::StringToCrs(*crs);
    if (!mg_crs) {
      throw QueryRuntimeException("Invalid CRS.");
    }
  } else if (srid) {
    mg_crs = storage::SridToCrs(static_cast<storage::Srid>(*srid));
    if (!mg_crs) {
      throw QueryRuntimeException("Invalid SRID.");
    }
  }

  auto inferred_as_wgs = (from_longitude || from_latitude);
  if (mg_crs && storage::IsCartesian(*mg_crs) && inferred_as_wgs) {
    throw QueryRuntimeException("Cartesian points must be constructed with x, y, z not longitude, latitude, height");
  }

  using enum storage::CoordinateReferenceSystem;
  if (!mg_crs) {
    if (!z_opt) {
      mg_crs = inferred_as_wgs ? WGS84_2d : Cartesian_2d;
    } else {
      mg_crs = inferred_as_wgs ? WGS84_3d : Cartesian_3d;
    }
  }

  auto check_point_ranges = [](auto const &x, auto const &y) {
    return (x >= -180.0 && x <= 180.0 && y >= -90.0 && y <= 90.0);
  };

  if (storage::IsWGS(*mg_crs) && !check_point_ranges(x, y)) {
    throw QueryRuntimeException(
        "Longitude/x [-180, 180] and latitude/y [-90, 90] must be in the given range for WGS point types.");
  }

  if (!z_opt) {
    if (!storage::valid2d(*mg_crs)) {
      throw QueryRuntimeException("Concluded point type is 2D but CRS/SRID says it is 3D.");
    }
    return TypedValue(storage::Point2d{*mg_crs, x, y}, ctx.memory);
  }

  if (!storage::valid3d(*mg_crs)) {
    throw QueryRuntimeException("Concluded point type is 3D but CRS/SRID says it is 2D.");
  }
  return TypedValue(storage::Point3d{*mg_crs, x, y, z_opt->first}, ctx.memory);
}

TypedValue Distance(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Point2d, Point3d, Null>, Or<Point2d, Point3d, Null>>("distance", args, nargs);

  if (args[0].IsNull() || args[1].IsNull()) {
    return TypedValue(ctx.memory);
  }

  auto type1 = args[0].type();
  auto type2 = args[1].type();

  if (type1 != type2) {
    return TypedValue(ctx.memory);
  }

  auto distance_func = [&]<typename T>(T const &point1, T const &point2) {
    if (point1.crs() != point2.crs()) {
      return TypedValue(ctx.memory);
    }
    return TypedValue{storage::Distance(point1, point2), ctx.memory};
  };

  return (type1 == TypedValue::Type::Point2d)
             ? std::invoke(distance_func, args[0].ValuePoint2d(), args[1].ValuePoint2d())
             : std::invoke(distance_func, args[0].ValuePoint3d(), args[1].ValuePoint3d());
}

TypedValue WithinBBox(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Point2d, Point3d, Null>, Or<Point2d, Point3d, Null>, Or<Point2d, Point3d, Null>>("withinbbox", args, nargs);

  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValue(ctx.memory);
  }

  auto type1 = args[0].type();
  auto type2 = args[1].type();
  auto type3 = args[2].type();

  if (type1 != type2 || type1 != type3) {
    return TypedValue(ctx.memory);
  }

  auto within_bbox_func = [&ctx]<typename T>(T const &point, T const &lower_left, T const &upper_right) {
    if (point.crs() != lower_left.crs() || point.crs() != upper_right.crs()) {
      return TypedValue(ctx.memory);
    }

    return TypedValue(storage::WithinBBox(point, lower_left, upper_right), ctx.memory);
  };

  return (type1 == TypedValue::Type::Point2d)
             ? std::invoke(within_bbox_func, args[0].ValuePoint2d(), args[1].ValuePoint2d(), args[2].ValuePoint2d())
             : std::invoke(within_bbox_func, args[0].ValuePoint3d(), args[1].ValuePoint3d(), args[2].ValuePoint3d());
}

// Returns the current hops limit if set, otherwise null.
TypedValue GetHopsCounter(const TypedValue * /*args*/, int64_t /*nargs*/, const FunctionContext &ctx) {
  return TypedValue(ctx.hops_counter, ctx.memory);
}

TypedValue Username(const TypedValue * /*args*/, int64_t /*nargs*/, const FunctionContext &ctx) {
  FType<void>("username", /*args*/ nullptr, /*nargs*/ 0);
  // In triggers, use triggering_user (invoker), otherwise use user_or_role
  const auto &user = ctx.triggering_user ? ctx.triggering_user : ctx.user_or_role;
  if (!user) {
    return TypedValue(ctx.memory);
  }
  const auto &username = user->username();
  if (!username) {
    return TypedValue(ctx.memory);
  }
  return TypedValue(*username, ctx.memory);
}

TypedValue Roles(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<String>>("db_name", args, nargs);
  // In triggers, use triggering_user (invoker), otherwise use user_or_role
  const auto &user = ctx.triggering_user ? ctx.triggering_user : ctx.user_or_role;
  if (!user) {
    return TypedValue(TypedValue::TVector(ctx.memory));
  }

  std::optional<std::string> db_name;
  if (nargs > 0) {
    db_name.emplace(args[0].ValueString());
  }

  // nullopt = show all roles
  auto const rolenames = user->GetRolenames(db_name);
  TypedValue::TVector roles_list(ctx.memory);
  roles_list.reserve(rolenames.size());
  for (auto const &rolename : rolenames) {
    roles_list.emplace_back(TypedValue::TString(rolename, ctx.memory));  // string to pmr string
  }
  return TypedValue(std::move(roles_list));
}

// virtualNode(handle, labels, properties) constructs a synthetic node: a node with no origin,
// holding an overlay property store only. Its identity is a fresh synthetic gid; the first
// argument is the import handle, stored on the node to wire virtual edges to it by reference when a
// projection is assembled, not the node's identity. Labels may be a single string or a list.
TypedValue VirtualNodeCtor(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Integer, Or<String, List>, Map>("virtualNode", args, nargs);

  const auto alloc = VirtualNode::allocator_type{ctx.memory};

  VirtualNode::label_list labels{alloc};
  if (args[1].type() == TypedValue::Type::String) {
    labels.emplace_back(args[1].ValueString());
  } else {
    for (const auto &label : args[1].ValueList()) {
      if (label.type() != TypedValue::Type::String) {
        throw QueryRuntimeException("virtualNode() labels must be strings.");
      }
      labels.emplace_back(label.ValueString());
    }
  }

  VirtualNode::property_map properties{alloc};
  auto *name_id_mapper = ctx.db_accessor->GetStorageAccessor()->GetNameIdMapper();
  for (const auto &[name, value] : args[2].ValueMap()) {
    properties.insert_or_assign(ctx.db_accessor->NameToProperty(name), value.ToPropertyValue(name_id_mapper));
  }

  VirtualNode node{std::move(labels), std::move(properties), alloc};
  node.SetHandle(args[0].ValueInt());
  return TypedValue(std::move(node), ctx.memory);
}

// virtualEdge(type, from, to) constructs a synthetic edge with a fresh synthetic gid. Each endpoint
// is given as a virtual node (a resolved endpoint) or as a gid handle (an unresolved endpoint bound
// to a node only when a projection is assembled from lists); the two forms may be mixed. A real
// vertex is not an endpoint - wire a real node by passing its id() as the handle.
TypedValue VirtualEdgeCtor(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<String, Or<Integer, Vertex>, Or<Integer, Vertex>>("virtualEdge", args, nargs);

  auto resolve = [&](const TypedValue &endpoint) -> VirtualEdge::Endpoint {
    if (endpoint.IsInt()) return endpoint.ValueInt();
    if (endpoint.IsVirtualNode()) {
      return std::allocate_shared<VirtualNode>(std::pmr::polymorphic_allocator<VirtualNode>(ctx.memory),
                                               endpoint.ValueVirtualNode());
    }
    throw QueryRuntimeException(
        "virtualEdge() endpoints must be a virtual node or a gid handle; to wire a real node, pass "
        "its id() as the handle, e.g. virtualEdge('T', id(n1), id(n2)).");
  };

  utils::pmr::string edge_type{args[0].ValueString(), ctx.memory};
  VirtualEdge edge{resolve(args[1]), resolve(args[2]), std::move(edge_type), VirtualEdge::allocator_type{ctx.memory}};
  return TypedValue(std::move(edge), ctx.memory);
}

// virtualGraph(nodes, edges, config?) assembles a projection from a list of synthetic nodes and a
// list of synthetic edges, binding each edge's endpoints to a node by import handle (or by identity
// for a resolved endpoint). Nulls in either list are skipped; a real vertex or edge is rejected. The
// optional config map's onDanglingEdge key chooses 'error' (default) or 'drop' for an edge whose
// endpoint matches no listed node.
TypedValue VirtualGraphCtor(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<List, List, Optional<Map>>("virtualGraph", args, nargs);

  std::vector<VirtualNode> nodes;
  for (const auto &element : args[0].ValueList()) {
    if (element.IsNull()) continue;
    if (!element.IsVirtualNode()) {
      throw QueryRuntimeException(
          "virtualGraph() node list must contain virtual nodes or nulls; use project()/derive() for real nodes.");
    }
    nodes.push_back(element.ValueVirtualNode());
  }

  std::vector<VirtualEdge> edges;
  for (const auto &element : args[1].ValueList()) {
    if (element.IsNull()) continue;
    if (!element.IsVirtualEdge()) {
      throw QueryRuntimeException(
          "virtualGraph() edge list must contain virtual edges or nulls; use project()/derive() for real edges.");
    }
    edges.push_back(element.ValueVirtualEdge());
  }

  auto policy = DanglingEdgePolicy::kError;
  if (nargs == 3) {
    const auto &config = args[2].ValueMap();
    if (const auto it = config.find("onDanglingEdge"); it != config.end()) {
      if (it->second.type() != TypedValue::Type::String) {
        throw QueryRuntimeException("virtualGraph() option 'onDanglingEdge' must be 'error' or 'drop'.");
      }
      const auto &mode = it->second.ValueString();
      if (mode == "drop") {
        policy = DanglingEdgePolicy::kDrop;
      } else if (mode == "error") {
        policy = DanglingEdgePolicy::kError;
      } else {
        throw QueryRuntimeException("virtualGraph() option 'onDanglingEdge' must be 'error' or 'drop', got '{}'.",
                                    std::string_view{mode});
      }
    }
  }

  auto graph = AssembleVirtualGraph(nodes, edges, policy, VirtualGraph::allocator_type{ctx.memory});
  return TypedValue(std::move(graph), ctx.memory);
}

auto const builtin_functions = absl::flat_hash_map<std::string, func_info>{
    // Predicate functions
    {"ISEMPTY", func_info{.func_ = IsEmpty, .is_pure_ = true}},

    // Scalar functions
    {"DEGREE", func_info{.func_ = Degree, .is_pure_ = true}},
    {"INDEGREE", func_info{.func_ = InDegree, .is_pure_ = true}},
    {"OUTDEGREE", func_info{.func_ = OutDegree, .is_pure_ = true}},
    {"ENDNODE", func_info{.func_ = EndNode, .is_pure_ = true}},
    {"HEAD", func_info{.func_ = Head, .is_pure_ = true}},
    {kId, func_info{.func_ = Id, .is_pure_ = true}},
    {kVirtualId, func_info{.func_ = VirtualId, .is_pure_ = true}},
    {kElementId, func_info{.func_ = ElementId, .is_pure_ = true}},
    {"LAST", func_info{.func_ = Last, .is_pure_ = true}},
    {"PROPERTIES", func_info{.func_ = Properties, .is_pure_ = true}},
    {"RANDOMUUID", func_info{.func_ = RandomUuid, .is_pure_ = false}},
    {"SIZE", func_info{.func_ = Size, .is_pure_ = true}},
    {"LENGTH", func_info{.func_ = Size, .is_pure_ = true}},
    {"PROPERTYSIZE", func_info{.func_ = PropertySize, .is_pure_ = true}},
    {"STARTNODE", func_info{.func_ = StartNode, .is_pure_ = true}},
    {"TIMESTAMP", func_info{.func_ = Timestamp, .is_pure_ = false}},
    {"TOBOOLEAN", func_info{.func_ = ToBoolean, .is_pure_ = true}},
    {"TOFLOAT", func_info{.func_ = ToFloat, .is_pure_ = true}},
    {"TOINTEGER", func_info{.func_ = ToInteger, .is_pure_ = true}},
    {"TOBOOLEANORNULL", func_info{.func_ = ToBooleanOrNull, .is_pure_ = true}},
    {"TOFLOATORNULL", func_info{.func_ = ToFloatOrNull, .is_pure_ = true}},
    {"TOINTEGERORNULL", func_info{.func_ = ToIntegerOrNull, .is_pure_ = true}},
    {"TOBOOLEANLIST", func_info{.func_ = ToBooleanList, .is_pure_ = true}},
    {"TOFLOATLIST", func_info{.func_ = ToFloatList, .is_pure_ = true}},
    {"TOINTEGERLIST", func_info{.func_ = ToIntegerList, .is_pure_ = true}},
    {"TYPE", func_info{.func_ = Type, .is_pure_ = true}},
    {"VALUETYPE", func_info{.func_ = ValueType, .is_pure_ = true}},

    // List, map functions
    {"KEYS", func_info{.func_ = Keys, .is_pure_ = true}},
    {"LABELS", func_info{.func_ = Labels, .is_pure_ = true}},
    {"NODES", func_info{.func_ = Nodes, .is_pure_ = true}},
    {"RANGE", func_info{.func_ = Range, .is_pure_ = true}},
    {"RELATIONSHIPS", func_info{.func_ = Relationships, .is_pure_ = true}},
    {"TAIL", func_info{.func_ = Tail, .is_pure_ = true}},
    {"TOSET", func_info{.func_ = ToSet, .is_pure_ = true}},
    {"UNIFORMSAMPLE", func_info{.func_ = UniformSample, .is_pure_ = false}},
    {"VALUES", func_info{.func_ = Values, .is_pure_ = true}},

    // Virtual graph constructors. Not pure: each call mints a fresh synthetic gid.
    {"VIRTUALNODE", func_info{.func_ = VirtualNodeCtor, .is_pure_ = false}},
    {"VIRTUALEDGE", func_info{.func_ = VirtualEdgeCtor, .is_pure_ = false}},
    {"VIRTUALGRAPH", func_info{.func_ = VirtualGraphCtor, .is_pure_ = false}},

    // Mathematical functions - numeric
    {"ABS", func_info{.func_ = Abs, .is_pure_ = true}},
    {"CEIL", func_info{.func_ = Ceil, .is_pure_ = true}},
    {"FLOOR", func_info{.func_ = Floor, .is_pure_ = true}},
    {"RAND", func_info{.func_ = Rand, .is_pure_ = false}},
    {"ROUND", func_info{.func_ = Round, .is_pure_ = true}},
    {"SIGN", func_info{.func_ = Sign, .is_pure_ = true}},

    // Mathematical functions - logarithmic
    {"E", func_info{.func_ = E, .is_pure_ = true}},
    {"EXP", func_info{.func_ = Exp, .is_pure_ = true}},
    {"LOG", func_info{.func_ = Log, .is_pure_ = true}},
    {"LOG10", func_info{.func_ = Log10, .is_pure_ = true}},
    {"SQRT", func_info{.func_ = Sqrt, .is_pure_ = true}},

    // Mathematical functions - trigonometric
    {"ACOS", func_info{.func_ = Acos, .is_pure_ = true}},
    {"ASIN", func_info{.func_ = Asin, .is_pure_ = true}},
    {"ATAN", func_info{.func_ = Atan, .is_pure_ = true}},
    {"ATAN2", func_info{.func_ = Atan2, .is_pure_ = true}},
    {"COS", func_info{.func_ = Cos, .is_pure_ = true}},
    {"PI", func_info{.func_ = Pi, .is_pure_ = true}},
    {"SIN", func_info{.func_ = Sin, .is_pure_ = true}},
    {"TAN", func_info{.func_ = Tan, .is_pure_ = true}},

    // String functions
    {kContains, func_info{.func_ = Contains, .is_pure_ = true}},
    {kEndsWith, func_info{.func_ = EndsWith, .is_pure_ = true}},
    {"LEFT", func_info{.func_ = Left, .is_pure_ = true}},
    {"LTRIM", func_info{.func_ = LTrim, .is_pure_ = true}},
    {"REPLACE", func_info{.func_ = Replace, .is_pure_ = true}},
    {"REVERSE", func_info{.func_ = Reverse, .is_pure_ = true}},
    {"RIGHT", func_info{.func_ = Right, .is_pure_ = true}},
    {"RTRIM", func_info{.func_ = RTrim, .is_pure_ = true}},
    {"SPLIT", func_info{.func_ = Split, .is_pure_ = true}},
    {kStartsWith, func_info{.func_ = StartsWith, .is_pure_ = true}},
    {"SUBSTRING", func_info{.func_ = Substring, .is_pure_ = true}},
    {"TOLOWER", func_info{.func_ = ToLower, .is_pure_ = true}},
    {"TOSTRING", func_info{.func_ = ToString, .is_pure_ = true}},
    {"TOSTRINGORNULL", func_info{.func_ = ToStringOrNull, .is_pure_ = true}},
    {"TOSTRINGLIST", func_info{.func_ = ToStringList, .is_pure_ = true}},
    {"TOUPPER", func_info{.func_ = ToUpper, .is_pure_ = true}},
    {"TRIM", func_info{.func_ = Trim, .is_pure_ = true}},

    // Memgraph specific functions
    {"ASSERT", func_info{.func_ = Assert, .is_pure_ = false}},
    {"COUNTER", func_info{.func_ = Counter, .is_pure_ = false}},
    {"TOBYTESTRING", func_info{.func_ = ToByteString, .is_pure_ = true}},
    {"FROMBYTESTRING", func_info{.func_ = FromByteString, .is_pure_ = true}},
    {"DATE", func_info{.func_ = Date, .is_pure_ = false}},
    {"LOCALTIME", func_info{.func_ = LocalTime, .is_pure_ = false}},
    {"LOCALDATETIME", func_info{.func_ = LocalDateTime, .is_pure_ = false}},
    {"DATETIME", func_info{.func_ = DateTime, .is_pure_ = false}},
    {"DURATION", func_info{.func_ = Duration, .is_pure_ = true}},

    // Functions for enum types
    {"TOENUM", func_info{.func_ = ToEnum, .is_pure_ = true}},

    // Functions for point types
    {"POINT", func_info{.func_ = Point, .is_pure_ = true}},
    {"POINT.DISTANCE", func_info{.func_ = Distance, .is_pure_ = true}},
    {"POINT.WITHINBBOX", func_info{.func_ = WithinBBox, .is_pure_ = true}},

    // Functions for internal objects
    {"GETHOPSCOUNTER", func_info{.func_ = GetHopsCounter, .is_pure_ = false}},

    // User and role functions
    {"USERNAME", func_info{.func_ = Username, .is_pure_ = false}},
    {"ROLES", func_info{.func_ = Roles, .is_pure_ = false}},
};

auto UserFunction(const mgp_func &func, const std::string &fully_qualified_name) -> func_impl {
  return [func, fully_qualified_name](const TypedValue *args, int64_t nargs, const FunctionContext &ctx) -> TypedValue {
    // Lock on the module is already acquired in the AST construction
    procedure::ValidateArguments(std::span(args, args + nargs), func, fully_qualified_name);

    auto graph = mgp_graph::NonWritableGraph(*ctx.db_accessor, ctx.view);
    auto function_argument_list = mgp_list(ctx.memory);
    procedure::ConstructArguments(std::span(args, args + nargs), func, function_argument_list, graph);

    auto functx = mgp_func_context{ctx.db_accessor, ctx.view};
    auto maybe_res = mgp_func_result{};
    auto memory = mgp_memory{ctx.memory};
    func.cb(&function_argument_list, &functx, &maybe_res, &memory);
    if (maybe_res.error_msg) [[unlikely]] {
      throw QueryRuntimeException(*maybe_res.error_msg);
    }

    if (!maybe_res.value) [[unlikely]] {
      throw QueryRuntimeException(
          "Function '{}' didn't set the result nor the error message. Please either set the result by using "
          "mgp_func_result_set_value or the error by using mgp_func_result_set_error_msg.",
          fully_qualified_name);
    }

    return {*std::move(maybe_res.value), ctx.memory};
  };
}

}  // namespace

// There are some builtin functions that look like modules but are not. To ensure user defined functions don't conflict
// we maintain a list of reserved modules names.
auto ReservedBuiltInModuleNames() -> ::memgraph::utils::CaseInsensitiveSet const & {
  static auto const instance = memgraph::utils::CaseInsensitiveSet{"POINT"};
  return instance;
}

auto NameToFunction(const std::string &function_name) -> std::variant<std::monostate, func_impl, user_func> {
  // First lookup for built-in functions
  auto upper_case = utils::ToUpperCase(function_name);
  auto buildin_it = std::as_const(builtin_functions).find(upper_case);
  if (buildin_it != builtin_functions.cend()) {
    return buildin_it->second.func_;
  }

  // Next lookip for user-defined function from a module
  auto maybe_found = procedure::FindFunction(procedure::gModuleRegistry, function_name);
  if (maybe_found) {
    auto module_ptr = std::move((*maybe_found).first);
    const auto *func = (*maybe_found).second;
    return std::make_pair(UserFunction(*func, function_name), std::move(module_ptr));
  }

  // Does not exist
  return std::monostate{};
}

bool IsFunctionPure(std::string_view function_name) {
  // Lookup in builtin functions
  auto upper_case = utils::ToUpperCase(function_name);
  auto buildin_it = std::as_const(builtin_functions).find(upper_case);

  if (buildin_it != builtin_functions.cend()) {
    // Found a builtin function, return its purity status
    return buildin_it->second.is_pure_;
  }

  // Not a builtin function (could be user-defined or non-existent)
  // Currently, all non-builtin functions are considered not pure.
  // This may change in the future when we have a mechanism to mark
  // user-provided functions as pure.
  return false;
}

}  // namespace memgraph::query
