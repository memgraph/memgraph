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

#include "query/interpret/awesome_memgraph_functions.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <random>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

#include "query/exceptions.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/point_functions.hpp"
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
    return arg.IsVertex();
  } else if constexpr (std::is_same_v<ArgType, Edge>) {
    return arg.IsEdge();
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
        throw QueryRuntimeException("Optional '{}' argument at position {} must be either {}.", name, pos,
                                    ArgType::TypeNames());
      }
    } else {
      if (!ArgIsType<ArgType>(arg))
        throw QueryRuntimeException("Optional '{}' argument at position {} must be '{}'.", name, pos,
                                    ArgTypeName<ArgType>());
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
      throw QueryRuntimeException("'{}' requires exactly {} {}.", name, required_args,
                                  required_args == 1 ? "argument" : "arguments");
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

TypedValue Properties(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("properties", args, nargs);
  auto *dba = ctx.db_accessor;
  auto get_properties = [&](const auto &record_accessor) {
    TypedValue::TMap properties(ctx.memory);
    auto maybe_props = record_accessor.Properties(ctx.view);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get properties from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get properties from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting properties.");
      }
    }
    for (const auto &property : *maybe_props) {
      properties.emplace(dba->PropertyToName(property.first), property.second);
    }
    return TypedValue(std::move(properties));
  };
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsVertex()) {
    return get_properties(value.ValueVertex());
  } else {
    return get_properties(value.ValueEdge());
  }
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
    property_size = graph_entity.ValueVertex().GetPropertySize(*maybe_property_id, ctx.view).GetValue();
  } else if (graph_entity.IsEdge()) {
    property_size = graph_entity.ValueEdge().GetPropertySize(*maybe_property_id, ctx.view).GetValue();
  }

  return TypedValue(static_cast<int64_t>(property_size), ctx.memory);
}

TypedValue StartNode(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Edge>>("startNode", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(args[0].ValueEdge().From(), ctx.memory);
}

namespace {

size_t UnwrapDegreeResult(storage::Result<size_t> maybe_degree) {
  if (maybe_degree.HasError()) {
    switch (maybe_degree.GetError()) {
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

TypedValue Degree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("degree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t out_degree = UnwrapDegreeResult(vertex.OutDegree(ctx.view));
  size_t in_degree = UnwrapDegreeResult(vertex.InDegree(ctx.view));
  return TypedValue(static_cast<int64_t>(out_degree + in_degree), ctx.memory);
}

TypedValue InDegree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("inDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t in_degree = UnwrapDegreeResult(vertex.InDegree(ctx.view));
  return TypedValue(static_cast<int64_t>(in_degree), ctx.memory);
}

TypedValue OutDegree(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("outDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t out_degree = UnwrapDegreeResult(vertex.OutDegree(ctx.view));
  return TypedValue(static_cast<int64_t>(out_degree), ctx.memory);
}

TypedValue ToBoolean(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Bool, Integer, String>>("toBoolean", args, nargs);
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
  FType<Or<Null, Number, String>>("toFloat", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (value.IsInt()) {
    return TypedValue(static_cast<double>(value.ValueInt()), ctx.memory);
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
  FType<Or<Null, Bool, Number, String>>("toInteger", args, nargs);
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

TypedValue Type(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Edge>>("type", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(dba->EdgeTypeToName(args[0].ValueEdge().EdgeType()), ctx.memory);
}

TypedValue ValueType(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Bool, Integer, Double, String, List, Map, Vertex, Edge, Path, Date, LocalTime, LocalDateTime,
           ZonedDateTime, Duration, Graph, Enum, Point2d, Point3d>>("type", args, nargs);
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
    case TypedValue::Type::Function:
      throw QueryRuntimeException("Unknown value type! Please report an issue!");
  }
}

// TODO: How is Keys different from Properties function?
TypedValue Keys(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge, Map>>("keys", args, nargs);
  auto *dba = ctx.db_accessor;
  auto get_keys = [&](const auto &record_accessor) {
    TypedValue::TVector keys(ctx.memory);
    auto maybe_props = record_accessor.Properties(ctx.view);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get keys from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get keys from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting keys.");
      }
    }
    for (const auto &property : *maybe_props) {
      keys.emplace_back(dba->PropertyToName(property.first));
    }
    return TypedValue(std::move(keys));
  };
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  if (value.IsVertex()) {
    return get_keys(value.ValueVertex());
  }
  if (value.IsEdge()) {
    return get_keys(value.ValueEdge());
  }

  // map
  TypedValue::TVector keys(ctx.memory);
  for (const auto &[string_key, value] : value.ValueMap()) {
    keys.emplace_back(string_key);
  }
  return TypedValue(std::move(keys));
}

TypedValue Values(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge, Map>>("keys", args, nargs);

  auto get_values = [&](const auto &record_accessor) {
    TypedValue::TVector values(ctx.memory);
    auto maybe_props = record_accessor.Properties(ctx.view);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get keys from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get keys from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting keys.");
      }
    }
    for (const auto &[key, value] : *maybe_props) {
      values.emplace_back(std::move(value));
    }
    return TypedValue(std::move(values));
  };

  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValue(ctx.memory);
  }
  if (value.IsVertex()) {
    return get_values(value.ValueVertex());
  }
  if (value.IsEdge()) {
    return get_values(value.ValueEdge());
  }

  // map
  TypedValue::TVector values(ctx.memory);
  for (const auto &[string_key, value] : value.ValueMap()) {
    values.emplace_back(value);
  }

  return TypedValue(std::move(values));
}

TypedValue Labels(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex>>("labels", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  TypedValue::TVector labels(ctx.memory);
  auto maybe_labels = args[0].ValueVertex().Labels(ctx.view);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get labels from a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get labels from a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when getting labels.");
    }
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
    return s1.find(s2) != std::string::npos;
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

TypedValue Id(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, Vertex, Edge>>("id", args, nargs);
  const auto &arg = args[0];
  if (arg.IsNull()) {
    return TypedValue(ctx.memory);
  } else if (arg.IsVertex()) {
    return TypedValue(arg.ValueVertex().CypherId(), ctx.memory);
  } else {
    return TypedValue(arg.ValueEdge().CypherId(), ctx.memory);
  }
}

TypedValue ToString(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Or<Null, String, Number, Date, LocalTime, LocalDateTime, Duration, ZonedDateTime, Bool, Enum>>("toString", args,
                                                                                                       nargs);
  const auto &arg = args[0];
  if (arg.IsNull()) {
    return TypedValue(ctx.memory);
  }
  if (arg.IsString()) {
    return TypedValue(arg, ctx.memory);
  }
  if (arg.IsInt()) {
    // TODO: This is making a pointless copy of std::string, we may want to
    // use a different conversion to string
    return TypedValue(std::to_string(arg.ValueInt()), ctx.memory);
  }
  if (arg.IsDouble()) {
    return TypedValue(memgraph::utils::DoubleToString(arg.ValueDouble()), ctx.memory);
  }
  if (arg.IsDate()) {
    return TypedValue(arg.ValueDate().ToString(), ctx.memory);
  }
  if (arg.IsLocalTime()) {
    return TypedValue(arg.ValueLocalTime().ToString(), ctx.memory);
  }
  if (arg.IsLocalDateTime()) {
    return TypedValue(arg.ValueLocalDateTime().ToString(), ctx.memory);
  }
  if (arg.IsDuration()) {
    return TypedValue(arg.ValueDuration().ToString(), ctx.memory);
  }
  if (arg.IsZonedDateTime()) {
    return TypedValue(arg.ValueZonedDateTime().ToString(), ctx.memory);
  }

  if (arg.IsEnum()) {
    auto opt_str = ctx.db_accessor->EnumToName(arg.ValueEnum());
    if (opt_str.HasError()) throw QueryRuntimeException("'toString' the given enum can't be converted to a string");
    return TypedValue(*opt_str, ctx.memory);
  }

  return TypedValue(arg.ValueBool() ? "true" : "false", ctx.memory);
}

TypedValue Timestamp(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<Date, LocalTime, LocalDateTime, ZonedDateTime, Duration>>>("timestamp", args, nargs);
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
  return CallStringFunction(args, nargs, ctx.memory, "lTrim",
                            [&](const auto &str) { return TypedValue::TString(utils::LTrim(str), ctx.memory); });
}

TypedValue RTrim(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "rTrim",
                            [&](const auto &str) { return TypedValue::TString(utils::RTrim(str), ctx.memory); });
}

TypedValue Trim(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "trim",
                            [&](const auto &str) { return TypedValue::TString(utils::Trim(str), ctx.memory); });
}

TypedValue Reverse(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  return CallStringFunction(args, nargs, ctx.memory, "reverse",
                            [&](const auto &str) { return utils::Reversed(str, ctx.memory); });
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
    bytes.append(1, utils::MemcpyCast<decltype(bytes)::value_type>(byte));
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
    return utils::MemcpyCast<char>(ch);
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
void MapNumericParameters(auto &parameter_mappings, const auto &input_parameters) {
  for (const auto &[key, value] : input_parameters) {
    if (auto it = parameter_mappings.find(key); it != parameter_mappings.end()) {
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
}

TypedValue Date(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Optional<Or<String, Map, LocalDateTime>>>("date", args, nargs);
  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp).date(), ctx.memory);
  }

  if (args[0].IsLocalDateTime()) {
    return TypedValue(utils::Date{args[0].ValueLocalDateTime().date()}, ctx.memory);
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
  FType<Optional<Or<String, Map, LocalDateTime>>>("localtime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp).local_time(), ctx.memory);
  }

  if (args[0].IsLocalDateTime()) {
    return TypedValue(utils::LocalTime{args[0].ValueLocalDateTime().local_time()}, ctx.memory);
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
  FType<Optional<Or<String, Map>>>("localdatetime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::LocalDateTime(ctx.timestamp), ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[date_parameters, local_time_parameters] = ParseLocalDateTimeParameters(args[0].ValueString());
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
  FType<Or<String, Map>>("duration", args, nargs);

  if (args[0].IsString()) {
    return TypedValue(utils::Duration(ParseDurationParameters(args[0].ValueString())), ctx.memory);
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
  FType<Optional<Or<String, Map>>>("datetime", args, nargs);

  if (nargs == 0) {
    return TypedValue(utils::ZonedDateTime(utils::AsSysTime(ctx.timestamp), utils::DefaultTimezone()), ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &zoned_date_time_parameters = ParseZonedDateTimeParameters(args[0].ValueString());
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

  MapNumericParameters<Integer>(date_parameter_mappings, fields);
  auto zoned_date_time_parameters = utils::ZonedDateTimeParameters{date_parameters, time_parameters, timezone};
  return TypedValue(utils::ZonedDateTime(zoned_date_time_parameters), ctx.memory);
}

TypedValue ToEnum(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<String, Optional<String>>("toEnum", args, nargs);

  auto const &s1 = args[0].ValueString();
  if (nargs == 1) {
    auto enum_val = ctx.db_accessor->GetEnumValue(s1);
    if (enum_val.HasError()) throw QueryRuntimeException("Invalid enum '{}'", s1);
    return TypedValue(*enum_val, ctx.memory);
  }
  auto const &s2 = args[1].ValueString();
  auto enum_val = ctx.db_accessor->GetEnumValue(s1, s2);
  if (enum_val.HasError()) throw QueryRuntimeException("Invalid enum '{}::{}'", s1, s2);
  return TypedValue(*enum_val, ctx.memory);
}

TypedValue Point(const TypedValue *args, int64_t nargs, const FunctionContext &ctx) {
  FType<Map>("point", args, nargs);

  auto const &input = args[0].ValueMap();

  for (auto const &[k, v] : input) {
    if (v.IsNull()) {
      return TypedValue{ctx.memory};
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
  if (crs.has_value()) {
    mg_crs = storage::StringToCrs(*crs);
    if (!mg_crs.has_value()) {
      throw QueryRuntimeException("Invalid CRS.");
    }
  } else if (srid.has_value()) {
    mg_crs = storage::SridToCrs(static_cast<storage::Srid>(*srid));
    if (!mg_crs.has_value()) {
      throw QueryRuntimeException("Invalid SRID.");
    }
  }

  auto inferred_as_wgs = (from_longitude || from_latitude);
  if (mg_crs && storage::IsCartesian(*mg_crs) && inferred_as_wgs) {
    throw QueryRuntimeException("Cartesian points must be constructed with x, y, z not longitude, latitude, height");
  }

  using enum storage::CoordinateReferenceSystem;
  if (!mg_crs.has_value()) {
    if (!z_opt.has_value()) {
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

  if (!z_opt.has_value()) {
    return TypedValue(storage::Point2d{*mg_crs, x, y}, ctx.memory);
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

auto const builtin_functions = absl::flat_hash_map<std::string, func_impl>{
    // Scalar functions
    {"DEGREE", Degree},
    {"INDEGREE", InDegree},
    {"OUTDEGREE", OutDegree},
    {"ENDNODE", EndNode},
    {"HEAD", Head},
    {kId, Id},
    {"LAST", Last},
    {"PROPERTIES", Properties},
    {"RANDOMUUID", RandomUuid},
    {"SIZE", Size},
    {"PROPERTYSIZE", PropertySize},
    {"STARTNODE", StartNode},
    {"TIMESTAMP", Timestamp},
    {"TOBOOLEAN", ToBoolean},
    {"TOFLOAT", ToFloat},
    {"TOINTEGER", ToInteger},
    {"TYPE", Type},
    {"VALUETYPE", ValueType},

    // List, map functions
    {"KEYS", Keys},
    {"LABELS", Labels},
    {"NODES", Nodes},
    {"RANGE", Range},
    {"RELATIONSHIPS", Relationships},
    {"TAIL", Tail},
    {"UNIFORMSAMPLE", UniformSample},
    {"VALUES", Values},

    // Mathematical functions - numeric
    {"ABS", Abs},
    {"CEIL", Ceil},
    {"FLOOR", Floor},
    {"RAND", Rand},
    {"ROUND", Round},
    {"SIGN", Sign},

    // Mathematical functions - logarithmic
    {"E", E},
    {"EXP", Exp},
    {"LOG", Log},
    {"LOG10", Log10},
    {"SQRT", Sqrt},

    // Mathematical functions - trigonometric
    {"ACOS", Acos},
    {"ASIN", Asin},
    {"ATAN", Atan},
    {"ATAN2", Atan2},
    {"COS", Cos},
    {"PI", Pi},
    {"SIN", Sin},
    {"TAN", Tan},

    // String functions
    {kContains, Contains},
    {kEndsWith, EndsWith},
    {"LEFT", Left},
    {"LTRIM", LTrim},
    {"REPLACE", Replace},
    {"REVERSE", Reverse},
    {"RIGHT", Right},
    {"RTRIM", RTrim},
    {"SPLIT", Split},
    {kStartsWith, StartsWith},
    {"SUBSTRING", Substring},
    {"TOLOWER", ToLower},
    {"TOSTRING", ToString},
    {"TOUPPER", ToUpper},
    {"TRIM", Trim},

    // Memgraph specific functions
    {"ASSERT", Assert},
    {"COUNTER", Counter},
    {"TOBYTESTRING", ToByteString},
    {"FROMBYTESTRING", FromByteString},
    {"DATE", Date},
    {"LOCALTIME", LocalTime},
    {"LOCALDATETIME", LocalDateTime},
    {"DATETIME", DateTime},
    {"DURATION", Duration},

    // Functions for enum types
    {"TOENUM", ToEnum},

    // Functions for point types
    {"POINT", Point},
    {"POINT.DISTANCE", Distance},
    {"POINT.WITHINBBOX", WithinBBox},
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
    return buildin_it->second;
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

}  // namespace memgraph::query
