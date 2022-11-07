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

#pragma once

#include <functional>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "storage/v3/result.hpp"
#include "storage/v3/view.hpp"
#include "utils/algorithm.hpp"
#include "utils/cast.hpp"
#include "utils/concepts.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"

namespace memgraph::functions {

class FunctionRuntimeException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

template <typename TAccessor>
struct FunctionContext {
  TAccessor *db_accessor;
  utils::MemoryResource *memory;
  int64_t timestamp;
  std::unordered_map<std::string, int64_t> *counters;
  storage::v3::View view;
};

// Tags for the NameToFunction() function template
struct StorageEngineTag {};
struct QueryEngineTag {};

namespace impl {
struct SinkCallable {
  void operator()() {}
};
}  // namespace impl

/// Return the function implementation with the given name.
///
/// Note, returned function signature uses C-style access to an array to allow
/// having an array stored anywhere the caller likes, as long as it is
/// contiguous in memory. Since most functions don't take many arguments, it's
/// convenient to have them stored in the calling stack frame.
template <typename TypedValueT, typename FunctionContextT, typename Tag, typename Conv = impl::SinkCallable>
std::function<TypedValueT(const TypedValueT *arguments, int64_t num_arguments, const FunctionContextT &context)>
NameToFunction(const std::string &function_name);

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
const char kId[] = "ID";
}  // namespace

}  // namespace memgraph::functions

namespace memgraph::functions::impl {

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
// `FType` will throw a `FunctionRuntimeException` with an appropriate error
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

template <typename ArgType, typename TypedValueT>
bool ArgIsType(const TypedValueT &arg) {
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
  } else if constexpr (std::is_same_v<ArgType, void>) {
    return true;
  } else {
    static_assert(std::is_same_v<ArgType, Null>, "Unknown ArgType");
  }
  return false;
}

template <typename ArgType>
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
  } else {
    static_assert(std::is_same_v<ArgType, Null>, "Unknown ArgType");
  }
  return "<unknown-type>";
}

template <typename... ArgType>
struct Or;

template <typename ArgType>
struct Or<ArgType> {
  template <typename TypedValueT>
  static bool Check(const TypedValueT &arg) {
    return ArgIsType<ArgType>(arg);
  }

  static std::string TypeNames() { return ArgTypeName<ArgType>(); }
};

template <typename ArgType, typename... ArgTypes>
struct Or<ArgType, ArgTypes...> {
  template <typename TypedValueT>
  static bool Check(const TypedValueT &arg) {
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

template <typename... ArgTypes>
struct Optional;

template <typename ArgType>
struct Optional<ArgType> {
  static constexpr size_t size = 1;

  template <typename TypedValueT>
  static void Check(const char *name, const TypedValueT *args, int64_t nargs, int64_t pos) {
    if (nargs == 0) return;
    const TypedValueT &arg = args[0];
    if constexpr (IsOrType<ArgType>::value) {
      if (!ArgType::Check(arg)) {
        throw FunctionRuntimeException("Optional '{}' argument at position {} must be either {}.", name, pos,
                                       ArgType::TypeNames());
      }
    } else {
      if (!ArgIsType<ArgType>(arg))
        throw FunctionRuntimeException("Optional '{}' argument at position {} must be '{}'.", name, pos,
                                       ArgTypeName<ArgType>());
    }
  }
};

template <class ArgType, class... ArgTypes>
struct Optional<ArgType, ArgTypes...> {
  static constexpr size_t size = 1 + sizeof...(ArgTypes);

  template <typename TypedValueT>
  static void Check(const char *name, const TypedValueT *args, int64_t nargs, int64_t pos) {
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

template <typename TypedValueT, typename ArgType, typename... ArgTypes>
void FType(const char *name, const TypedValueT *args, int64_t nargs, int64_t pos = 1) {
  if constexpr (std::is_same_v<ArgType, void>) {
    if (nargs != 0) {
      throw FunctionRuntimeException("'{}' requires no arguments.", name);
    }
    return;
  }
  static constexpr int64_t required_args = FTypeRequiredArgs<ArgType, ArgTypes...>();
  static constexpr int64_t optional_args = FTypeOptionalArgs<ArgType, ArgTypes...>();
  static constexpr int64_t total_args = required_args + optional_args;
  if constexpr (optional_args > 0) {
    if (nargs < required_args || nargs > total_args) {
      throw FunctionRuntimeException("'{}' requires between {} and {} arguments.", name, required_args, total_args);
    }
  } else {
    if (nargs != required_args) {
      throw FunctionRuntimeException("'{}' requires exactly {} {}.", name, required_args,
                                     required_args == 1 ? "argument" : "arguments");
    }
  }
  const TypedValueT &arg = args[0];
  if constexpr (IsOrType<ArgType>::value) {
    if (!ArgType::Check(arg)) {
      throw FunctionRuntimeException("'{}' argument at position {} must be either {}.", name, pos,
                                     ArgType::TypeNames());
    }
  } else if constexpr (IsOptional<ArgType>::value) {
    static_assert(sizeof...(ArgTypes) == 0, "Optional arguments must be last!");
    ArgType::Check(name, args, nargs, pos);
  } else {
    if (!ArgIsType<ArgType>(arg)) {
      throw FunctionRuntimeException("'{}' argument at position {} must be '{}'", name, pos, ArgTypeName<ArgType>());
    }
  }
  if constexpr (sizeof...(ArgTypes) > 0) {
    FType<TypedValueT, ArgTypes...>(name, args + 1, nargs - 1, pos + 1);
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

template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT EndNode(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Edge>>("endNode", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  if constexpr (std::is_same_v<Tag, StorageEngineTag>) {
    const auto to = args[0].ValueEdge().To();
    auto pk = to.primary_key;
    auto maybe_vertex_accessor = ctx.db_accessor->FindVertex(pk, ctx.view);
    if (!maybe_vertex_accessor) {
      throw functions::FunctionRuntimeException("Trying to get properties from a deleted object.");
    }
    return TypedValueT(*maybe_vertex_accessor, ctx.memory);
  } else {
    return TypedValueT(args[0].ValueEdge().To(), ctx.memory);
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Head(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, List>>("head", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValueT(ctx.memory);
  return TypedValueT(list[0], ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Last(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, List>>("last", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValueT(ctx.memory);
  return TypedValueT(list.back(), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT, typename Tag, typename Conv>
TypedValueT Properties(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex, Edge>>("properties", args, nargs);
  auto *dba = ctx.db_accessor;
  auto get_properties = [&](const auto &record_accessor) {
    typename TypedValueT::TMap properties(ctx.memory);
    if constexpr (std::is_same_v<Tag, StorageEngineTag>) {
      auto maybe_props = record_accessor.Properties(ctx.view);
      if (maybe_props.HasError()) {
        switch (maybe_props.GetError()) {
          case storage::v3::Error::DELETED_OBJECT:
            throw functions::FunctionRuntimeException("Trying to get properties from a deleted object.");
          case storage::v3::Error::NONEXISTENT_OBJECT:
            throw functions::FunctionRuntimeException("Trying to get properties from an object that doesn't exist.");
          case storage::v3::Error::SERIALIZATION_ERROR:
          case storage::v3::Error::VERTEX_HAS_EDGES:
          case storage::v3::Error::PROPERTIES_DISABLED:
          case storage::v3::Error::VERTEX_ALREADY_INSERTED:
            throw functions::FunctionRuntimeException("Unexpected error when getting properties.");
        }
      }
      for (const auto &property : *maybe_props) {
        Conv conv;
        properties.emplace(dba->PropertyToName(property.first), conv(property.second));
      }
    } else {
      Conv conv;
      for (const auto &property : record_accessor.Properties()) {
        properties.emplace(utils::pmr::string(dba->PropertyToName(property.first), ctx.memory),
                           conv(property.second, dba));
      }
    }
    return TypedValueT(std::move(properties));
  };

  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsVertex()) {
    return get_properties(value.ValueVertex());
  } else {
    return get_properties(value.ValueEdge());
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Size(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, List, String, Map, Path>>("size", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsList()) {
    return TypedValueT(static_cast<int64_t>(value.ValueList().size()), ctx.memory);
  } else if (value.IsString()) {
    return TypedValueT(static_cast<int64_t>(value.ValueString().size()), ctx.memory);
  } else if (value.IsMap()) {
    // neo4j doesn't implement size for map, but I don't see a good reason not
    // to do it.
    return TypedValueT(static_cast<int64_t>(value.ValueMap().size()), ctx.memory);
  } else {
    return TypedValueT(static_cast<int64_t>(value.ValuePath().edges().size()), ctx.memory);
  }
}

template <typename TypedValueT, typename FunctionContextT, typename Tag, typename Conv>
TypedValueT StartNode(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Edge>>("startNode", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  if constexpr (std::is_same_v<Tag, StorageEngineTag>) {
    const auto to = args[0].ValueEdge().From();
    auto pk = to.primary_key;
    auto maybe_vertex_accessor = ctx.db_accessor->FindVertex(pk, ctx.view);
    if (!maybe_vertex_accessor) {
      throw functions::FunctionRuntimeException("Trying to get properties from a deleted object.");
    }
    return TypedValueT(*maybe_vertex_accessor, ctx.memory);
  } else {
    return TypedValueT(args[0].ValueEdge().From(), ctx.memory);
  }
}

namespace {

size_t UnwrapDegreeResult(storage::v3::Result<size_t> maybe_degree) {
  if (maybe_degree.HasError()) {
    switch (maybe_degree.GetError()) {
      case storage::v3::Error::DELETED_OBJECT:
        throw functions::FunctionRuntimeException("Trying to get degree of a deleted node.");
      case storage::v3::Error::NONEXISTENT_OBJECT:
        throw functions::FunctionRuntimeException("Trying to get degree of a node that doesn't exist.");
      case storage::v3::Error::SERIALIZATION_ERROR:
      case storage::v3::Error::VERTEX_HAS_EDGES:
      case storage::v3::Error::PROPERTIES_DISABLED:
      case storage::v3::Error::VERTEX_ALREADY_INSERTED:
        throw functions::FunctionRuntimeException("Unexpected error when getting node degree.");
    }
  }
  return *maybe_degree;
}

}  // namespace

template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT Degree(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex>>("degree", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t out_degree = 0;
  size_t in_degree = 0;
  if constexpr (std::same_as<Tag, StorageEngineTag>) {
    out_degree = UnwrapDegreeResult(vertex.OutDegree(ctx.view));
    in_degree = UnwrapDegreeResult(vertex.InDegree(ctx.view));
  } else {
    out_degree = vertex.OutDegree();
    in_degree = vertex.InDegree();
  }
  return TypedValueT(static_cast<int64_t>(out_degree + in_degree), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT InDegree(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex>>("inDegree", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t in_degree = 0;
  if constexpr (std::same_as<Tag, StorageEngineTag>) {
    in_degree = UnwrapDegreeResult(vertex.InDegree(ctx.view));
  } else {
    in_degree = vertex.InDegree();
  }
  return TypedValueT(static_cast<int64_t>(in_degree), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT OutDegree(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex>>("outDegree", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  size_t out_degree = 0;
  if constexpr (std::same_as<Tag, StorageEngineTag>) {
    out_degree = UnwrapDegreeResult(vertex.OutDegree(ctx.view));
  } else {
    out_degree = vertex.OutDegree();
  }
  return TypedValueT(static_cast<int64_t>(out_degree), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToBoolean(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Bool, Integer, String>>("toBoolean", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsBool()) {
    return TypedValueT(value.ValueBool(), ctx.memory);
  } else if (value.IsInt()) {
    return TypedValueT(value.ValueInt() != 0L, ctx.memory);
  } else {
    auto s = utils::ToUpperCase(utils::Trim(value.ValueString()));
    if (s == "TRUE") return TypedValueT(true, ctx.memory);
    if (s == "FALSE") return TypedValueT(false, ctx.memory);
    // I think this is just stupid and that exception should be thrown, but
    // neo4j does it this way...
    return TypedValueT(ctx.memory);
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToFloat(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Number, String>>("toFloat", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsInt()) {
    return TypedValueT(static_cast<double>(value.ValueInt()), ctx.memory);
  } else if (value.IsDouble()) {
    return TypedValueT(value, ctx.memory);
  } else {
    try {
      return TypedValueT(utils::ParseDouble(utils::Trim(value.ValueString())), ctx.memory);
    } catch (const utils::BasicException &) {
      return TypedValueT(ctx.memory);
    }
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToInteger(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Bool, Number, String>>("toInteger", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsBool()) {
    return TypedValueT(value.ValueBool() ? 1L : 0L, ctx.memory);
  } else if (value.IsInt()) {
    return TypedValueT(value, ctx.memory);
  } else if (value.IsDouble()) {
    return TypedValueT(static_cast<int64_t>(value.ValueDouble()), ctx.memory);
  } else {
    try {
      // Yup, this is correct. String is valid if it has floating point
      // number, then it is parsed and converted to int.
      return TypedValueT(static_cast<int64_t>(utils::ParseDouble(utils::Trim(value.ValueString()))), ctx.memory);
    } catch (const utils::BasicException &) {
      return TypedValueT(ctx.memory);
    }
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Type(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Edge>>("type", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  return TypedValueT(dba->EdgeTypeToName(args[0].ValueEdge().EdgeType()), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ValueType(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Bool, Integer, Double, String, List, Map, Vertex, Edge, Path>>("type", args, nargs);
  // The type names returned should be standardized openCypher type names.
  // https://github.com/opencypher/openCypher/blob/master/docs/openCypher9.pdf
  switch (args[0].type()) {
    case TypedValueT::Type::Null:
      return TypedValueT("NULL", ctx.memory);
    case TypedValueT::Type::Bool:
      return TypedValueT("BOOLEAN", ctx.memory);
    case TypedValueT::Type::Int:
      return TypedValueT("INTEGER", ctx.memory);
    case TypedValueT::Type::Double:
      return TypedValueT("FLOAT", ctx.memory);
    case TypedValueT::Type::String:
      return TypedValueT("STRING", ctx.memory);
    case TypedValueT::Type::List:
      return TypedValueT("LIST", ctx.memory);
    case TypedValueT::Type::Map:
      return TypedValueT("MAP", ctx.memory);
    case TypedValueT::Type::Vertex:
      return TypedValueT("NODE", ctx.memory);
    case TypedValueT::Type::Edge:
      return TypedValueT("RELATIONSHIP", ctx.memory);
    case TypedValueT::Type::Path:
      return TypedValueT("PATH", ctx.memory);
    case TypedValueT::Type::Date:
      return TypedValueT("DATE", ctx.memory);
    case TypedValueT::Type::LocalTime:
      return TypedValueT("LOCAL_TIME", ctx.memory);
    case TypedValueT::Type::LocalDateTime:
      return TypedValueT("LOCAL_DATE_TIME", ctx.memory);
    case TypedValueT::Type::Duration:
      return TypedValueT("DURATION", ctx.memory);
  }
}

// TODO: How is Keys different from Properties function?
template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT Keys(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex, Edge>>("keys", args, nargs);
  auto *dba = ctx.db_accessor;
  auto get_keys = [&](const auto &record_accessor) {
    typename TypedValueT::TVector keys(ctx.memory);
    if constexpr (std::same_as<Tag, StorageEngineTag>) {
      auto maybe_props = record_accessor.Properties(ctx.view);
      if (maybe_props.HasError()) {
        switch (maybe_props.GetError()) {
          case storage::v3::Error::DELETED_OBJECT:
            throw functions::FunctionRuntimeException("Trying to get keys from a deleted object.");
          case storage::v3::Error::NONEXISTENT_OBJECT:
            throw functions::FunctionRuntimeException("Trying to get keys from an object that doesn't exist.");
          case storage::v3::Error::SERIALIZATION_ERROR:
          case storage::v3::Error::VERTEX_HAS_EDGES:
          case storage::v3::Error::PROPERTIES_DISABLED:
          case storage::v3::Error::VERTEX_ALREADY_INSERTED:
            throw functions::FunctionRuntimeException("Unexpected error when getting keys.");
        }
      }
      for (const auto &property : *maybe_props) {
        keys.emplace_back(dba->PropertyToName(property.first));
      }
    } else {
      for (const auto &property : record_accessor.Properties()) {
        keys.emplace_back(dba->PropertyToName(property.first));
      }
    }
    return TypedValueT(std::move(keys));
  };

  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsVertex()) {
    return get_keys(value.ValueVertex());
  } else {
    return get_keys(value.ValueEdge());
  }
}

template <typename TypedValueT, typename FunctionContextT, typename Tag>
TypedValueT Labels(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex>>("labels", args, nargs);
  auto *dba = ctx.db_accessor;
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  typename TypedValueT::TVector labels(ctx.memory);
  if constexpr (std::is_same_v<Tag, StorageEngineTag>) {
    auto maybe_labels = args[0].ValueVertex().Labels(ctx.view);
    if (maybe_labels.HasError()) {
      switch (maybe_labels.GetError()) {
        case storage::v3::Error::DELETED_OBJECT:
          throw functions::FunctionRuntimeException("Trying to get labels from a deleted node.");
        case storage::v3::Error::NONEXISTENT_OBJECT:
          throw functions::FunctionRuntimeException("Trying to get labels from a node that doesn't exist.");
        case storage::v3::Error::SERIALIZATION_ERROR:
        case storage::v3::Error::VERTEX_HAS_EDGES:
        case storage::v3::Error::PROPERTIES_DISABLED:
        case storage::v3::Error::VERTEX_ALREADY_INSERTED:
          throw functions::FunctionRuntimeException("Unexpected error when getting labels.");
      }
    }
    for (const auto &label : *maybe_labels) {
      labels.emplace_back(dba->LabelToName(label));
    }
  } else {
    auto vertex = args[0].ValueVertex();
    for (const auto &label : vertex.Labels()) {
      labels.emplace_back(dba->LabelToName(label.id));
    }
  }
  return TypedValueT(std::move(labels));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Nodes(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Path>>("nodes", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &vertices = args[0].ValuePath().vertices();
  typename TypedValueT::TVector values(ctx.memory);
  values.reserve(vertices.size());
  for (const auto &v : vertices) values.emplace_back(v);
  return TypedValueT(std::move(values));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Relationships(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Path>>("relationships", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &edges = args[0].ValuePath().edges();
  typename TypedValueT::TVector values(ctx.memory);
  values.reserve(edges.size());
  for (const auto &e : edges) values.emplace_back(e);
  return TypedValueT(std::move(values));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Range(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Integer>, Or<Null, Integer>, Optional<Or<Null, NonZeroInteger>>>("range", args, nargs);
  for (int64_t i = 0; i < nargs; ++i)
    if (args[i].IsNull()) return TypedValueT(ctx.memory);
  auto lbound = args[0].ValueInt();
  auto rbound = args[1].ValueInt();
  int64_t step = nargs == 3 ? args[2].ValueInt() : 1;
  typename TypedValueT::TVector list(ctx.memory);
  if (lbound <= rbound && step > 0) {
    for (auto i = lbound; i <= rbound; i += step) {
      list.emplace_back(i);
    }
  } else if (lbound >= rbound && step < 0) {
    for (auto i = lbound; i >= rbound; i += step) {
      list.emplace_back(i);
    }
  }
  return TypedValueT(std::move(list));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Tail(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, List>>("tail", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  typename TypedValueT::TVector list(args[0].ValueList(), ctx.memory);
  if (list.empty()) return TypedValueT(std::move(list));
  list.erase(list.begin());
  return TypedValueT(std::move(list));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT UniformSample(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, List>, Or<Null, NonNegativeInteger>>("uniformSample", args, nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  if (args[0].IsNull() || args[1].IsNull()) return TypedValueT(ctx.memory);
  const auto &population = args[0].ValueList();
  auto population_size = population.size();
  if (population_size == 0) return TypedValueT(ctx.memory);
  auto desired_length = args[1].ValueInt();
  std::uniform_int_distribution<uint64_t> rand_dist{0, population_size - 1};
  typename TypedValueT::TVector sampled(ctx.memory);
  sampled.reserve(desired_length);
  for (int64_t i = 0; i < desired_length; ++i) {
    sampled.emplace_back(population[rand_dist(pseudo_rand_gen_)]);
  }
  return TypedValueT(std::move(sampled));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Abs(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Number>>("abs", args, nargs);
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsInt()) {
    return TypedValueT(std::abs(value.ValueInt()), ctx.memory);
  } else {
    return TypedValueT(std::abs(value.ValueDouble()), ctx.memory);
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                                  \
  template <typename TypedValueT, typename FunctionContextT>                              \
  TypedValueT name(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) { \
    FType<TypedValueT, Or<Null, Number>>(#lowercased_name, args, nargs);                  \
    const auto &value = args[0];                                                          \
    if (value.IsNull()) {                                                                 \
      return TypedValueT(ctx.memory);                                                     \
    } else if (value.IsInt()) {                                                           \
      return TypedValueT(lowercased_name(value.ValueInt()), ctx.memory);                  \
    } else {                                                                              \
      return TypedValueT(lowercased_name(value.ValueDouble()), ctx.memory);               \
    }                                                                                     \
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

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Atan2(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Number>, Or<Null, Number>>("atan2", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValueT(ctx.memory);
  auto to_double = [](const TypedValueT &t) -> double {
    if (t.IsInt()) {
      return t.ValueInt();
    } else {
      return t.ValueDouble();
    }
  };
  double y = to_double(args[0]);
  double x = to_double(args[1]);
  return TypedValueT(atan2(y, x), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Sign(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Number>>("sign", args, nargs);
  auto sign = [&](auto x) { return TypedValueT((0 < x) - (x < 0), ctx.memory); };
  const auto &value = args[0];
  if (value.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (value.IsInt()) {
    return sign(value.ValueInt());
  } else {
    return sign(value.ValueDouble());
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT E(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, void>("e", args, nargs);
  return TypedValueT(M_E, ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Pi(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, void>("pi", args, nargs);
  return TypedValueT(M_PI, ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Rand(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, void>("rand", args, nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  return TypedValueT(rand_dist_(pseudo_rand_gen_), ctx.memory);
}

template <class TPredicate, typename TypedValueT, typename FunctionContextT>
TypedValueT StringMatchOperator(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, Or<Null, String>>(TPredicate::name, args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValueT(ctx.memory);
  const auto &s1 = args[0].ValueString();
  const auto &s2 = args[1].ValueString();
  return TypedValueT(TPredicate{}(s1, s2), ctx.memory);
}

// Check if s1 starts with s2.
template <typename TypedValueT>
struct StartsWithPredicate {
  static constexpr const char *name = "startsWith";
  bool operator()(const typename TypedValueT::TString &s1, const typename TypedValueT::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.begin(), s2.end(), s1.begin());
  }
};

template <typename TypedValueT, typename FunctionContextT>
inline auto StartsWith = StringMatchOperator<StartsWithPredicate<TypedValueT>, TypedValueT, FunctionContextT>;

// Check if s1 ends with s2.
template <typename TypedValueT>
struct EndsWithPredicate {
  static constexpr const char *name = "endsWith";
  bool operator()(const typename TypedValueT::TString &s1, const typename TypedValueT::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
  }
};

template <typename TypedValueT, typename FunctionContextT>
inline auto EndsWith = StringMatchOperator<EndsWithPredicate<TypedValueT>, TypedValueT, FunctionContextT>;

// Check if s1 contains s2.
template <typename TypedValueT>
struct ContainsPredicate {
  static constexpr const char *name = "contains";
  bool operator()(const typename TypedValueT::TString &s1, const typename TypedValueT::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return s1.find(s2) != std::string::npos;
  }
};

template <typename TypedValueT, typename FunctionContextT>
inline auto Contains = StringMatchOperator<ContainsPredicate<TypedValueT>, TypedValueT, FunctionContextT>;

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Assert(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Bool, Optional<String>>("assert", args, nargs);
  if (!args[0].ValueBool()) {
    std::string message("Assertion failed");
    if (nargs == 2) {
      message += ": ";
      message += args[1].ValueString();
    }
    message += ".";
    throw FunctionRuntimeException(message);
  }
  return TypedValueT(args[0], ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Counter(const TypedValueT *args, int64_t nargs, const FunctionContextT &context) {
  FType<TypedValueT, String, Integer, Optional<NonZeroInteger>>("counter", args, nargs);
  int64_t step = 1;
  if (nargs == 3) {
    step = args[2].ValueInt();
  }

  auto [it, inserted] = context.counters->emplace(args[0].ValueString(), args[1].ValueInt());
  auto value = it->second;
  it->second += step;

  return TypedValueT(value, context.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Id(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, Vertex, Edge>>("id", args, nargs);
  const auto &arg = args[0];
  if (arg.IsNull()) {
    return TypedValueT(ctx.memory);
  } else if (arg.IsVertex()) {
    return TypedValueT(static_cast<int64_t>(arg.ValueVertex().CypherId()), ctx.memory);
  } else {
    return TypedValueT(static_cast<int64_t>(arg.ValueEdge().CypherId()), ctx.memory);
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToString(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String, Number, Date, LocalTime, LocalDateTime, Duration, Bool>>("toString", args, nargs);
  const auto &arg = args[0];
  if (arg.IsNull()) {
    return TypedValueT(ctx.memory);
  }
  if (arg.IsString()) {
    return TypedValueT(arg, ctx.memory);
  }
  if (arg.IsInt()) {
    // TODO: This is making a pointless copy of std::string, we may want to
    // use a different conversion to string
    return TypedValueT(std::to_string(arg.ValueInt()), ctx.memory);
  }
  if (arg.IsDouble()) {
    return TypedValueT(std::to_string(arg.ValueDouble()), ctx.memory);
  }
  if (arg.IsDate()) {
    return TypedValueT(arg.ValueDate().ToString(), ctx.memory);
  }
  if (arg.IsLocalTime()) {
    return TypedValueT(arg.ValueLocalTime().ToString(), ctx.memory);
  }
  if (arg.IsLocalDateTime()) {
    return TypedValueT(arg.ValueLocalDateTime().ToString(), ctx.memory);
  }
  if (arg.IsDuration()) {
    return TypedValueT(arg.ValueDuration().ToString(), ctx.memory);
  }

  return TypedValueT(arg.ValueBool() ? "true" : "false", ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Timestamp(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Optional<Or<Date, LocalTime, LocalDateTime, Duration>>>("timestamp", args, nargs);
  const auto &arg = *args;
  if (arg.IsDate()) {
    return TypedValueT(arg.ValueDate().MicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsLocalTime()) {
    return TypedValueT(arg.ValueLocalTime().MicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsLocalDateTime()) {
    return TypedValueT(arg.ValueLocalDateTime().MicrosecondsSinceEpoch(), ctx.memory);
  }
  if (arg.IsDuration()) {
    return TypedValueT(arg.ValueDuration().microseconds, ctx.memory);
  }
  return TypedValueT(ctx.timestamp, ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Left(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, Or<Null, NonNegativeInteger>>("left", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValueT(ctx.memory);
  return TypedValueT(utils::Substr(args[0].ValueString(), 0, args[1].ValueInt()), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Right(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, Or<Null, NonNegativeInteger>>("right", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValueT(ctx.memory);
  const auto &str = args[0].ValueString();
  auto len = args[1].ValueInt();
  return len <= str.size() ? TypedValueT(utils::Substr(str, str.size() - len, len), ctx.memory)
                           : TypedValueT(str, ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT CallStringFunction(
    const TypedValueT *args, int64_t nargs, utils::MemoryResource *memory, const char *name,
    std::function<typename TypedValueT::TString(const typename TypedValueT::TString &)> fun) {
  FType<TypedValueT, Or<Null, String>>(name, args, nargs);
  if (args[0].IsNull()) return TypedValueT(memory);
  return TypedValueT(fun(args[0].ValueString()), memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT LTrim(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(args, nargs, ctx.memory, "lTrim", [&](const auto &str) {
    return typename TypedValueT::TString(utils::LTrim(str), ctx.memory);
  });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT RTrim(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(args, nargs, ctx.memory, "rTrim", [&](const auto &str) {
    return typename TypedValueT::TString(utils::RTrim(str), ctx.memory);
  });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Trim(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(args, nargs, ctx.memory, "trim", [&](const auto &str) {
    return typename TypedValueT::TString(utils::Trim(str), ctx.memory);
  });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Reverse(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(
      args, nargs, ctx.memory, "reverse", [&](const auto &str) { return utils::Reversed(str, ctx.memory); });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToLower(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(args, nargs, ctx.memory, "toLower", [&](const auto &str) {
    typename TypedValueT::TString res(ctx.memory);
    utils::ToLowerCase(&res, str);
    return res;
  });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToUpper(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  return CallStringFunction<TypedValueT, FunctionContextT>(args, nargs, ctx.memory, "toUpper", [&](const auto &str) {
    typename TypedValueT::TString res(ctx.memory);
    utils::ToUpperCase(&res, str);
    return res;
  });
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Replace(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, Or<Null, String>, Or<Null, String>>("replace", args, nargs);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValueT(ctx.memory);
  }
  typename TypedValueT::TString replaced(ctx.memory);
  utils::Replace(&replaced, args[0].ValueString(), args[1].ValueString(), args[2].ValueString());
  return TypedValueT(std::move(replaced));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Split(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, Or<Null, String>>("split", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) {
    return TypedValueT(ctx.memory);
  }
  typename TypedValueT::TVector result(ctx.memory);
  utils::Split(&result, args[0].ValueString(), args[1].ValueString());
  return TypedValueT(std::move(result));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Substring(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<Null, String>, NonNegativeInteger, Optional<NonNegativeInteger>>("substring", args, nargs);
  if (args[0].IsNull()) return TypedValueT(ctx.memory);
  const auto &str = args[0].ValueString();
  auto start = args[1].ValueInt();
  if (nargs == 2) return TypedValueT(utils::Substr(str, start), ctx.memory);
  auto len = args[2].ValueInt();
  return TypedValueT(utils::Substr(str, start, len), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT ToByteString(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, String>("toByteString", args, nargs);
  const auto &str = args[0].ValueString();
  if (str.empty()) return TypedValueT("", ctx.memory);
  if (!utils::StartsWith(str, "0x") && !utils::StartsWith(str, "0X")) {
    throw FunctionRuntimeException("'toByteString' argument must start with '0x'");
  }
  const auto &hex_str = utils::Substr(str, 2);
  auto read_hex = [](const char ch) -> unsigned char {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    throw FunctionRuntimeException("'toByteString' argument has an invalid character '{}'", ch);
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
  return TypedValueT(std::move(bytes));
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT FromByteString(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, String, Optional<PositiveInteger>>("fromByteString", args, nargs);
  const auto &bytes = args[0].ValueString();
  if (bytes.empty()) return TypedValueT("", ctx.memory);
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
  return TypedValueT(std::move(str));
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
        throw FunctionRuntimeException("Invalid value for key '{}'. Expected {}", key, error);
      }
    } else {
      throw FunctionRuntimeException("Unknown key '{}'.", key);
    }
  }
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Date(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Optional<Or<String, Map>>>("date", args, nargs);
  if (nargs == 0) {
    return TypedValueT(utils::LocalDateTime(ctx.timestamp).date, ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[date_parameters, is_extended] = utils::ParseDateParameters(args[0].ValueString());
    return TypedValueT(utils::Date(date_parameters), ctx.memory);
  }

  utils::DateParameters date_parameters;

  using namespace std::literals;
  std::unordered_map parameter_mappings = {std::pair{"year"sv, &date_parameters.year},
                                           std::pair{"month"sv, &date_parameters.month},
                                           std::pair{"day"sv, &date_parameters.day}};

  MapNumericParameters<Integer>(parameter_mappings, args[0].ValueMap());
  return TypedValueT(utils::Date(date_parameters), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT LocalTime(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Optional<Or<String, Map>>>("localtime", args, nargs);

  if (nargs == 0) {
    return TypedValueT(utils::LocalDateTime(ctx.timestamp).local_time, ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[local_time_parameters, is_extended] = utils::ParseLocalTimeParameters(args[0].ValueString());
    return TypedValueT(utils::LocalTime(local_time_parameters), ctx.memory);
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
  return TypedValueT(utils::LocalTime(local_time_parameters), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT LocalDateTime(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Optional<Or<String, Map>>>("localdatetime", args, nargs);

  if (nargs == 0) {
    return TypedValueT(utils::LocalDateTime(ctx.timestamp), ctx.memory);
  }

  if (args[0].IsString()) {
    const auto &[date_parameters, local_time_parameters] = ParseLocalDateTimeParameters(args[0].ValueString());
    return TypedValueT(utils::LocalDateTime(date_parameters, local_time_parameters), ctx.memory);
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
  return TypedValueT(utils::LocalDateTime(date_parameters, local_time_parameters), ctx.memory);
}

template <typename TypedValueT, typename FunctionContextT>
TypedValueT Duration(const TypedValueT *args, int64_t nargs, const FunctionContextT &ctx) {
  FType<TypedValueT, Or<String, Map>>("duration", args, nargs);

  if (args[0].IsString()) {
    return TypedValueT(utils::Duration(ParseDurationParameters(args[0].ValueString())), ctx.memory);
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
  return TypedValueT(utils::Duration(duration_parameters), ctx.memory);
}

}  // namespace memgraph::functions::impl

namespace memgraph::functions {

template <typename TypedValueT, typename FunctionContextT, typename Tag, typename Conv>
std::function<TypedValueT(const TypedValueT *arguments, int64_t num_arguments, const FunctionContextT &context)>
NameToFunction(const std::string &function_name) {
  // Scalar functions
  if (function_name == "DEGREE") return functions::impl::Degree<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "INDEGREE") return functions::impl::InDegree<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "OUTDEGREE") return functions::impl::OutDegree<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "ENDNODE") return functions::impl::EndNode<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "HEAD") return functions::impl::Head<TypedValueT, FunctionContextT>;
  if (function_name == kId) return functions::impl::Id<TypedValueT, FunctionContextT>;
  if (function_name == "LAST") return functions::impl::Last<TypedValueT, FunctionContextT>;
  if (function_name == "PROPERTIES") return functions::impl::Properties<TypedValueT, FunctionContextT, Tag, Conv>;
  if (function_name == "SIZE") return functions::impl::Size<TypedValueT, FunctionContextT>;
  if (function_name == "STARTNODE") return functions::impl::StartNode<TypedValueT, FunctionContextT, Tag, Conv>;
  if (function_name == "TIMESTAMP") return functions::impl::Timestamp<TypedValueT, FunctionContextT>;
  if (function_name == "TOBOOLEAN") return functions::impl::ToBoolean<TypedValueT, FunctionContextT>;
  if (function_name == "TOFLOAT") return functions::impl::ToFloat<TypedValueT, FunctionContextT>;
  if (function_name == "TOINTEGER") return functions::impl::ToInteger<TypedValueT, FunctionContextT>;
  if (function_name == "TYPE") return functions::impl::Type<TypedValueT, FunctionContextT>;
  if (function_name == "VALUETYPE") return functions::impl::ValueType<TypedValueT, FunctionContextT>;

  // List functions
  if (function_name == "KEYS") return functions::impl::Keys<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "LABELS") return functions::impl::Labels<TypedValueT, FunctionContextT, Tag>;
  if (function_name == "NODES") return functions::impl::Nodes<TypedValueT, FunctionContextT>;
  if (function_name == "RANGE") return functions::impl::Range<TypedValueT, FunctionContextT>;
  if (function_name == "RELATIONSHIPS") return functions::impl::Relationships<TypedValueT, FunctionContextT>;
  if (function_name == "TAIL") return functions::impl::Tail<TypedValueT, FunctionContextT>;
  if (function_name == "UNIFORMSAMPLE") return functions::impl::UniformSample<TypedValueT, FunctionContextT>;

  // Mathematical functions - numeric
  if (function_name == "ABS") return functions::impl::Abs<TypedValueT, FunctionContextT>;
  if (function_name == "CEIL") return functions::impl::Ceil<TypedValueT, FunctionContextT>;
  if (function_name == "FLOOR") return functions::impl::Floor<TypedValueT, FunctionContextT>;
  if (function_name == "RAND") return functions::impl::Rand<TypedValueT, FunctionContextT>;
  if (function_name == "ROUND") return functions::impl::Round<TypedValueT, FunctionContextT>;
  if (function_name == "SIGN") return functions::impl::Sign<TypedValueT, FunctionContextT>;

  // Mathematical functions - logarithmic
  if (function_name == "E") return functions::impl::E<TypedValueT, FunctionContextT>;
  if (function_name == "EXP") return functions::impl::Exp<TypedValueT, FunctionContextT>;
  if (function_name == "LOG") return functions::impl::Log<TypedValueT, FunctionContextT>;
  if (function_name == "LOG10") return functions::impl::Log10<TypedValueT, FunctionContextT>;
  if (function_name == "SQRT") return functions::impl::Sqrt<TypedValueT, FunctionContextT>;

  // Mathematical functions - trigonometric
  if (function_name == "ACOS") return functions::impl::Acos<TypedValueT, FunctionContextT>;
  if (function_name == "ASIN") return functions::impl::Asin<TypedValueT, FunctionContextT>;
  if (function_name == "ATAN") return functions::impl::Atan<TypedValueT, FunctionContextT>;
  if (function_name == "ATAN2") return functions::impl::Atan2<TypedValueT, FunctionContextT>;
  if (function_name == "COS") return functions::impl::Cos<TypedValueT, FunctionContextT>;
  if (function_name == "PI") return functions::impl::Pi<TypedValueT, FunctionContextT>;
  if (function_name == "SIN") return functions::impl::Sin<TypedValueT, FunctionContextT>;
  if (function_name == "TAN") return functions::impl::Tan<TypedValueT, FunctionContextT>;

  // String functions
  if (function_name == kContains) return functions::impl::Contains<TypedValueT, FunctionContextT>;
  if (function_name == kEndsWith) return functions::impl::EndsWith<TypedValueT, FunctionContextT>;
  if (function_name == "LEFT") return functions::impl::Left<TypedValueT, FunctionContextT>;
  if (function_name == "LTRIM") return functions::impl::LTrim<TypedValueT, FunctionContextT>;
  if (function_name == "REPLACE") return functions::impl::Replace<TypedValueT, FunctionContextT>;
  if (function_name == "REVERSE") return functions::impl::Reverse<TypedValueT, FunctionContextT>;
  if (function_name == "RIGHT") return functions::impl::Right<TypedValueT, FunctionContextT>;
  if (function_name == "RTRIM") return functions::impl::RTrim<TypedValueT, FunctionContextT>;
  if (function_name == "SPLIT") return functions::impl::Split<TypedValueT, FunctionContextT>;
  if (function_name == kStartsWith) return functions::impl::StartsWith<TypedValueT, FunctionContextT>;
  if (function_name == "SUBSTRING") return functions::impl::Substring<TypedValueT, FunctionContextT>;
  if (function_name == "TOLOWER") return functions::impl::ToLower<TypedValueT, FunctionContextT>;
  if (function_name == "TOSTRING") return functions::impl::ToString<TypedValueT, FunctionContextT>;
  if (function_name == "TOUPPER") return functions::impl::ToUpper<TypedValueT, FunctionContextT>;
  if (function_name == "TRIM") return functions::impl::Trim<TypedValueT, FunctionContextT>;

  // Memgraph specific functions
  if (function_name == "ASSERT") return functions::impl::Assert<TypedValueT, FunctionContextT>;
  if (function_name == "COUNTER") return functions::impl::Counter<TypedValueT, FunctionContextT>;
  if (function_name == "TOBYTESTRING") return functions::impl::ToByteString<TypedValueT, FunctionContextT>;
  if (function_name == "FROMBYTESTRING") return functions::impl::FromByteString<TypedValueT, FunctionContextT>;

  // Functions for temporal types
  if (function_name == "DATE") return functions::impl::Date<TypedValueT, FunctionContextT>;
  if (function_name == "LOCALTIME") return functions::impl::LocalTime<TypedValueT, FunctionContextT>;
  if (function_name == "LOCALDATETIME") return functions::impl::LocalDateTime<TypedValueT, FunctionContextT>;
  if (function_name == "DURATION") return functions::impl::Duration<TypedValueT, FunctionContextT>;

  return nullptr;
}

}  // namespace memgraph::functions
