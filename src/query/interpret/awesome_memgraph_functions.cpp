#include "query/interpret/awesome_memgraph_functions.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <random>

#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "utils/string.hpp"

namespace query {
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
struct Number {};
struct List {};
struct String {};
struct Map {};
struct Edge {};
struct Vertex {};
struct Path {};

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
  } else if constexpr (std::is_same_v<ArgType, void>) {
    return true;
  } else {
    static_assert(std::is_same_v<ArgType, Null>, "Unknown ArgType");
  }
  return false;
}

template <class ArgType>
constexpr const char *ArgTypeName() {
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
    return "edge";
  } else if constexpr (std::is_same_v<ArgType, Path>) {
    return "path";
  } else if constexpr (std::is_same_v<ArgType, void>) {
    return "void";
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
      return fmt::format("'{}', {}", ArgTypeName<ArgType>(),
                         Or<ArgTypes...>::TypeNames());
    } else {
      return fmt::format("'{}' or '{}'", ArgTypeName<ArgType>(),
                         Or<ArgTypes...>::TypeNames());
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

  static void Check(const char *name, const TypedValue *args, int64_t nargs,
                    int64_t pos) {
    if (nargs == 0) return;
    const TypedValue &arg = args[0];
    if constexpr (IsOrType<ArgType>::value) {
      if (!ArgType::Check(arg)) {
        throw QueryRuntimeException(
            "Optional '{}' argument at position {} must be either {}.", name,
            pos, ArgType::TypeNames());
      }
    } else {
      if (!ArgIsType<ArgType>(arg))
        throw QueryRuntimeException(
            "Optional '{}' argument at position {} must be '{}'.", name, pos,
            ArgTypeName<ArgType>());
    }
  }
};

template <class ArgType, class... ArgTypes>
struct Optional<ArgType, ArgTypes...> {
  static constexpr size_t size = 1 + sizeof...(ArgTypes);

  static void Check(const char *name, const TypedValue *args, int64_t nargs,
                    int64_t pos) {
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
void FType(const char *name, const TypedValue *args, int64_t nargs,
           int64_t pos = 1) {
  if constexpr (std::is_same_v<ArgType, void>) {
    if (nargs != 0) {
      throw QueryRuntimeException("'{}' requires no arguments.", name);
    }
    return;
  }
  constexpr int64_t required_args = FTypeRequiredArgs<ArgType, ArgTypes...>();
  constexpr int64_t optional_args = FTypeOptionalArgs<ArgType, ArgTypes...>();
  constexpr int64_t total_args = required_args + optional_args;
  if constexpr (optional_args > 0) {
    if (nargs < required_args || nargs > total_args) {
      throw QueryRuntimeException("'{}' requires between {} and {} arguments.",
                                  name, required_args, total_args);
    }
  } else {
    if (nargs != required_args) {
      throw QueryRuntimeException(
          "'{}' requires exactly {} {}.", name, required_args,
          required_args == 1 ? "argument" : "arguments");
    }
  }
  const TypedValue &arg = args[0];
  if constexpr (IsOrType<ArgType>::value) {
    if (!ArgType::Check(arg)) {
      throw QueryRuntimeException(
          "'{}' argument at position {} must be either {}.", name, pos,
          ArgType::TypeNames());
    }
  } else if constexpr (IsOptional<ArgType>::value) {
    static_assert(sizeof...(ArgTypes) == 0, "Optional arguments must be last!");
    ArgType::Check(name, args, nargs, pos);
  } else {
    if (!ArgIsType<ArgType>(arg)) {
      throw QueryRuntimeException("'{}' argument at position {} must be '{}'",
                                  name, pos, ArgTypeName<ArgType>());
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

/////////////////////////// IMPORTANT NOTE! ////////////////////////////////////
// All of the functions take mutable `TypedValue *` to arguments, but none of
// the functions should ever need to actually modify the arguments! Let's try to
// keep our sanity in a good state by treating the arguments as immutable.
////////////////////////////////////////////////////////////////////////////////

TypedValue EndNode(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx, database::GraphDbAccessor *) {
  FType<Or<Null, Edge>>("endNode", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(args[0].ValueEdge().to(), ctx.memory);
}

TypedValue Head(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<Or<Null, List>>("head", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValue(ctx.memory);
  return TypedValue(list[0], ctx.memory);
}

TypedValue Last(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<Or<Null, List>>("last", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &list = args[0].ValueList();
  if (list.empty()) return TypedValue(ctx.memory);
  return TypedValue(list.back(), ctx.memory);
}

TypedValue Properties(TypedValue *args, int64_t nargs,
                      const EvaluationContext &ctx,
                      database::GraphDbAccessor *dba) {
  FType<Or<Null, Vertex, Edge>>("properties", args, nargs);
  auto get_properties = [&](const auto &record_accessor) {
    TypedValue::TMap properties(ctx.memory);
    for (const auto &property : record_accessor.Properties()) {
      properties.emplace(dba->PropertyName(property.first), property.second);
    }
    return TypedValue(std::move(properties));
  };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Vertex:
      return get_properties(args[0].Value<VertexAccessor>());
    case TypedValue::Type::Edge:
      return get_properties(args[0].Value<EdgeAccessor>());
    default:
      throw QueryRuntimeException(
          "'properties' argument must be a node or an edge.");
  }
}

TypedValue Size(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<Or<Null, List, String, Map, Path>>("size", args, nargs);
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::List:
      return TypedValue(static_cast<int64_t>(args[0].ValueList().size()),
                        ctx.memory);
    case TypedValue::Type::String:
      return TypedValue(static_cast<int64_t>(args[0].ValueString().size()),
                        ctx.memory);
    case TypedValue::Type::Map:
      // neo4j doesn't implement size for map, but I don't see a good reason not
      // to do it.
      return TypedValue(static_cast<int64_t>(args[0].ValueMap().size()),
                        ctx.memory);
    case TypedValue::Type::Path:
      return TypedValue(
          static_cast<int64_t>(args[0].ValuePath().edges().size()), ctx.memory);
    default:
      throw QueryRuntimeException(
          "'size' argument must be a string, a collection or a path.");
  }
}

TypedValue StartNode(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<Or<Null, Edge>>("startNode", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(args[0].ValueEdge().from(), ctx.memory);
}

TypedValue Degree(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                  database::GraphDbAccessor *) {
  FType<Or<Null, Vertex>>("degree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  return TypedValue(
      static_cast<int64_t>(vertex.out_degree() + vertex.in_degree()),
      ctx.memory);
}

TypedValue InDegree(TypedValue *args, int64_t nargs,
                    const EvaluationContext &ctx, database::GraphDbAccessor *) {
  FType<Or<Null, Vertex>>("inDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  return TypedValue(static_cast<int64_t>(vertex.in_degree()), ctx.memory);
}

TypedValue OutDegree(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<Or<Null, Vertex>>("outDegree", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertex = args[0].ValueVertex();
  return TypedValue(static_cast<int64_t>(vertex.out_degree()), ctx.memory);
}

TypedValue ToBoolean(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<Or<Null, Bool, Integer, String>>("toBoolean", args, nargs);
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Bool:
      return TypedValue(args[0].Value<bool>(), ctx.memory);
    case TypedValue::Type::Int:
      return TypedValue(args[0].ValueInt() != 0L, ctx.memory);
    case TypedValue::Type::String: {
      auto s = utils::ToUpperCase(utils::Trim(args[0].ValueString()));
      if (s == "TRUE") return TypedValue(true, ctx.memory);
      if (s == "FALSE") return TypedValue(false, ctx.memory);
      // I think this is just stupid and that exception should be thrown, but
      // neo4j does it this way...
      return TypedValue(ctx.memory);
    }
    default:
      throw QueryRuntimeException(
          "'toBoolean' argument must be an integer, a string or a boolean.");
  }
}

TypedValue ToFloat(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx, database::GraphDbAccessor *) {
  FType<Or<Null, Number, String>>("toFloat", args, nargs);
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Int:
      return TypedValue(static_cast<double>(args[0].Value<int64_t>()),
                        ctx.memory);
    case TypedValue::Type::Double:
      return TypedValue(args[0], ctx.memory);
    case TypedValue::Type::String:
      try {
        return TypedValue(
            utils::ParseDouble(utils::Trim(args[0].ValueString())), ctx.memory);
      } catch (const utils::BasicException &) {
        return TypedValue(ctx.memory);
      }
    default:
      throw QueryRuntimeException(
          "'toFloat' argument must be a string or a number.");
  }
}

TypedValue ToInteger(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<Or<Null, Bool, Number, String>>("toInteger", args, nargs);
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Bool:
      return TypedValue(args[0].ValueBool() ? 1L : 0L, ctx.memory);
    case TypedValue::Type::Int:
      return TypedValue(args[0], ctx.memory);
    case TypedValue::Type::Double:
      return TypedValue(static_cast<int64_t>(args[0].Value<double>()),
                        ctx.memory);
    case TypedValue::Type::String:
      try {
        // Yup, this is correct. String is valid if it has floating point
        // number, then it is parsed and converted to int.
        return TypedValue(static_cast<int64_t>(utils::ParseDouble(
                              utils::Trim(args[0].ValueString()))),
                          ctx.memory);
      } catch (const utils::BasicException &) {
        return TypedValue(ctx.memory);
      }
    default:
      throw QueryRuntimeException(
          "'toInteger' argument must be a string, a boolean or a number.");
  }
}

TypedValue Type(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *dba) {
  FType<Or<Null, Edge>>("type", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(dba->EdgeTypeName(args[0].ValueEdge().EdgeType()),
                    ctx.memory);
}

TypedValue Keys(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *dba) {
  FType<Or<Null, Vertex, Edge>>("keys", args, nargs);
  auto get_keys = [&](const auto &record_accessor) {
    TypedValue::TVector keys(ctx.memory);
    for (const auto &property : record_accessor.Properties()) {
      keys.emplace_back(dba->PropertyName(property.first));
    }
    return TypedValue(std::move(keys));
  };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Vertex:
      return get_keys(args[0].Value<VertexAccessor>());
    case TypedValue::Type::Edge:
      return get_keys(args[0].Value<EdgeAccessor>());
    default:
      throw QueryRuntimeException("'keys' argument must be a node or an edge.");
  }
}

TypedValue Labels(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                  database::GraphDbAccessor *dba) {
  FType<Or<Null, Vertex>>("labels", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  TypedValue::TVector labels(ctx.memory);
  for (const auto &label : args[0].ValueVertex().labels()) {
    labels.emplace_back(dba->LabelName(label));
  }
  return TypedValue(std::move(labels));
}

TypedValue Nodes(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *) {
  FType<Or<Null, Path>>("nodes", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &vertices = args[0].ValuePath().vertices();
  TypedValue::TVector values(ctx.memory);
  values.reserve(vertices.size());
  for (const auto &v : vertices) values.emplace_back(v);
  return TypedValue(std::move(values));
}

TypedValue Relationships(TypedValue *args, int64_t nargs,
                         const EvaluationContext &ctx,
                         database::GraphDbAccessor *) {
  FType<Or<Null, Path>>("relationships", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &edges = args[0].ValuePath().edges();
  TypedValue::TVector values(ctx.memory);
  values.reserve(edges.size());
  for (const auto &e : edges) values.emplace_back(e);
  return TypedValue(std::move(values));
}

TypedValue Range(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *) {
  FType<Or<Null, Integer>, Or<Null, Integer>,
        Optional<Or<Null, NonZeroInteger>>>("range", args, nargs);
  for (int64_t i = 0; i < nargs; ++i)
    if (args[i].IsNull()) return TypedValue(ctx.memory);
  auto lbound = args[0].Value<int64_t>();
  auto rbound = args[1].Value<int64_t>();
  int64_t step = nargs == 3 ? args[2].Value<int64_t>() : 1;
  TypedValue::TVector list(ctx.memory);
  if (lbound <= rbound && step > 0) {
    for (auto i = lbound; i <= rbound; i += step) {
      list.emplace_back(i);
    }
  } else if (lbound >= rbound && step < 0) {
    for (auto i = lbound; i >= rbound; i += step) {
      list.emplace_back(i);
    }
  }
  return TypedValue(std::move(list));
}

TypedValue Tail(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<Or<Null, List>>("tail", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  TypedValue::TVector list(args[0].ValueList(), ctx.memory);
  if (list.empty()) return TypedValue(std::move(list));
  list.erase(list.begin());
  return TypedValue(std::move(list));
}

TypedValue UniformSample(TypedValue *args, int64_t nargs,
                         const EvaluationContext &ctx,
                         database::GraphDbAccessor *) {
  FType<Or<Null, List>, Or<Null, NonNegativeInteger>>("uniformSample", args,
                                                      nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &population = args[0].ValueList();
  auto population_size = population.size();
  if (population_size == 0) return TypedValue(ctx.memory);
  auto desired_length = args[1].ValueInt();
  std::uniform_int_distribution<uint64_t> rand_dist{0, population_size - 1};
  TypedValue::TVector sampled(ctx.memory);
  sampled.reserve(desired_length);
  for (int i = 0; i < desired_length; ++i) {
    sampled.emplace_back(population[rand_dist(pseudo_rand_gen_)]);
  }
  return TypedValue(std::move(sampled));
}

TypedValue Abs(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
               database::GraphDbAccessor *) {
  FType<Or<Null, Number>>("abs", args, nargs);
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Int:
      return TypedValue(std::abs(args[0].Value<int64_t>()), ctx.memory);
    case TypedValue::Type::Double:
      return TypedValue(std::abs(args[0].Value<double>()), ctx.memory);
    default:
      throw QueryRuntimeException("'abs' argument should be a number.");
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                       \
  TypedValue name(TypedValue *args, int64_t nargs,                             \
                  const EvaluationContext &ctx, database::GraphDbAccessor *) { \
    FType<Or<Null, Number>>(#lowercased_name, args, nargs);                    \
    switch (args[0].type()) {                                                  \
      case TypedValue::Type::Null:                                             \
        return TypedValue(ctx.memory);                                         \
      case TypedValue::Type::Int:                                              \
        return TypedValue(lowercased_name(args[0].Value<int64_t>()),           \
                          ctx.memory);                                         \
      case TypedValue::Type::Double:                                           \
        return TypedValue(lowercased_name(args[0].Value<double>()),            \
                          ctx.memory);                                         \
      default:                                                                 \
        throw QueryRuntimeException(#lowercased_name                           \
                                    " argument must be a number.");            \
    }                                                                          \
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

TypedValue Atan2(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *) {
  FType<Or<Null, Number>, Or<Null, Number>>("atan2", args, nargs);
  if (args[0].type() == TypedValue::Type::Null) return TypedValue(ctx.memory);
  if (args[1].type() == TypedValue::Type::Null) return TypedValue(ctx.memory);
  auto to_double = [](const TypedValue &t) -> double {
    switch (t.type()) {
      case TypedValue::Type::Int:
        return t.Value<int64_t>();
      case TypedValue::Type::Double:
        return t.Value<double>();
      default:
        throw QueryRuntimeException("Arguments of 'atan2' must be numbers.");
    }
  };
  double y = to_double(args[0]);
  double x = to_double(args[1]);
  return TypedValue(atan2(y, x), ctx.memory);
}

TypedValue Sign(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<Or<Null, Number>>("sign", args, nargs);
  auto sign = [&](auto x) { return TypedValue((0 < x) - (x < 0), ctx.memory); };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::Int:
      return sign(args[0].Value<int64_t>());
    case TypedValue::Type::Double:
      return sign(args[0].Value<double>());
    default:
      throw QueryRuntimeException("'sign' argument must be a number.");
  }
}

TypedValue E(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
             database::GraphDbAccessor *) {
  FType<void>("e", args, nargs);
  return TypedValue(M_E, ctx.memory);
}

TypedValue Pi(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
              database::GraphDbAccessor *) {
  FType<void>("pi", args, nargs);
  return TypedValue(M_PI, ctx.memory);
}

TypedValue Rand(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  FType<void>("rand", args, nargs);
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  return TypedValue(rand_dist_(pseudo_rand_gen_), ctx.memory);
}

template <class TPredicate>
TypedValue StringMatchOperator(TypedValue *args, int64_t nargs,
                               const EvaluationContext &ctx,
                               database::GraphDbAccessor *) {
  FType<Or<Null, String>, Or<Null, String>>(TPredicate::name, args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &s1 = args[0].ValueString();
  const auto &s2 = args[1].ValueString();
  return TypedValue(TPredicate{}(s1, s2), ctx.memory);
}

// Check if s1 starts with s2.
struct StartsWithPredicate {
  constexpr static const char *name = "startsWith";
  bool operator()(const TypedValue::TString &s1,
                  const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.begin(), s2.end(), s1.begin());
  }
};
auto StartsWith = StringMatchOperator<StartsWithPredicate>;

// Check if s1 ends with s2.
struct EndsWithPredicate {
  constexpr static const char *name = "endsWith";
  bool operator()(const TypedValue::TString &s1,
                  const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
  }
};
auto EndsWith = StringMatchOperator<EndsWithPredicate>;

// Check if s1 contains s2.
struct ContainsPredicate {
  constexpr static const char *name = "contains";
  bool operator()(const TypedValue::TString &s1,
                  const TypedValue::TString &s2) const {
    if (s1.size() < s2.size()) return false;
    return s1.find(s2) != std::string::npos;
  }
};
auto Contains = StringMatchOperator<ContainsPredicate>;

TypedValue Assert(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                  database::GraphDbAccessor *) {
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

TypedValue Counter(TypedValue *args, int64_t nargs,
                   const EvaluationContext &context,
                   database::GraphDbAccessor *) {
  FType<String, Integer, Optional<NonZeroInteger>>("counter", args, nargs);
  int64_t step = 1;
  if (nargs == 3) {
    step = args[2].ValueInt();
  }

  auto [it, inserted] =
      context.counters.emplace(args[0].ValueString(), args[1].ValueInt());
  auto value = it->second;
  it->second += step;

  return TypedValue(value, context.memory);
}

TypedValue Id(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
              database::GraphDbAccessor *dba) {
  FType<Or<Vertex, Edge>>("id", args, nargs);
  const auto &arg = args[0];
  if (arg.IsVertex())
    return TypedValue(arg.ValueVertex().CypherId(), ctx.memory);
  else
    return TypedValue(arg.ValueEdge().CypherId(), ctx.memory);
}

TypedValue ToString(TypedValue *args, int64_t nargs,
                    const EvaluationContext &ctx, database::GraphDbAccessor *) {
  FType<Or<Null, String, Number, Bool>>("toString", args, nargs);
  const auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Null:
      return TypedValue(ctx.memory);
    case TypedValue::Type::String:
      return TypedValue(arg, ctx.memory);
    case TypedValue::Type::Int:
      // TODO: This is making a pointless copy of std::string, we may want to
      // use a different conversion to string
      return TypedValue(std::to_string(arg.ValueInt()), ctx.memory);
    case TypedValue::Type::Double:
      return TypedValue(std::to_string(arg.ValueDouble()), ctx.memory);
    case TypedValue::Type::Bool:
      return TypedValue(arg.ValueBool() ? "true" : "false", ctx.memory);
    default:
      throw QueryRuntimeException(
          "'toString' argument must be a number, a string or a boolean.");
  }
}

TypedValue Timestamp(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<void>("timestamp", args, nargs);
  return TypedValue(ctx.timestamp, ctx.memory);
}

TypedValue Left(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *dba) {
  FType<Or<Null, String>, Or<Null, NonNegativeInteger>>("left", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  return TypedValue(utils::Substr(args[0].ValueString(), 0, args[1].ValueInt()),
                    ctx.memory);
}

TypedValue Right(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *dba) {
  FType<Or<Null, String>, Or<Null, NonNegativeInteger>>("right", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) return TypedValue(ctx.memory);
  const auto &str = args[0].ValueString();
  auto len = args[1].ValueInt();
  return len <= str.size()
             ? TypedValue(utils::Substr(str, str.size() - len, len), ctx.memory)
             : TypedValue(str, ctx.memory);
}

TypedValue CallStringFunction(
    TypedValue *args, int64_t nargs, utils::MemoryResource *memory,
    const char *name,
    std::function<TypedValue::TString(const TypedValue::TString &)> fun) {
  FType<Or<Null, String>>(name, args, nargs);
  if (args[0].IsNull()) return TypedValue(memory);
  return TypedValue(fun(args[0].ValueString()), memory);
}

TypedValue LTrim(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *) {
  return CallStringFunction(
      args, nargs, ctx.memory, "lTrim", [&](const auto &str) {
        return TypedValue::TString(utils::LTrim(str), ctx.memory);
      });
}

TypedValue RTrim(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *) {
  return CallStringFunction(
      args, nargs, ctx.memory, "rTrim", [&](const auto &str) {
        return TypedValue::TString(utils::RTrim(str), ctx.memory);
      });
}

TypedValue Trim(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                database::GraphDbAccessor *) {
  return CallStringFunction(
      args, nargs, ctx.memory, "trim", [&](const auto &str) {
        return TypedValue::TString(utils::Trim(str), ctx.memory);
      });
}

TypedValue Reverse(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx, database::GraphDbAccessor *) {
  return CallStringFunction(
      args, nargs, ctx.memory, "reverse",
      [&](const auto &str) { return utils::Reversed(str, ctx.memory); });
}

TypedValue ToLower(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx, database::GraphDbAccessor *) {
  return CallStringFunction(args, nargs, ctx.memory, "toLower",
                            [&](const auto &str) {
                              TypedValue::TString res(ctx.memory);
                              utils::ToLowerCase(&res, str);
                              return res;
                            });
}

TypedValue ToUpper(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx, database::GraphDbAccessor *) {
  return CallStringFunction(args, nargs, ctx.memory, "toUpper",
                            [&](const auto &str) {
                              TypedValue::TString res(ctx.memory);
                              utils::ToUpperCase(&res, str);
                              return res;
                            });
}

TypedValue Replace(TypedValue *args, int64_t nargs,
                   const EvaluationContext &ctx,
                   database::GraphDbAccessor *dba) {
  FType<Or<Null, String>, Or<Null, String>, Or<Null, String>>("replace", args,
                                                              nargs);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TString replaced(ctx.memory);
  utils::Replace(&replaced, args[0].ValueString(), args[1].ValueString(),
                 args[2].ValueString());
  return TypedValue(std::move(replaced));
}

TypedValue Split(TypedValue *args, int64_t nargs, const EvaluationContext &ctx,
                 database::GraphDbAccessor *dba) {
  FType<Or<Null, String>, Or<Null, String>>("split", args, nargs);
  if (args[0].IsNull() || args[1].IsNull()) {
    return TypedValue(ctx.memory);
  }
  TypedValue::TVector result(ctx.memory);
  utils::Split(&result, args[0].ValueString(), args[1].ValueString());
  return TypedValue(std::move(result));
}

TypedValue Substring(TypedValue *args, int64_t nargs,
                     const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  FType<Or<Null, String>, NonNegativeInteger, Optional<NonNegativeInteger>>(
      "substring", args, nargs);
  if (args[0].IsNull()) return TypedValue(ctx.memory);
  const auto &str = args[0].ValueString();
  auto start = args[1].ValueInt();
  if (nargs == 2) return TypedValue(utils::Substr(str, start), ctx.memory);
  auto len = args[2].ValueInt();
  return TypedValue(utils::Substr(str, start, len), ctx.memory);
}

}  // namespace

std::function<TypedValue(TypedValue *, int64_t, const EvaluationContext &ctx,
                         database::GraphDbAccessor *)>
NameToFunction(const std::string &function_name) {
  // Scalar functions
  if (function_name == "DEGREE") return Degree;
  if (function_name == "INDEGREE") return InDegree;
  if (function_name == "OUTDEGREE") return OutDegree;
  if (function_name == "ENDNODE") return EndNode;
  if (function_name == "HEAD") return Head;
  if (function_name == "ID") return Id;
  if (function_name == "LAST") return Last;
  if (function_name == "PROPERTIES") return Properties;
  if (function_name == "SIZE") return Size;
  if (function_name == "STARTNODE") return StartNode;
  if (function_name == "TIMESTAMP") return Timestamp;
  if (function_name == "TOBOOLEAN") return ToBoolean;
  if (function_name == "TOFLOAT") return ToFloat;
  if (function_name == "TOINTEGER") return ToInteger;
  if (function_name == "TYPE") return Type;

  // List functions
  if (function_name == "KEYS") return Keys;
  if (function_name == "LABELS") return Labels;
  if (function_name == "NODES") return Nodes;
  if (function_name == "RANGE") return Range;
  if (function_name == "RELATIONSHIPS") return Relationships;
  if (function_name == "TAIL") return Tail;
  if (function_name == "UNIFORMSAMPLE") return UniformSample;

  // Mathematical functions - numeric
  if (function_name == "ABS") return Abs;
  if (function_name == "CEIL") return Ceil;
  if (function_name == "FLOOR") return Floor;
  if (function_name == "RAND") return Rand;
  if (function_name == "ROUND") return Round;
  if (function_name == "SIGN") return Sign;

  // Mathematical functions - logarithmic
  if (function_name == "E") return E;
  if (function_name == "EXP") return Exp;
  if (function_name == "LOG") return Log;
  if (function_name == "LOG10") return Log10;
  if (function_name == "SQRT") return Sqrt;

  // Mathematical functions - trigonometric
  if (function_name == "ACOS") return Acos;
  if (function_name == "ASIN") return Asin;
  if (function_name == "ATAN") return Atan;
  if (function_name == "ATAN2") return Atan2;
  if (function_name == "COS") return Cos;
  if (function_name == "PI") return Pi;
  if (function_name == "SIN") return Sin;
  if (function_name == "TAN") return Tan;

  // String functions
  if (function_name == kContains) return Contains;
  if (function_name == kEndsWith) return EndsWith;
  if (function_name == "LEFT") return Left;
  if (function_name == "LTRIM") return LTrim;
  if (function_name == "REPLACE") return Replace;
  if (function_name == "REVERSE") return Reverse;
  if (function_name == "RIGHT") return Right;
  if (function_name == "RTRIM") return RTrim;
  if (function_name == "SPLIT") return Split;
  if (function_name == kStartsWith) return StartsWith;
  if (function_name == "SUBSTRING") return Substring;
  if (function_name == "TOLOWER") return ToLower;
  if (function_name == "TOSTRING") return ToString;
  if (function_name == "TOUPPER") return ToUpper;
  if (function_name == "TRIM") return Trim;

  // Memgraph specific functions
  if (function_name == "ASSERT") return Assert;
  if (function_name == "COUNTER") return Counter;

  return nullptr;
}

}  // namespace query
