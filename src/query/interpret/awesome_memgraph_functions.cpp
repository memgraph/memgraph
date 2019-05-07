#include "query/interpret/awesome_memgraph_functions.hpp"

#include <cctype>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <random>

#include "database/single_node/dump.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "utils/string.hpp"

namespace query {
namespace {

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

TypedValue EndNode(TypedValue *args, int64_t nargs, const EvaluationContext &,
                   database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'endNode' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().to();
    default:
      throw QueryRuntimeException("'endNode' argument must be an edge.");
  }
}

TypedValue Head(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'head' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::List: {
      const auto &list = args[0].Value<std::vector<TypedValue>>();
      if (list.empty()) return TypedValue::Null;
      return list[0];
    }
    default:
      throw QueryRuntimeException("'head' argument must be a list.");
  }
}

TypedValue Last(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'last' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::List: {
      const auto &list = args[0].Value<std::vector<TypedValue>>();
      if (list.empty()) return TypedValue::Null;
      return list.back();
    }
    default:
      throw QueryRuntimeException("'last' argument must be a list.");
  }
}

TypedValue Properties(TypedValue *args, int64_t nargs,
                      const EvaluationContext &,
                      database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'properties' requires exactly one argument.");
  }
  auto get_properties = [&](const auto &record_accessor) {
    std::map<std::string, TypedValue> properties;
    for (const auto &property : record_accessor.Properties()) {
      properties[dba->PropertyName(property.first)] = property.second;
    }
    return properties;
  };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex:
      return get_properties(args[0].Value<VertexAccessor>());
    case TypedValue::Type::Edge:
      return get_properties(args[0].Value<EdgeAccessor>());
    default:
      throw QueryRuntimeException(
          "'properties' argument must be a node or an edge.");
  }
}

TypedValue Size(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'size' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::List:
      return static_cast<int64_t>(
          args[0].Value<std::vector<TypedValue>>().size());
    case TypedValue::Type::String:
      return static_cast<int64_t>(args[0].Value<std::string>().size());
    case TypedValue::Type::Map:
      // neo4j doesn't implement size for map, but I don't see a good reason not
      // to do it.
      return static_cast<int64_t>(
          args[0].Value<std::map<std::string, TypedValue>>().size());
    case TypedValue::Type::Path:
      return static_cast<int64_t>(args[0].ValuePath().edges().size());
    default:
      throw QueryRuntimeException(
          "'size' argument must be a string, a collection or a path.");
  }
}

TypedValue StartNode(TypedValue *args, int64_t nargs, const EvaluationContext &,
                     database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'startNode' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().from();
    default:
      throw QueryRuntimeException("'startNode' argument must be an edge.");
  }
}

TypedValue Degree(TypedValue *args, int64_t nargs, const EvaluationContext &,
                  database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'degree' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      auto &vertex = args[0].Value<VertexAccessor>();
      return static_cast<int64_t>(vertex.out_degree() + vertex.in_degree());
    }
    default:
      throw QueryRuntimeException("'degree' argument must be a node.");
  }
}

TypedValue InDegree(TypedValue *args, int64_t nargs, const EvaluationContext &,
                    database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'inDegree' requires exactly one argument.");
  }

  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      auto &vertex = args[0].Value<VertexAccessor>();
      return static_cast<int64_t>(vertex.in_degree());
    }
    default:
      throw QueryRuntimeException("'inDegree' argument must be a node.");
  }
}

TypedValue OutDegree(TypedValue *args, int64_t nargs, const EvaluationContext &,
                     database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'outDegree' requires exactly one argument.");
  }

  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      auto &vertex = args[0].Value<VertexAccessor>();
      return static_cast<int64_t>(vertex.out_degree());
    }
    default:
      throw QueryRuntimeException("'outDegree' argument must be a node.");
  }
}

TypedValue ToBoolean(TypedValue *args, int64_t nargs, const EvaluationContext &,
                     database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'toBoolean' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Bool:
      return args[0].Value<bool>();
    case TypedValue::Type::Int:
      return args[0].ValueInt() != 0L;
    case TypedValue::Type::String: {
      auto s = utils::ToUpperCase(utils::Trim(args[0].Value<std::string>()));
      if (s == "TRUE") return true;
      if (s == "FALSE") return false;
      // I think this is just stupid and that exception should be thrown, but
      // neo4j does it this way...
      return TypedValue::Null;
    }
    default:
      throw QueryRuntimeException(
          "'toBoolean' argument must be an integer, a string or a boolean.");
  }
}

TypedValue ToFloat(TypedValue *args, int64_t nargs, const EvaluationContext &,
                   database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'toFloat' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return static_cast<double>(args[0].Value<int64_t>());
    case TypedValue::Type::Double:
      return args[0];
    case TypedValue::Type::String:
      try {
        return utils::ParseDouble(utils::Trim(args[0].Value<std::string>()));
      } catch (const utils::BasicException &) {
        return TypedValue::Null;
      }
    default:
      throw QueryRuntimeException(
          "'toFloat' argument must be a string or a number.");
  }
}

TypedValue ToInteger(TypedValue *args, int64_t nargs, const EvaluationContext &,
                     database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'toInteger' requires exactly one argument'");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Bool:
      return args[0].ValueBool() ? 1L : 0L;
    case TypedValue::Type::Int:
      return args[0];
    case TypedValue::Type::Double:
      return static_cast<int64_t>(args[0].Value<double>());
    case TypedValue::Type::String:
      try {
        // Yup, this is correct. String is valid if it has floating point
        // number, then it is parsed and converted to int.
        return static_cast<int64_t>(
            utils::ParseDouble(utils::Trim(args[0].Value<std::string>())));
      } catch (const utils::BasicException &) {
        return TypedValue::Null;
      }
    default:
      throw QueryRuntimeException(
          "'toInteger' argument must be a string, a boolean or a number.");
  }
}

TypedValue Type(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'type' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return dba->EdgeTypeName(args[0].Value<EdgeAccessor>().EdgeType());
    default:
      throw QueryRuntimeException("'type' argument must be an edge.");
  }
}

TypedValue Keys(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'keys' requires exactly one argument.");
  }
  auto get_keys = [&](const auto &record_accessor) {
    std::vector<TypedValue> keys;
    for (const auto &property : record_accessor.Properties()) {
      keys.push_back(dba->PropertyName(property.first));
    }
    return keys;
  };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex:
      return get_keys(args[0].Value<VertexAccessor>());
    case TypedValue::Type::Edge:
      return get_keys(args[0].Value<EdgeAccessor>());
    default:
      throw QueryRuntimeException("'keys' argument must be a node or an edge.");
  }
}

TypedValue Labels(TypedValue *args, int64_t nargs, const EvaluationContext &,
                  database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'labels' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      std::vector<TypedValue> labels;
      for (const auto &label : args[0].Value<VertexAccessor>().labels()) {
        labels.push_back(dba->LabelName(label));
      }
      return labels;
    }
    default:
      throw QueryRuntimeException("'labels' argument must be a node.");
  }
}

TypedValue Nodes(TypedValue *args, int64_t nargs, const EvaluationContext &,
                 database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'nodes' requires exactly one argument.");
  }
  if (args[0].IsNull()) return TypedValue::Null;
  if (!args[0].IsPath()) {
    throw QueryRuntimeException("'nodes' argument should be a path.");
  }
  auto &vertices = args[0].ValuePath().vertices();
  return std::vector<TypedValue>(vertices.begin(), vertices.end());
}

TypedValue Relationships(TypedValue *args, int64_t nargs,
                         const EvaluationContext &,
                         database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException(
        "'relationships' requires exactly one argument.");
  }
  if (args[0].IsNull()) return TypedValue::Null;
  if (!args[0].IsPath()) {
    throw QueryRuntimeException("'relationships' argument must be a path.");
  }
  auto &edges = args[0].ValuePath().edges();
  return std::vector<TypedValue>(edges.begin(), edges.end());
}

TypedValue Range(TypedValue *args, int64_t nargs, const EvaluationContext &,
                 database::GraphDbAccessor *) {
  if (nargs != 2 && nargs != 3) {
    throw QueryRuntimeException("'range' requires two or three arguments.");
  }
  bool has_null = false;
  auto check_type = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::Int) {
      throw QueryRuntimeException("arguments of 'range' must be integers.");
    }
  };
  for (int64_t i = 0; i < nargs; ++i) check_type(args[i]);
  if (has_null) return TypedValue::Null;
  auto lbound = args[0].Value<int64_t>();
  auto rbound = args[1].Value<int64_t>();
  int64_t step = nargs == 3 ? args[2].Value<int64_t>() : 1;
  if (step == 0) {
    throw QueryRuntimeException("step argument of 'range' can't be zero.");
  }
  std::vector<TypedValue> list;
  if (lbound <= rbound && step > 0) {
    for (auto i = lbound; i <= rbound; i += step) {
      list.push_back(i);
    }
  } else if (lbound >= rbound && step < 0) {
    for (auto i = lbound; i >= rbound; i += step) {
      list.push_back(i);
    }
  }
  return list;
}

TypedValue Tail(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'tail' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::List: {
      auto list = args[0].Value<std::vector<TypedValue>>();
      if (list.empty()) return list;
      list.erase(list.begin());
      return list;
    }
    default:
      throw QueryRuntimeException("'tail' argument must be a list.");
  }
}

TypedValue UniformSample(TypedValue *args, int64_t nargs,
                         const EvaluationContext &,
                         database::GraphDbAccessor *) {
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  if (nargs != 2) {
    throw QueryRuntimeException(
        "'uniformSample' requires exactly two arguments.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      if (args[1].IsNull() || (args[1].IsInt() && args[1].ValueInt() >= 0)) {
        return TypedValue::Null;
      }
      throw QueryRuntimeException(
          "Second argument of 'uniformSample' must be a non-negative integer.");
    case TypedValue::Type::List:
      if (args[1].IsInt() && args[1].ValueInt() >= 0) {
        auto &population = args[0].Value<std::vector<TypedValue>>();
        auto population_size = population.size();
        if (population_size == 0) return TypedValue::Null;
        auto desired_length = args[1].ValueInt();
        std::uniform_int_distribution<uint64_t> rand_dist{0,
                                                          population_size - 1};
        std::vector<TypedValue> sampled;
        sampled.reserve(desired_length);
        for (int i = 0; i < desired_length; ++i) {
          sampled.push_back(population[rand_dist(pseudo_rand_gen_)]);
        }
        return sampled;
      }
      throw QueryRuntimeException(
          "Second argument of 'uniformSample' must be a non-negative integer.");
    default:
      throw QueryRuntimeException(
          "First argument of 'uniformSample' must be a list.");
  }
}

TypedValue Abs(TypedValue *args, int64_t nargs, const EvaluationContext &,
               database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'abs' requires exactly one argument.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return static_cast<int64_t>(
          std::abs(static_cast<long long>(args[0].Value<int64_t>())));
    case TypedValue::Type::Double:
      return std::abs(args[0].Value<double>());
    default:
      throw QueryRuntimeException("'abs' argument should be a number.");
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                      \
  TypedValue name(TypedValue *args, int64_t nargs, const EvaluationContext &, \
                  database::GraphDbAccessor *) {                              \
    if (nargs != 1) {                                                         \
      throw QueryRuntimeException("'" #lowercased_name                        \
                                  "' requires exactly one argument.");        \
    }                                                                         \
    switch (args[0].type()) {                                                 \
      case TypedValue::Type::Null:                                            \
        return TypedValue::Null;                                              \
      case TypedValue::Type::Int:                                             \
        return lowercased_name(args[0].Value<int64_t>());                     \
      case TypedValue::Type::Double:                                          \
        return lowercased_name(args[0].Value<double>());                      \
      default:                                                                \
        throw QueryRuntimeException(#lowercased_name                          \
                                    " argument must be a number.");           \
    }                                                                         \
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

TypedValue Atan2(TypedValue *args, int64_t nargs, const EvaluationContext &,
                 database::GraphDbAccessor *) {
  if (nargs != 2) {
    throw QueryRuntimeException("'atan2' requires two arguments.");
  }
  if (args[0].type() == TypedValue::Type::Null) return TypedValue::Null;
  if (args[1].type() == TypedValue::Type::Null) return TypedValue::Null;
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
  return atan2(y, x);
}

TypedValue Sign(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'sign' requires exactly one argument.");
  }
  auto sign = [](auto x) { return (0 < x) - (x < 0); };
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return sign(args[0].Value<int64_t>());
    case TypedValue::Type::Double:
      return sign(args[0].Value<double>());
    default:
      throw QueryRuntimeException("'sign' argument must be a number.");
  }
}

TypedValue E(TypedValue *, int64_t nargs, const EvaluationContext &,
             database::GraphDbAccessor *) {
  if (nargs != 0) {
    throw QueryRuntimeException("'e' requires no arguments.");
  }
  return M_E;
}

TypedValue Pi(TypedValue *, int64_t nargs, const EvaluationContext &,
              database::GraphDbAccessor *) {
  if (nargs != 0) {
    throw QueryRuntimeException("'pi' requires no arguments.");
  }
  return M_PI;
}

TypedValue Rand(TypedValue *, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *) {
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  if (nargs != 0) {
    throw QueryRuntimeException("'rand' requires no arguments.");
  }
  return rand_dist_(pseudo_rand_gen_);
}

template <bool (*Predicate)(const std::string &s1, const std::string &s2)>
TypedValue StringMatchOperator(TypedValue *args, int64_t nargs,
                               const EvaluationContext &,
                               database::GraphDbAccessor *) {
  if (nargs != 2) {
    throw QueryRuntimeException(
        "'startsWith' and 'endsWith' require two arguments.");
  }
  bool has_null = false;
  auto check_arg = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::String) {
      throw QueryRuntimeException(
          "Arguments of 'startsWith' and 'endsWith' must be strings.");
    }
  };
  check_arg(args[0]);
  check_arg(args[1]);
  if (has_null) return TypedValue::Null;
  const auto &s1 = args[0].Value<std::string>();
  const auto &s2 = args[1].Value<std::string>();
  return Predicate(s1, s2);
}

// Check if s1 starts with s2.
bool StartsWithPredicate(const std::string &s1, const std::string &s2) {
  if (s1.size() < s2.size()) return false;
  return std::equal(s2.begin(), s2.end(), s1.begin());
}
auto StartsWith = StringMatchOperator<StartsWithPredicate>;

// Check if s1 ends with s2.
bool EndsWithPredicate(const std::string &s1, const std::string &s2) {
  if (s1.size() < s2.size()) return false;
  return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
}
auto EndsWith = StringMatchOperator<EndsWithPredicate>;

// Check if s1 contains s2.
bool ContainsPredicate(const std::string &s1, const std::string &s2) {
  if (s1.size() < s2.size()) return false;
  return s1.find(s2) != std::string::npos;
}
auto Contains = StringMatchOperator<ContainsPredicate>;

TypedValue Assert(TypedValue *args, int64_t nargs, const EvaluationContext &,
                  database::GraphDbAccessor *) {
  if (nargs < 1 || nargs > 2) {
    throw QueryRuntimeException("'assert' requires one or two arguments");
  }
  if (args[0].type() != TypedValue::Type::Bool)
    throw QueryRuntimeException(
        "First argument of 'assert' must be a boolean.");
  if (nargs == 2 && args[1].type() != TypedValue::Type::String)
    throw QueryRuntimeException(
        "Second argument of 'assert' must be a string.");
  if (!args[0].ValueBool()) {
    std::string message("Assertion failed");
    if (nargs == 2) message += ": " + args[1].ValueString();
    message += ".";
    throw QueryRuntimeException(message);
  }
  return args[0];
}

TypedValue Counter(TypedValue *args, int64_t nargs, const EvaluationContext &,
                   database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'counter' requires exactly one argument.");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException("'counter' argument must be a string.");

  return dba->Counter(args[0].ValueString());
}

TypedValue CounterSet(TypedValue *args, int64_t nargs,
                      const EvaluationContext &,
                      database::GraphDbAccessor *dba) {
  if (nargs != 2) {
    throw QueryRuntimeException("'counterSet' requires two arguments.");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException(
        "First argument of 'counterSet' must be a string.");
  if (!args[1].IsInt())
    throw QueryRuntimeException(
        "Second argument of 'counterSet' must be an integer.");
  dba->CounterSet(args[0].ValueString(), args[1].ValueInt());
  return TypedValue::Null;
}

#ifdef MG_DISTRIBUTED
TypedValue WorkerId(TypedValue *args, int64_t nargs, const EvaluationContext &,
                    database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'workerId' requires exactly one argument.");
  }
  auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Vertex:
      return arg.ValueVertex().GlobalAddress().worker_id();
    case TypedValue::Type::Edge:
      return arg.ValueEdge().GlobalAddress().worker_id();
    default:
      throw QueryRuntimeException(
          "'workerId' argument must be a node or an edge.");
  }
}
#endif

TypedValue Id(TypedValue *args, int64_t nargs, const EvaluationContext &,
              database::GraphDbAccessor *dba) {
  if (nargs != 1) {
    throw QueryRuntimeException("'id' requires exactly one argument.");
  }
  auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Vertex: {
      return TypedValue(arg.ValueVertex().CypherId());
    }
    case TypedValue::Type::Edge: {
      return TypedValue(arg.ValueEdge().CypherId());
    }
    default:
      throw QueryRuntimeException("'id' argument must be a node or an edge.");
  }
}

TypedValue ToString(TypedValue *args, int64_t nargs, const EvaluationContext &,
                    database::GraphDbAccessor *) {
  if (nargs != 1) {
    throw QueryRuntimeException("'toString' requires exactly one argument.");
  }
  auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::String:
      return arg;
    case TypedValue::Type::Int:
      return std::to_string(arg.ValueInt());
    case TypedValue::Type::Double:
      return std::to_string(arg.ValueDouble());
    case TypedValue::Type::Bool:
      return arg.ValueBool() ? "true" : "false";
    default:
      throw QueryRuntimeException(
          "'toString' argument must be a number, a string or a boolean.");
  }
}

TypedValue Timestamp(TypedValue *, int64_t nargs, const EvaluationContext &ctx,
                     database::GraphDbAccessor *) {
  if (nargs != 0) {
    throw QueryRuntimeException("'timestamp' requires no arguments.");
  }
  return ctx.timestamp;
}

TypedValue Left(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *dba) {
  if (nargs != 2) {
    throw QueryRuntimeException("'left' requires two arguments.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      if (args[1].IsNull() || (args[1].IsInt() && args[1].ValueInt() >= 0)) {
        return TypedValue::Null;
      }
      throw QueryRuntimeException(
          "Second argument of 'left' must be a non-negative integer.");
    case TypedValue::Type::String:
      if (args[1].IsInt() && args[1].ValueInt() >= 0) {
        return args[0].ValueString().substr(0, args[1].ValueInt());
      }
      throw QueryRuntimeException(
          "Second argument of 'left' must be a non-negative integer.");
    default:
      throw QueryRuntimeException("First argument of 'left' must be a string.");
  }
}

TypedValue Right(TypedValue *args, int64_t nargs, const EvaluationContext &,
                 database::GraphDbAccessor *dba) {
  if (nargs != 2) {
    throw QueryRuntimeException("'right' requires two arguments.");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      if (args[1].IsNull() || (args[1].IsInt() && args[1].ValueInt() >= 0)) {
        return TypedValue::Null;
      }
      throw QueryRuntimeException(
          "Second argument of 'right' must be a non-negative integer.");
    case TypedValue::Type::String: {
      const auto &str = args[0].ValueString();
      if (args[1].IsInt() && args[1].ValueInt() >= 0) {
        int len = args[1].ValueInt();
        return len <= str.size() ? str.substr(str.size() - len, len) : str;
      }
      throw QueryRuntimeException(
          "Second argument of 'right' must be a non-negative integer.");
    }
    default:
      throw QueryRuntimeException(
          "First argument of 'right' must be a string.");
  }
}

#define WRAP_STRING_FUNCTION(name, lowercased_name, function)                 \
  TypedValue name(TypedValue *args, int64_t nargs, const EvaluationContext &, \
                  database::GraphDbAccessor *) {                              \
    if (nargs != 1) {                                                         \
      throw QueryRuntimeException("'" #lowercased_name                        \
                                  "' requires exactly one argument.");        \
    }                                                                         \
    switch (args[0].type()) {                                                 \
      case TypedValue::Type::Null:                                            \
        return TypedValue::Null;                                              \
      case TypedValue::Type::String:                                          \
        return function(args[0].ValueString());                               \
      default:                                                                \
        throw QueryRuntimeException("'" #lowercased_name                      \
                                    "' argument should be a string.");        \
    }                                                                         \
  }

WRAP_STRING_FUNCTION(LTrim, lTrim, utils::LTrim);
WRAP_STRING_FUNCTION(RTrim, rTrim, utils::RTrim);
WRAP_STRING_FUNCTION(Trim, trim, utils::Trim);
WRAP_STRING_FUNCTION(Reverse, reverse, utils::Reversed);
WRAP_STRING_FUNCTION(ToLower, toLower, utils::ToLowerCase);
WRAP_STRING_FUNCTION(ToUpper, toUpper, utils::ToUpperCase);

TypedValue Replace(TypedValue *args, int64_t nargs, const EvaluationContext &,
                   database::GraphDbAccessor *dba) {
  if (nargs != 3) {
    throw QueryRuntimeException("'replace' requires three arguments.");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException(
        "First argument of 'replace' should be a string.");
  }
  if (!args[1].IsNull() && !args[1].IsString()) {
    throw QueryRuntimeException(
        "Second argument of 'replace' should be a string.");
  }
  if (!args[2].IsNull() && !args[2].IsString()) {
    throw QueryRuntimeException(
        "Third argument of 'replace' should be a string.");
  }
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValue::Null;
  }
  return utils::Replace(args[0].ValueString(), args[1].ValueString(),
                        args[2].ValueString());
}

TypedValue Split(TypedValue *args, int64_t nargs, const EvaluationContext &,
                 database::GraphDbAccessor *dba) {
  if (nargs != 2) {
    throw QueryRuntimeException("'split' requires two arguments.");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException(
        "First argument of 'split' should be a string.");
  }
  if (!args[1].IsNull() && !args[1].IsString()) {
    throw QueryRuntimeException(
        "Second argument of 'split' should be a string.");
  }
  if (args[0].IsNull() || args[1].IsNull()) {
    return TypedValue::Null;
  }
  std::vector<TypedValue> result;
  for (const auto &str :
       utils::Split(args[0].ValueString(), args[1].ValueString())) {
    result.emplace_back(str);
  }
  return result;
}

TypedValue Substring(TypedValue *args, int64_t nargs, const EvaluationContext &,
                     database::GraphDbAccessor *) {
  if (nargs != 2 && nargs != 3) {
    throw QueryRuntimeException("'substring' requires two or three arguments.");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException(
        "First argument of 'substring' should be a string.");
  }
  if (!args[1].IsInt() || args[1].ValueInt() < 0) {
    throw QueryRuntimeException(
        "Second argument of 'substring' should be a non-negative integer.");
  }
  if (nargs == 3 && (!args[2].IsInt() || args[2].ValueInt() < 0)) {
    throw QueryRuntimeException(
        "Third argument of 'substring' should be a non-negative integer.");
  }
  if (args[0].IsNull()) {
    return TypedValue::Null;
  }
  const auto &str = args[0].ValueString();
  int start = args[1].ValueInt();
  if (nargs == 2) {
    return start < str.size() ? str.substr(start) : "";
  }
  int len = args[2].ValueInt();
  return start < str.size() ? str.substr(start, len) : "";
}

#if MG_SINGLE_NODE
TypedValue Dump(TypedValue *args, int64_t nargs, const EvaluationContext &,
                database::GraphDbAccessor *dba) {
  if (nargs != 0) {
    throw QueryRuntimeException("'dump' does not expect any arguments.");
  }
  std::ostringstream oss;
  database::DumpToCypher(&oss, dba);
  return oss.str();
}
#endif  // MG_SINGLE_NODE

}  // namespace

std::function<TypedValue(TypedValue *, int64_t, const EvaluationContext &,
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
  if (function_name == "COUNTERSET") return CounterSet;
#ifdef MG_SINGLE_NODE
  if (function_name == "DUMP") return Dump;
#endif
#ifdef MG_DISTRIBUTED
  if (function_name == "WORKERID") return WorkerId;
#endif

  return nullptr;
}

}  // namespace query
