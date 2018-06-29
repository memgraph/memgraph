#include "query/interpret/awesome_memgraph_functions.hpp"

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

TypedValue Coalesce(const std::vector<TypedValue> &args, Context *) {
  if (args.size() == 0U) {
    throw QueryRuntimeException("coalesce requires at least one argument");
  }
  for (auto &arg : args) {
    if (arg.type() != TypedValue::Type::Null) {
      return arg;
    }
  }
  return TypedValue::Null;
}

TypedValue EndNode(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("endNode requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().to();
    default:
      throw QueryRuntimeException("endNode argument should be an edge");
  }
}

TypedValue Head(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("head requires one argument");
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
      throw QueryRuntimeException("head argument should be a list");
  }
}

TypedValue Last(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("last requires one argument");
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
      throw QueryRuntimeException("last argument should be a list");
  }
}

TypedValue Properties(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("properties requires one argument");
  }
  auto get_properties = [&](const auto &record_accessor) {
    std::map<std::string, TypedValue> properties;
    for (const auto &property : record_accessor.Properties()) {
      properties[ctx->db_accessor_.PropertyName(property.first)] =
          property.second;
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
          "properties argument should be a vertex or an edge");
  }
}

TypedValue Size(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("size requires one argument");
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
          "size argument should be a string, a collection or a path");
  }
}

TypedValue StartNode(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("startNode requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().from();
    default:
      throw QueryRuntimeException("startNode argument should be an edge");
  }
}

TypedValue Degree(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("degree requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      auto &vertex = args[0].Value<VertexAccessor>();
      return static_cast<int64_t>(vertex.out_degree() + vertex.in_degree());
    }
    default:
      throw QueryRuntimeException("degree argument should be a vertex");
  }
}

TypedValue ToBoolean(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toBoolean requires one argument");
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
          "toBoolean argument should be an integer, a string or a boolean");
  }
}

TypedValue ToFloat(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toFloat requires one argument");
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
          "toFloat argument should be a string or a number");
  }
}

TypedValue ToInteger(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toInteger requires one argument");
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
          "toInteger argument should be a string, a boolean or a number");
  }
}

TypedValue Type(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("type requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return ctx->db_accessor_.EdgeTypeName(
          args[0].Value<EdgeAccessor>().EdgeType());
    default:
      throw QueryRuntimeException("type argument should be an edge");
  }
}

TypedValue Keys(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("keys requires one argument");
  }
  auto get_keys = [&](const auto &record_accessor) {
    std::vector<TypedValue> keys;
    for (const auto &property : record_accessor.Properties()) {
      keys.push_back(ctx->db_accessor_.PropertyName(property.first));
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
      throw QueryRuntimeException(
          "keys argument should be a vertex or an edge");
  }
}

TypedValue Labels(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("labels requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      std::vector<TypedValue> labels;
      for (const auto &label : args[0].Value<VertexAccessor>().labels()) {
        labels.push_back(ctx->db_accessor_.LabelName(label));
      }
      return labels;
    }
    default:
      throw QueryRuntimeException("labels argument should be a vertex");
  }
}

TypedValue Nodes(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("nodes requires one argument");
  }
  if (args[0].IsNull()) return TypedValue::Null;
  if (!args[0].IsPath()) {
    throw QueryRuntimeException("nodes argument should be a path");
  }
  auto &vertices = args[0].ValuePath().vertices();
  return std::vector<TypedValue>(vertices.begin(), vertices.end());
}

TypedValue Relationships(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("relationships requires one argument");
  }
  if (args[0].IsNull()) return TypedValue::Null;
  if (!args[0].IsPath()) {
    throw QueryRuntimeException("relationships argument should be a path");
  }
  auto &edges = args[0].ValuePath().edges();
  return std::vector<TypedValue>(edges.begin(), edges.end());
}

TypedValue Range(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 2U && args.size() != 3U) {
    throw QueryRuntimeException("range requires two or three arguments");
  }
  bool has_null = false;
  auto check_type = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::Int) {
      throw QueryRuntimeException("arguments of range should be integers");
    }
  };
  std::for_each(args.begin(), args.end(), check_type);
  if (has_null) return TypedValue::Null;
  auto lbound = args[0].Value<int64_t>();
  auto rbound = args[1].Value<int64_t>();
  int64_t step = args.size() == 3U ? args[2].Value<int64_t>() : 1;
  if (step == 0) {
    throw QueryRuntimeException("step argument in range can't be zero");
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

TypedValue Tail(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("tail requires one argument");
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
      throw QueryRuntimeException("tail argument should be a list");
  }
}

TypedValue Abs(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("abs requires one argument");
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
      throw QueryRuntimeException("abs argument should be a number");
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                      \
  TypedValue name(const std::vector<TypedValue> &args, Context *) {           \
    if (args.size() != 1U) {                                                  \
      throw QueryRuntimeException(#lowercased_name " requires one argument"); \
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
                                    " argument should be a number");          \
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

TypedValue Atan2(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 2U) {
    throw QueryRuntimeException("atan2 requires two arguments");
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
        throw QueryRuntimeException("arguments of atan2 should be numbers");
    }
  };
  double y = to_double(args[0]);
  double x = to_double(args[1]);
  return atan2(y, x);
}

TypedValue Sign(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("sign requires one argument");
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
      throw QueryRuntimeException("sign argument should be a number");
  }
}

TypedValue E(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 0U) {
    throw QueryRuntimeException("e requires no arguments");
  }
  return M_E;
}

TypedValue Pi(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 0U) {
    throw QueryRuntimeException("pi requires no arguments");
  }
  return M_PI;
}

TypedValue Rand(const std::vector<TypedValue> &args, Context *) {
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  if (args.size() != 0U) {
    throw QueryRuntimeException("rand requires no arguments");
  }
  return rand_dist_(pseudo_rand_gen_);
}

template <bool (*Predicate)(const std::string &s1, const std::string &s2)>
TypedValue StringMatchOperator(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 2U) {
    throw QueryRuntimeException(
        "startsWith and endsWith require two arguments");
  }
  bool has_null = false;
  auto check_arg = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::String) {
      throw QueryRuntimeException(
          "arguments of startsWith and endsWith should be strings");
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

TypedValue Assert(const std::vector<TypedValue> &args, Context *) {
  if (args.size() < 1U || args.size() > 2U) {
    throw QueryRuntimeException("assert requires one or two arguments");
  }
  if (args[0].type() != TypedValue::Type::Bool)
    throw QueryRuntimeException("first argument of assert must be a boolean");
  if (args.size() == 2U && args[1].type() != TypedValue::Type::String)
    throw QueryRuntimeException("second argument of assert must be a string");
  if (!args[0].ValueBool()) {
    std::string message("assertion failed");
    if (args.size() == 2U) message += ": " + args[1].ValueString();
    throw QueryRuntimeException(message);
  }
  return args[0];
}

TypedValue Counter(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("counter requires one argument");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException("counter argument must be a string");

  return ctx->db_accessor_.Counter(args[0].ValueString());
}

TypedValue CounterSet(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 2U) {
    throw QueryRuntimeException("counterSet requires two arguments");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException(
        "first argument of counterSet must be a string");
  if (!args[1].IsInt())
    throw QueryRuntimeException(
        "second argument of counterSet must be an integer");
  ctx->db_accessor_.CounterSet(args[0].ValueString(), args[1].ValueInt());
  return TypedValue::Null;
}

TypedValue IndexInfo(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 0U)
    throw QueryRuntimeException("indexInfo requires no arguments");

  auto info = ctx->db_accessor_.IndexInfo();
  return std::vector<TypedValue>(info.begin(), info.end());
}

TypedValue WorkerId(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("workerId requires one argument");
  }
  auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Vertex:
      return arg.ValueVertex().GlobalAddress().worker_id();
    case TypedValue::Type::Edge:
      return arg.ValueEdge().GlobalAddress().worker_id();
    default:
      throw QueryRuntimeException(
          "workerId argument must be a vertex or an edge");
  }
}

TypedValue Id(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("id requires one argument");
  }
  auto &arg = args[0];
  switch (arg.type()) {
    case TypedValue::Type::Vertex: {
      auto id = arg.ValueVertex().PropsAt(
          ctx->db_accessor_.Property(PropertyValueStore::IdPropertyName));
      if (id.IsNull()) {
        throw QueryRuntimeException(
            "IDs are not set on vertices, --generate-vertex-ids flag must be "
            "set on startup to automatically generate them");
      }
      return id.Value<int64_t>();
    }
    case TypedValue::Type::Edge: {
      auto id = arg.ValueEdge().PropsAt(
          ctx->db_accessor_.Property(PropertyValueStore::IdPropertyName));
      if (id.IsNull()) {
        throw QueryRuntimeException(
            "IDs are not set on edges, --generate-edge-ids flag must be set on "
            "startup to automatically generate them");
      }
      return id.Value<int64_t>();
    }
    default:
      throw QueryRuntimeException("id argument must be a vertex or an edge");
  }
}

TypedValue ToString(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toString requires one argument");
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
          "toString argument must be a number, a string or a boolean");
  }
}

TypedValue Timestamp(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 0) {
    throw QueryRuntimeException("timestamp requires no arguments");
  }
  return ctx->timestamp_;
}

TypedValue Left(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 2) {
    throw QueryRuntimeException("left requires two arguments");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      if (args[1].IsNull() || (args[1].IsInt() && args[1].ValueInt() >= 0)) {
        return TypedValue::Null;
      }
      throw QueryRuntimeException(
          "second argument of left must be a non-negative integer");
    case TypedValue::Type::String:
      if (args[1].IsInt() && args[1].ValueInt() >= 0) {
        return args[0].ValueString().substr(0, args[1].ValueInt());
      }
      throw QueryRuntimeException(
          "second argument of left must be a non-negative integer");
    default:
      throw QueryRuntimeException("first argument of left must be a string");
  }
}

TypedValue Right(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 2) {
    throw QueryRuntimeException("right requires two arguments");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      if (args[1].IsNull() || (args[1].IsInt() && args[1].ValueInt() >= 0)) {
        return TypedValue::Null;
      }
      throw QueryRuntimeException(
          "second argument of right must be a non-negative integer");
    case TypedValue::Type::String: {
      const auto &str = args[0].ValueString();
      if (args[1].IsInt() && args[1].ValueInt() >= 0) {
        int len = args[1].ValueInt();
        return len <= str.size() ? str.substr(str.size() - len, len) : str;
      }
      throw QueryRuntimeException(
          "second argument of right must be a non-negative integer");
    }
    default:
      throw QueryRuntimeException("first argument of right must be a string");
  }
}

#define WRAP_STRING_FUNCTION(name, lowercased_name, function)                 \
  TypedValue name(const std::vector<TypedValue> &args, Context *) {           \
    if (args.size() != 1U) {                                                  \
      throw QueryRuntimeException(#lowercased_name " requires one argument"); \
    }                                                                         \
    switch (args[0].type()) {                                                 \
      case TypedValue::Type::Null:                                            \
        return TypedValue::Null;                                              \
      case TypedValue::Type::String:                                          \
        return function(args[0].ValueString());                               \
      default:                                                                \
        throw QueryRuntimeException(#lowercased_name                          \
                                    " argument should be a string");          \
    }                                                                         \
  }

WRAP_STRING_FUNCTION(LTrim, lTrim, utils::LTrim);
WRAP_STRING_FUNCTION(RTrim, rTrim, utils::RTrim);
WRAP_STRING_FUNCTION(Trim, trim, utils::Trim);
WRAP_STRING_FUNCTION(Reverse, reverse, utils::Reversed);
WRAP_STRING_FUNCTION(ToLower, toLower, utils::ToLowerCase);
WRAP_STRING_FUNCTION(ToUpper, toUpper, utils::ToUpperCase);

TypedValue Replace(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 3U) {
    throw QueryRuntimeException("replace requires three arguments");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException("first argument of replace should be a string");
  }
  if (!args[1].IsNull() && !args[1].IsString()) {
    throw QueryRuntimeException(
        "second argument of replace should be a string");
  }
  if (!args[2].IsNull() && !args[2].IsString()) {
    throw QueryRuntimeException("third argument of replace should be a string");
  }
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return TypedValue::Null;
  }
  return utils::Replace(args[0].ValueString(), args[1].ValueString(),
                        args[2].ValueString());
}

TypedValue Split(const std::vector<TypedValue> &args, Context *ctx) {
  if (args.size() != 2U) {
    throw QueryRuntimeException("split requires two arguments");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException("first argument of split should be a string");
  }
  if (!args[1].IsNull() && !args[1].IsString()) {
    throw QueryRuntimeException("second argument of split should be a string");
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

TypedValue Substring(const std::vector<TypedValue> &args, Context *) {
  if (args.size() != 2U && args.size() != 3U) {
    throw QueryRuntimeException("substring requires two or three arguments");
  }
  if (!args[0].IsNull() && !args[0].IsString()) {
    throw QueryRuntimeException(
        "first argument of substring should be a string");
  }
  if (!args[1].IsInt() || args[1].ValueInt() < 0) {
    throw QueryRuntimeException(
        "second argument of substring should be a non-negative integer");
  }
  if (args.size() == 3U && (!args[2].IsInt() || args[2].ValueInt() < 0)) {
    throw QueryRuntimeException(
        "third argument of substring should be a non-negative integer");
  }
  if (args[0].IsNull()) {
    return TypedValue::Null;
  }
  const auto &str = args[0].ValueString();
  int start = args[1].ValueInt();
  if (args.size() == 2U) {
    return start < str.size() ? str.substr(start) : "";
  }
  int len = args[2].ValueInt();
  return start < str.size() ? str.substr(start, len) : "";
}

}  // namespace

std::function<TypedValue(const std::vector<TypedValue> &, Context *)>
NameToFunction(const std::string &function_name) {
  // Scalar functions
  if (function_name == "COALESCE") return Coalesce;
  if (function_name == "DEGREE") return Degree;
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
  if (function_name == "INDEXINFO") return IndexInfo;
  if (function_name == "WORKERID") return WorkerId;

  return nullptr;
}
}  // namespace query
