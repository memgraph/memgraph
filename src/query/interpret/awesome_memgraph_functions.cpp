#include "query/interpret/awesome_memgraph_functions.hpp"

#include <cctype>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <random>

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
// TODO: Implement timestamp, every time it is called in a query it needs to
// return same time. We need to store query start time somwhere.
// TODO: Implement rest of the list functions.
// TODO: Implement rand
// TODO: Implement degrees, haversin, radians
// TODO: Implement string and spatial functions

TypedValue Coalesce(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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

TypedValue EndNode(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("endNode requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().to();
    default:
      throw QueryRuntimeException("endNode called with incompatible type");
  }
}

TypedValue Head(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("head called with incompatible type");
  }
}

TypedValue Last(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("last called with incompatible type");
  }
}

TypedValue Properties(const std::vector<TypedValue> &args,
                      GraphDbAccessor &db_accessor) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("properties requires one argument");
  }
  auto get_properties = [&](const auto &record_accessor) {
    std::map<std::string, TypedValue> properties;
    for (const auto &property : record_accessor.Properties()) {
      properties[db_accessor.PropertyName(property.first)] = property.second;
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
      throw QueryRuntimeException("properties called with incompatible type");
  }
}

TypedValue Size(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
    default:
      throw QueryRuntimeException("size called with incompatible type");
  }
}

TypedValue StartNode(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("startNode requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return args[0].Value<EdgeAccessor>().from();
    default:
      throw QueryRuntimeException("startNode called with incompatible type");
  }
}

TypedValue Degree(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("degree called with incompatible type");
  }
}

TypedValue ToBoolean(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toBoolean requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Bool:
      return args[0].Value<bool>();
    case TypedValue::Type::String: {
      auto s = utils::ToUpperCase(utils::Trim(args[0].Value<std::string>()));
      if (s == "TRUE") return true;
      if (s == "FALSE") return false;
      // I think this is just stupid and that exception should be thrown, but
      // neo4j does it this way...
      return TypedValue::Null;
    }
    default:
      throw QueryRuntimeException("toBoolean called with incompatible type");
  }
}

TypedValue ToFloat(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("toFloat called with incompatible type");
  }
}

TypedValue ToInteger(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("toInteger requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
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
      throw QueryRuntimeException("toInteger called with incompatible type");
  }
}

TypedValue Type(const std::vector<TypedValue> &args,
                GraphDbAccessor &db_accessor) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("type requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Edge:
      return db_accessor.EdgeTypeName(args[0].Value<EdgeAccessor>().EdgeType());
    default:
      throw QueryRuntimeException("type called with incompatible type");
  }
}

TypedValue Keys(const std::vector<TypedValue> &args,
                GraphDbAccessor &db_accessor) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("keys requires one argument");
  }
  auto get_keys = [&](const auto &record_accessor) {
    std::vector<TypedValue> keys;
    for (const auto &property : record_accessor.Properties()) {
      keys.push_back(db_accessor.PropertyName(property.first));
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
      throw QueryRuntimeException("keys called with incompatible type");
  }
}

TypedValue Labels(const std::vector<TypedValue> &args,
                  GraphDbAccessor &db_accessor) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("labels requires one argument");
  }
  switch (args[0].type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Vertex: {
      std::vector<TypedValue> labels;
      for (const auto &label : args[0].Value<VertexAccessor>().labels()) {
        labels.push_back(db_accessor.LabelName(label));
      }
      return labels;
    }
    default:
      throw QueryRuntimeException("labels called with incompatible type");
  }
}

TypedValue Range(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 2U && args.size() != 3U) {
    throw QueryRuntimeException("range requires two or three arguments");
  }
  bool has_null = false;
  auto check_type = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::Int) {
      throw QueryRuntimeException("range called with incompatible type");
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

TypedValue Tail(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("tail called with incompatible type");
  }
}

TypedValue Abs(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("abs called with incompatible type");
  }
}

#define WRAP_CMATH_FLOAT_FUNCTION(name, lowercased_name)                      \
  TypedValue name(const std::vector<TypedValue> &args, GraphDbAccessor &) {   \
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
                                    " called with incompatible type");        \
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

TypedValue Atan2(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
        throw QueryRuntimeException("atan2 called with incompatible types");
    }
  };
  double y = to_double(args[0]);
  double x = to_double(args[1]);
  return atan2(y, x);
}

TypedValue Sign(const std::vector<TypedValue> &args, GraphDbAccessor &) {
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
      throw QueryRuntimeException("sign called with incompatible type");
  }
}

TypedValue E(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 0U) {
    throw QueryRuntimeException("e shouldn't be called with arguments");
  }
  return M_E;
}

TypedValue Pi(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() != 0U) {
    throw QueryRuntimeException("pi shouldn't be called with arguments");
  }
  return M_PI;
}

TypedValue Rand(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  static thread_local std::mt19937 pseudo_rand_gen_{std::random_device{}()};
  static thread_local std::uniform_real_distribution<> rand_dist_{0, 1};
  if (args.size() != 0U) {
    throw QueryRuntimeException("rand shouldn't be called with arguments");
  }
  return rand_dist_(pseudo_rand_gen_);
}

template <bool (*Predicate)(const std::string &s1, const std::string &s2)>
TypedValue StringMatchOperator(const std::vector<TypedValue> &args,
                               GraphDbAccessor &) {
  if (args.size() != 2U) {
    throw QueryRuntimeException(
        "startsWith shouldn't be called with 2 arguments");
  }
  bool has_null = false;
  auto check_arg = [&](const TypedValue &t) {
    if (t.IsNull()) {
      has_null = true;
    } else if (t.type() != TypedValue::Type::String) {
      throw QueryRuntimeException("startsWith called with incompatible type");
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

TypedValue Assert(const std::vector<TypedValue> &args, GraphDbAccessor &) {
  if (args.size() < 1U || args.size() > 2U) {
    throw QueryRuntimeException("assert takes one or two arguments");
  }
  if (args[0].type() != TypedValue::Type::Bool)
    throw QueryRuntimeException("first assert argument must be bool");
  if (args.size() == 2U && args[1].type() != TypedValue::Type::String)
    throw QueryRuntimeException("second assert argument must be a string");
  if (!args[0].ValueBool()) {
    std::string message("assertion failed");
    if (args.size() == 2U) message += ": " + args[1].ValueString();
    throw QueryRuntimeException(message);
  }
  return args[0];
}

TypedValue Counter(const std::vector<TypedValue> &args, GraphDbAccessor &dba) {
  if (args.size() != 1U) {
    throw QueryRuntimeException("counter takes one argument");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException("first counter argument must be a string");

  return dba.Counter(args[0].ValueString());
}

TypedValue CounterSet(const std::vector<TypedValue> &args, GraphDbAccessor &dba) {
  if (args.size() != 2U) {
    throw QueryRuntimeException("counterSet takes two arguments");
  }
  if (!args[0].IsString())
    throw QueryRuntimeException("first counterSet argument must be a string");
  if (!args[1].IsInt())
    throw QueryRuntimeException("first counterSet argument must be an int");

  dba.CounterSet(args[0].ValueString(), args[1].ValueInt());
  return TypedValue::Null;
}
}  // annonymous namespace

std::function<TypedValue(const std::vector<TypedValue> &, GraphDbAccessor &)>
NameToFunction(const std::string &function_name) {
  if (function_name == "COALESCE") return Coalesce;
  if (function_name == "ENDNODE") return EndNode;
  if (function_name == "HEAD") return Head;
  if (function_name == "LAST") return Last;
  if (function_name == "PROPERTIES") return Properties;
  if (function_name == "SIZE") return Size;
  if (function_name == "STARTNODE") return StartNode;
  if (function_name == "DEGREE") return Degree;
  if (function_name == "TOBOOLEAN") return ToBoolean;
  if (function_name == "TOFLOAT") return ToFloat;
  if (function_name == "TOINTEGER") return ToInteger;
  if (function_name == "TYPE") return Type;
  if (function_name == "KEYS") return Keys;
  if (function_name == "LABELS") return Labels;
  if (function_name == "RANGE") return Range;
  if (function_name == "TAIL") return Tail;
  if (function_name == "ABS") return Abs;
  if (function_name == "CEIL") return Ceil;
  if (function_name == "FLOOR") return Floor;
  if (function_name == "ROUND") return Round;
  if (function_name == "EXP") return Exp;
  if (function_name == "LOG") return Log;
  if (function_name == "LOG10") return Log10;
  if (function_name == "SQRT") return Sqrt;
  if (function_name == "ACOS") return Acos;
  if (function_name == "ASIN") return Asin;
  if (function_name == "ATAN") return Atan;
  if (function_name == "ATAN2") return Atan2;
  if (function_name == "COS") return Cos;
  if (function_name == "SIN") return Sin;
  if (function_name == "TAN") return Tan;
  if (function_name == "SIGN") return Sign;
  if (function_name == "E") return E;
  if (function_name == "PI") return Pi;
  if (function_name == "RAND") return Rand;
  if (function_name == kStartsWith) return StartsWith;
  if (function_name == kEndsWith) return EndsWith;
  if (function_name == kContains) return Contains;
  if (function_name == "ASSERT") return Assert;
  if (function_name == "COUNTER") return Counter;
  if (function_name == "COUNTERSET") return CounterSet;
  return nullptr;
}
}  // namespace query
