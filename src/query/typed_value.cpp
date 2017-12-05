#include "query/typed_value.hpp"

#include <fmt/format.h>
#include <cmath>
#include <iostream>
#include <memory>

#include "glog/logging.h"

#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/hashing/fnv.hpp"

namespace query {

TypedValue::TypedValue(const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      type_ = Type::Null;
      return;
    case PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = value.Value<bool>();
      return;
    case PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = value.Value<int64_t>();
      return;
    case PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = value.Value<double>();
      return;
    case PropertyValue::Type::String:
      type_ = Type::String;
      new (&string_v) std::string(value.Value<std::string>());
      return;
    case PropertyValue::Type::List: {
      type_ = Type::List;
      auto vec = value.Value<std::vector<PropertyValue>>();
      new (&list_v) std::vector<TypedValue>(vec.begin(), vec.end());
      return;
    }
    case PropertyValue::Type::Map: {
      type_ = Type::Map;
      auto map = value.Value<std::map<std::string, PropertyValue>>();
      new (&map_v) std::map<std::string, TypedValue>(map.begin(), map.end());
      return;
    }
  }
  LOG(FATAL) << "Unsupported type";
}

TypedValue::TypedValue(const TypedValue &other) : type_(other.type_) {
  switch (other.type_) {
    case TypedValue::Type::Null:
      return;
    case TypedValue::Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case TypedValue::Type::String:
      new (&string_v) std::string(other.string_v);
      return;
    case Type::List:
      new (&list_v) std::vector<TypedValue>(other.list_v);
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, TypedValue>(other.map_v);
      return;
    case Type::Vertex:
      new (&vertex_v) VertexAccessor(other.vertex_v);
      return;
    case Type::Edge:
      new (&edge_v) EdgeAccessor(other.edge_v);
      return;
    case Type::Path:
      new (&path_v) Path(other.path_v);
      return;
  }
  LOG(FATAL) << "Unsupported TypedValue::Type";
}

TypedValue::operator PropertyValue() const {
  switch (type_) {
    case TypedValue::Type::Null:
      return PropertyValue::Null;
    case TypedValue::Type::Bool:
      return PropertyValue(bool_v);
    case TypedValue::Type::Int:
      return PropertyValue(int_v);
    case TypedValue::Type::Double:
      return PropertyValue(double_v);
    case TypedValue::Type::String:
      return PropertyValue(string_v);
    case TypedValue::Type::List:
      return PropertyValue(
          std::vector<PropertyValue>(list_v.begin(), list_v.end()));
    case TypedValue::Type::Map:
      return PropertyValue(
          std::map<std::string, PropertyValue>(map_v.begin(), map_v.end()));
    default:
      break;
  }
  throw TypedValueException(
      "Unsupported conversion from TypedValue to PropertyValue");
}

#define DEFINE_VALUE_AND_TYPE_GETTERS(type_param, type_enum, field)          \
  template <>                                                                \
  type_param &TypedValue::Value<type_param>() {                              \
    if (type_ != Type::type_enum)                                            \
      throw TypedValueException("Incompatible template param and type");     \
    return field;                                                            \
  }                                                                          \
                                                                             \
  template <>                                                                \
  const type_param &TypedValue::Value<type_param>() const {                  \
    if (type_ != Type::type_enum)                                            \
      throw TypedValueException("Incompatible template param and type");     \
    return field;                                                            \
  }                                                                          \
                                                                             \
  type_param &TypedValue::Value##type_enum() { return Value<type_param>(); } \
                                                                             \
  const type_param &TypedValue::Value##type_enum() const {                   \
    return Value<type_param>();                                              \
  }                                                                          \
                                                                             \
  bool TypedValue::Is##type_enum() const { return type_ == Type::type_enum; }

DEFINE_VALUE_AND_TYPE_GETTERS(bool, Bool, bool_v)
DEFINE_VALUE_AND_TYPE_GETTERS(int64_t, Int, int_v)
DEFINE_VALUE_AND_TYPE_GETTERS(double, Double, double_v)
DEFINE_VALUE_AND_TYPE_GETTERS(std::string, String, string_v)
DEFINE_VALUE_AND_TYPE_GETTERS(std::vector<TypedValue>, List, list_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::value_map_t, Map, map_v)
DEFINE_VALUE_AND_TYPE_GETTERS(VertexAccessor, Vertex, vertex_v)
DEFINE_VALUE_AND_TYPE_GETTERS(EdgeAccessor, Edge, edge_v)
DEFINE_VALUE_AND_TYPE_GETTERS(Path, Path, path_v)

#undef DEFINE_VALUE_AND_TYPE_GETTERS

bool TypedValue::IsNull() const { return type_ == Type::Null; }

bool TypedValue::IsNumeric() const { return IsInt() || IsDouble(); }

bool TypedValue::IsPropertyValue() const {
  switch (type_) {
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
    case Type::List:
    case Type::Map:
      return true;
    default:
      return false;
  }
}

std::ostream &operator<<(std::ostream &os, const TypedValue::Type type) {
  switch (type) {
    case TypedValue::Type::Null:
      return os << "null";
    case TypedValue::Type::Bool:
      return os << "bool";
    case TypedValue::Type::Int:
      return os << "int";
    case TypedValue::Type::Double:
      return os << "double";
    case TypedValue::Type::String:
      return os << "string";
    case TypedValue::Type::List:
      return os << "list";
    case TypedValue::Type::Map:
      return os << "map";
    case TypedValue::Type::Vertex:
      return os << "vertex";
    case TypedValue::Type::Edge:
      return os << "edge";
    case TypedValue::Type::Path:
      return os << "path";
  }
  LOG(FATAL) << "Unsupported TypedValue::Type";
}

std::ostream &operator<<(std::ostream &os, const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return os << "Null";
    case TypedValue::Type::Bool:
      return os << (value.Value<bool>() ? "true" : "false");
    case TypedValue::Type::Int:
      return os << value.Value<int64_t>();
    case TypedValue::Type::Double:
      return os << value.Value<double>();
    case TypedValue::Type::String:
      return os << value.Value<std::string>();
    case TypedValue::Type::List:
      os << "[";
      utils::PrintIterable(os, value.Value<std::vector<TypedValue>>());
      return os << "]";
    case TypedValue::Type::Map:
      os << "{";
      utils::PrintIterable(os, value.Value<std::map<std::string, TypedValue>>(),
                           ", ", [](auto &stream, const auto &pair) {
                             stream << pair.first << ": " << pair.second;
                           });
      return os << "}";
    case TypedValue::Type::Vertex:
      return os << value.Value<VertexAccessor>();
    case TypedValue::Type::Edge:
      return os << value.Value<EdgeAccessor>();
    case TypedValue::Type::Path:
      return os << value.Value<Path>();
  }
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

#define DEFINE_TYPED_VALUE_ASSIGNMENT(type_param, typed_value_type, member) \
  TypedValue &TypedValue::operator=(const type_param &other) {              \
    if (this->type_ == TypedValue::Type::typed_value_type) {                \
      this->member = other;                                                 \
    } else {                                                                \
      *this = TypedValue(other);                                            \
    }                                                                       \
                                                                            \
    return *this;                                                           \
  }

DEFINE_TYPED_VALUE_ASSIGNMENT(char *const, String, string_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(int, Int, int_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(bool, Bool, bool_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(int64_t, Int, int_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(double, Double, double_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(std::string, String, string_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(std::vector<TypedValue>, List, list_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(TypedValue::value_map_t, Map, map_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(VertexAccessor, Vertex, vertex_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(EdgeAccessor, Edge, edge_v)
DEFINE_TYPED_VALUE_ASSIGNMENT(Path, Path, path_v)

#undef DEFINE_TYPED_VALUE_ASSIGNMENT

TypedValue &TypedValue::operator=(const TypedValue &other) {
  if (this != &other) {
    this->~TypedValue();
    // set the type of this
    type_ = other.type_;

    switch (other.type_) {
      case TypedValue::Type::Null:
        return *this;
      case TypedValue::Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case TypedValue::Type::Int:
        this->int_v = other.int_v;
        return *this;
      case TypedValue::Type::Double:
        this->double_v = other.double_v;
        return *this;
      case TypedValue::Type::String:
        new (&string_v) std::string(other.string_v);
        return *this;
      case TypedValue::Type::List:
        new (&list_v) std::vector<TypedValue>(other.list_v);
        return *this;
      case TypedValue::Type::Map:
        new (&map_v) std::map<std::string, TypedValue>(other.map_v);
        return *this;
      case TypedValue::Type::Vertex:
        new (&vertex_v) VertexAccessor(other.vertex_v);
        return *this;
      case TypedValue::Type::Edge:
        new (&edge_v) EdgeAccessor(other.edge_v);
        return *this;
      case TypedValue::Type::Path:
        new (&path_v) Path(other.path_v);
        return *this;
    }
    LOG(FATAL) << "Unsupported TypedValue::Type";
  }
  return *this;
}

const TypedValue TypedValue::Null = TypedValue();

TypedValue::~TypedValue() {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      return;

    // we need to call destructors for non primitive types since we used
    // placement new
    case Type::String:
      // Clang fails to compile ~std::string. It seems it is a bug in some
      // versions of clang. using namespace std statement solves the issue.
      using namespace std;
      string_v.~string();
      return;
    case Type::List:
      using namespace std;
      list_v.~vector<TypedValue>();
      return;
    case Type::Map:
      using namespace std;
      map_v.~map<std::string, TypedValue>();
      return;
    case Type::Vertex:
      vertex_v.~VertexAccessor();
      return;
    case Type::Edge:
      edge_v.~EdgeAccessor();
      return;
    case Type::Path:
      path_v.~Path();
      return;
  }
  LOG(FATAL) << "Unsupported TypedValue::Type";
}

/**
 * Returns the double value of a value.
 * The value MUST be either Double or Int.
 *
 * @param value
 * @return
 */
double ToDouble(const TypedValue &value) {
  switch (value.type()) {
    case TypedValue::Type::Int:
      return (double)value.Value<int64_t>();
    case TypedValue::Type::Double:
      return value.Value<double>();
    default:
      throw TypedValueException(
          "Unsupported TypedValue::Type conversion to double");
  }
}

TypedValue operator<(const TypedValue &a, const TypedValue &b) {
  auto is_legal = [](TypedValue::Type type) {
    switch (type) {
      case TypedValue::Type::Null:
      case TypedValue::Type::Int:
      case TypedValue::Type::Double:
      case TypedValue::Type::String:
        return true;
      default:
        return false;
    }
  };
  if (!is_legal(a.type()) || !is_legal(b.type()))
    throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(),
                              b.type());

  if (a.IsNull() || b.IsNull()) return TypedValue::Null;

  if (a.IsString() || b.IsString()) {
    if (a.type() != b.type()) {
      throw TypedValueException("Invalid 'less' operand types({} + {})",
                                a.type(), b.type());
    } else {
      return a.Value<std::string>() < b.Value<std::string>();
    }
  }

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return ToDouble(a) < ToDouble(b);
  } else {
    return a.Value<int64_t>() < b.Value<int64_t>();
  }
}

/** Equality between two typed values that returns either a bool
 * or Null TypedValue (never raises an exception).
 *
 * For the old version of equality that raised an exception
 * when comparing incompatible types, see the version of
 * this file at 2017-04-12.
 */
TypedValue operator==(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;

  // check we have values that can be compared
  // this means that either they're the same type, or (int, double) combo
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric()))) return false;

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return a.Value<bool>() == b.Value<bool>();
    case TypedValue::Type::Int:
      if (b.IsDouble())
        return ToDouble(a) == ToDouble(b);
      else
        return a.Value<int64_t>() == b.Value<int64_t>();
    case TypedValue::Type::Double:
      return ToDouble(a) == ToDouble(b);
    case TypedValue::Type::String:
      return a.Value<std::string>() == b.Value<std::string>();
    case TypedValue::Type::Vertex:
      return a.Value<VertexAccessor>() == b.Value<VertexAccessor>();
    case TypedValue::Type::Edge:
      return a.Value<EdgeAccessor>() == b.Value<EdgeAccessor>();
    case TypedValue::Type::List: {
      // We are not compatible with neo4j at this point. In neo4j 2 = [2]
      // compares
      // to true. That is not the end of unselfishness of developers at neo4j so
      // they allow us to use as many braces as we want to get to the truth in
      // list comparison, so [[2]] = [[[[[[2]]]]]] compares to true in neo4j as
      // well. Because, why not?
      // At memgraph we prefer sanity so [1,2] = [1,2] compares to true and
      // 2 = [2] compares to false.
      auto &list_a = a.Value<std::vector<TypedValue>>();
      auto &list_b = b.Value<std::vector<TypedValue>>();
      if (list_a.size() != list_b.size()) return false;
      // two arrays are considered equal (by neo) if all their
      // elements are bool-equal. this means that:
      //    [1] == [null] -> false
      //    [null] == [null] -> true
      // in that sense array-comparison never results in Null
      return std::equal(list_a.begin(), list_a.end(), list_b.begin(),
                        TypedValue::BoolEqual{});
    }
    case TypedValue::Type::Map: {
      auto &map_a = a.Value<std::map<std::string, TypedValue>>();
      auto &map_b = b.Value<std::map<std::string, TypedValue>>();
      if (map_a.size() != map_b.size()) return false;
      for (auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end()) return false;
        TypedValue comparison = kv_a.second == found_b_it->second;
        if (comparison.IsNull() || !comparison.Value<bool>()) return false;
      }
      return true;
    }
    case TypedValue::Type::Path:
      return a.ValuePath() == b.ValuePath();
    default:
      LOG(FATAL) << "Unhandled comparison for types";
  }
}

TypedValue operator!(const TypedValue &a) {
  if (a.IsNull()) return TypedValue::Null;
  if (a.IsBool()) return TypedValue(!a.Value<bool>());
  throw TypedValueException("Invalid logical not operand type (!{})", a.type());
}

/**
 * Turns a numeric or string value into a string.
 *
 * @param value a value.
 * @return A string.
 */
std::string ValueToString(const TypedValue &value) {
  if (value.IsString()) return value.Value<std::string>();
  if (value.IsInt()) return std::to_string(value.Value<int64_t>());
  if (value.IsDouble()) return fmt::format("{}", value.Value<double>());
  // unsupported situations
  throw TypedValueException(
      "Unsupported TypedValue::Type conversion to string");
}

TypedValue operator-(const TypedValue &a) {
  if (a.IsNull()) return TypedValue::Null;
  if (a.IsInt()) return -a.Value<int64_t>();
  if (a.IsDouble()) return -a.Value<double>();
  throw TypedValueException("Invalid unary minus operand type (-{})", a.type());
}

TypedValue operator+(const TypedValue &a) {
  if (a.IsNull()) return TypedValue::Null;
  if (a.IsInt()) return +a.Value<int64_t>();
  if (a.IsDouble()) return +a.Value<double>();
  throw TypedValueException("Invalid unary plus operand type (+{})", a.type());
}

/**
 * Raises a TypedValueException if the given values do not support arithmetic
 * operations. If they do, nothing happens.
 *
 * @param a First value.
 * @param b Second value.
 * @param string_ok If or not for the given operation it's valid to work with
 *  String values (typically it's OK only for sum).
 *  @param op_name Name of the operation, used only for exception description,
 *  if raised.
 */
inline void EnsureArithmeticallyOk(const TypedValue &a, const TypedValue &b,
                                   bool string_ok, const std::string &op_name) {
  auto is_legal = [string_ok](const TypedValue &value) {
    return value.IsNumeric() ||
           (string_ok && value.type() == TypedValue::Type::String);
  };

  // Note that List and Null can also be valid in arithmetic ops. They are not
  // checked here because they are handled before this check is performed in
  // arithmetic op implementations.

  if (!is_legal(a) || !is_legal(b))
    throw TypedValueException("Invalid {} operand types {}, {}", op_name,
                              a.type(), b.type());
}

TypedValue operator+(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;

  if (a.IsList() || b.IsList()) {
    std::vector<TypedValue> list;
    auto append_list = [&list](const TypedValue &v) {
      if (v.IsList()) {
        auto list2 = v.Value<std::vector<TypedValue>>();
        list.insert(list.end(), list2.begin(), list2.end());
      } else {
        list.push_back(v);
      }
    };
    append_list(a);
    append_list(b);
    return TypedValue(list);
  }

  EnsureArithmeticallyOk(a, b, true, "addition");
  // no more Bool nor Null, summing works on anything from here onward

  if (a.IsString() || b.IsString()) return ValueToString(a) + ValueToString(b);

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return ToDouble(a) + ToDouble(b);
  } else {
    return a.Value<int64_t>() + b.Value<int64_t>();
  }
}

TypedValue operator-(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  EnsureArithmeticallyOk(a, b, false, "subtraction");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return ToDouble(a) - ToDouble(b);
  } else {
    return a.Value<int64_t>() - b.Value<int64_t>();
  }
}

TypedValue operator/(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  EnsureArithmeticallyOk(a, b, false, "division");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return ToDouble(a) / ToDouble(b);
  } else {
    if (b.Value<int64_t>() == 0LL)
      throw TypedValueException("Division by zero");
    return a.Value<int64_t>() / b.Value<int64_t>();
  }
}

TypedValue operator*(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  EnsureArithmeticallyOk(a, b, false, "multiplication");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return ToDouble(a) * ToDouble(b);
  } else {
    return a.Value<int64_t>() * b.Value<int64_t>();
  }
}

TypedValue operator%(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  EnsureArithmeticallyOk(a, b, false, "modulo");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return (double)fmod(ToDouble(a), ToDouble(b));
  } else {
    if (b.Value<int64_t>() == 0LL) throw TypedValueException("Mod with zero");
    return a.Value<int64_t>() % b.Value<int64_t>();
  }
}

inline void EnsureLogicallyOk(const TypedValue &a, const TypedValue &b,
                              const std::string &op_name) {
  if (!((a.IsBool() || a.IsNull()) && (b.IsBool() || b.IsNull())))
    throw TypedValueException("Invalid {} operand types({} && {})", op_name,
                              a.type(), b.type());
}

TypedValue operator&&(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical AND");
  // at this point we only have null and bool
  // if either operand is false, the result is false
  if (a.IsBool() && !a.Value<bool>()) return false;
  if (b.IsBool() && !b.Value<bool>()) return false;
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  // neither is false, neither is null, thus both are true
  return true;
}

TypedValue operator||(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical OR");
  // at this point we only have null and bool
  // if either operand is true, the result is true
  if (a.IsBool() && a.Value<bool>()) return true;
  if (b.IsBool() && b.Value<bool>()) return true;
  if (a.IsNull() || b.IsNull()) return TypedValue::Null;
  // neither is true, neither is null, thus both are false
  return false;
}

TypedValue operator^(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical XOR");
  // at this point we only have null and bool
  if (a.IsNull() || b.IsNull())
    return TypedValue::Null;
  else
    return static_cast<bool>(a.Value<bool>() ^ b.Value<bool>());
}

bool TypedValue::BoolEqual::operator()(const TypedValue &lhs,
                                       const TypedValue &rhs) const {
  if (lhs.IsNull() && rhs.IsNull()) return true;
  TypedValue equality_result = lhs == rhs;
  switch (equality_result.type()) {
    case TypedValue::Type::Bool:
      return equality_result.Value<bool>();
    case TypedValue::Type::Null:
      return false;
    default:
      LOG(FATAL)
          << "Equality between two TypedValues resulted in something other "
             "then Null or bool";
  }
}

size_t TypedValue::Hash::operator()(const TypedValue &value) const {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return 31;
    case TypedValue::Type::Bool:
      return std::hash<bool>{}(value.Value<bool>());
    case TypedValue::Type::Int:
      // we cast int to double for hashing purposes
      // to be consistent with TypedValue equality
      // in which (2.0 == 2) returns true
      return std::hash<double>{}((double)value.Value<int64_t>());
    case TypedValue::Type::Double:
      return std::hash<double>{}(value.Value<double>());
    case TypedValue::Type::String:
      return std::hash<std::string>{}(value.Value<std::string>());
    case TypedValue::Type::List: {
      return FnvCollection<std::vector<TypedValue>, TypedValue, Hash>{}(
          value.Value<std::vector<TypedValue>>());
    }
    case TypedValue::Type::Map: {
      size_t hash = 6543457;
      for (const auto &kv : value.Value<std::map<std::string, TypedValue>>()) {
        hash ^= std::hash<std::string>{}(kv.first);
        hash ^= this->operator()(kv.second);
      }
      return hash;
    }
    case TypedValue::Type::Vertex:
      return value.Value<VertexAccessor>().gid();
    case TypedValue::Type::Edge:
      return value.Value<EdgeAccessor>().gid();
    case TypedValue::Type::Path:
      return FnvCollection<std::vector<VertexAccessor>, VertexAccessor>{}(
                 value.ValuePath().vertices()) ^
             FnvCollection<std::vector<EdgeAccessor>, EdgeAccessor>{}(
                 value.ValuePath().edges());
  }
  LOG(FATAL) << "Unhandled TypedValue.type() in hash function";
}

}  // namespace query
