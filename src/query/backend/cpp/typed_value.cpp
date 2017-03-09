#include "query/backend/cpp/typed_value.hpp"

#include <fmt/format.h>
#include <cmath>
#include <iostream>
#include <memory>

#include "utils/assert.hpp"

TypedValue::TypedValue(const PropertyValue& value) {
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
    case PropertyValue::Type::String:
      type_ = Type::String;
      new (&string_v) std::string(value.Value<std::string>());
    case PropertyValue::Type::List:
      type_ = Type::List;
      auto vec = value.Value<std::vector<PropertyValue>>();
      new (&list_v) std::vector<TypedValue>(vec.begin(), vec.end());
      return;
  }
  permanent_fail("Unsupported type");
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
    default:
      throw TypedValueException(
          "Unsupported conversion from TypedValue to PropertyValue");
  }
}

// TODO: Refactor this. Value<bool> should be ValueBool. If we do it in that way
// we could return reference for complex types and value for primitive types.
// Other solution would be to add additional overloads for references, for
// example Value<string&>.
// Value extraction template instantiations
template <>
bool TypedValue::Value<bool>() const {
  if (type_ != TypedValue::Type::Bool) {
    throw TypedValueException("Incompatible template param and type");
  }
  return bool_v;
}

template <>
int64_t TypedValue::Value<int64_t>() const {
  if (type_ != TypedValue::Type::Int) {
    throw TypedValueException("Incompatible template param and type");
  }
  return int_v;
}

template <>
double TypedValue::Value<double>() const {
  if (type_ != TypedValue::Type::Double) {
    throw TypedValueException("Incompatible template param and type");
  }
  return double_v;
}

template <>
std::string TypedValue::Value<std::string>() const {
  if (type_ != TypedValue::Type::String) {
    throw TypedValueException("Incompatible template param and type");
  }
  return string_v;
}

template <>
std::vector<TypedValue> TypedValue::Value<std::vector<TypedValue>>() const {
  if (type_ != TypedValue::Type::List) {
    throw TypedValueException("Incompatible template param and type");
  }
  return list_v;
}

template <>
std::map<std::string, TypedValue>
TypedValue::Value<std::map<std::string, TypedValue>>() const {
  if (type_ != TypedValue::Type::Map) {
    throw TypedValueException("Incompatible template param and type");
  }
  return map_v;
}

template <>
VertexAccessor TypedValue::Value<VertexAccessor>() const {
  if (type_ != TypedValue::Type::Vertex) {
    throw TypedValueException("Incompatible template param and type");
  }
  return vertex_v;
}

template <>
EdgeAccessor TypedValue::Value<EdgeAccessor>() const {
  if (type_ != TypedValue::Type::Edge) {
    throw TypedValueException("Incompatible template param and type");
  }
  return edge_v;
}

template <>
Path TypedValue::Value<Path>() const {
  if (type_ != TypedValue::Type::Path) {
    throw TypedValueException("Incompatible template param and type");
  }
  return path_v;
}

TypedValue::TypedValue(const TypedValue& other) : type_(other.type_) {
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
  }
  permanent_fail("Unsupported TypedValue::Type");
}

std::ostream& operator<<(std::ostream& os, const TypedValue::Type type) {
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
  permanent_fail("Unsupported TypedValue::Type");
}

std::ostream& operator<<(std::ostream& os, const TypedValue& value) {
  switch (value.type_) {
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
      for (const auto& x : value.Value<std::vector<TypedValue>>()) {
        os << x << ",";
      }
      return os << "]";
    case TypedValue::Type::Map:
      os << "{";
      for (const auto& x : value.Value<std::map<std::string, TypedValue>>()) {
        os << x.first << ": " << x.second << ",";
      }
      return os << "}";
    case TypedValue::Type::Vertex:
      return os << value.Value<VertexAccessor>();
    case TypedValue::Type::Edge:
      return os << value.Value<EdgeAccessor>();
    case TypedValue::Type::Path:
      return os << value.Value<Path>();
  }
  permanent_fail("Unsupported PropertyValue::Type");
}

TypedValue& TypedValue::operator=(const TypedValue& other) {
  // set the type of this
  this->~TypedValue();
  type_ = other.type_;

  if (this != &other) {
    switch (other.type_) {
      case TypedValue::Type::Null:
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
  }
  permanent_fail("Unsupported TypedValue::Type");
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
  permanent_fail("Unsupported TypedValue::Type");
}

/**
 * Returns the double value of a value.
 * The value MUST be either Double or Int.
 *
 * @param value
 * @return
 */
double ToDouble(const TypedValue& value) {
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

TypedValue operator<(const TypedValue& a, const TypedValue& b) {
  if (a.type() == TypedValue::Type::Bool || b.type() == TypedValue::Type::Bool)
    throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(),
                              b.type());

  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  if (a.type() == TypedValue::Type::String ||
      b.type() == TypedValue::Type::String) {
    if (a.type() != b.type()) {
      throw TypedValueException("Invalid equality operand types({} + {})",
                                a.type(), b.type());
    } else {
      return a.Value<std::string>() < b.Value<std::string>();
    }
  }

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) < ToDouble(b);
  } else {
    return a.Value<int64_t>() < b.Value<int64_t>();
  }
}

// TODO: 2 = "2" -> false, I don't think this is handled correctly at the
// moment.
TypedValue operator==(const TypedValue& a, const TypedValue& b) {
  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  if (a.type() == TypedValue::Type::List ||
      b.type() == TypedValue::Type::List) {
    if (a.type() == TypedValue::Type::List &&
        b.type() == TypedValue::Type::List) {
      // Potential optimisation: There is no need to copies of both lists to
      // compare them. If operator becomes a friend of TypedValue class then
      // we
      // can compare list_v-s directly.
      auto list1 = a.Value<std::vector<TypedValue>>();
      auto list2 = b.Value<std::vector<TypedValue>>();
      if (list1.size() != list2.size()) return false;
      for (int i = 0; i < (int)list1.size(); ++i) {
        if (!(list1[i] == list2[i]).Value<bool>()) {
          return false;
        }
      }
      return true;
    }
    // We are not compatible with neo4j at this point. In neo4j 2 = [2]
    // compares
    // to true. That is not the end of unselfishness of developers at neo4j so
    // they allow us to use as many braces as we want to get to the truth in
    // list comparison, so [[2]] = [[[[[[2]]]]]] compares to true in neo4j as
    // well. Because, why not?
    // At memgraph we prefer sanity so [1,2] = [1,2] compares to true and
    // 2 = [2] compares to false.
    return false;
  }

  if (a.type() == TypedValue::Type::String ||
      b.type() == TypedValue::Type::String) {
    if (a.type() != b.type()) {
      throw TypedValueException("Invalid equality operand types({} + {})",
                                a.type(), b.type());
    } else {
      return a.Value<std::string>() == b.Value<std::string>();
    }
  }

  if (a.type() == TypedValue::Type::Bool ||
      b.type() == TypedValue::Type::Bool) {
    if (a.type() != b.type()) {
      throw TypedValueException("Invalid equality operand types({} + {})",
                                a.type(), b.type());
    } else {
      return a.Value<bool>() == b.Value<bool>();
    }
  }
  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) == ToDouble(b);
  } else {
    return a.Value<int64_t>() == b.Value<int64_t>();
  }
}

TypedValue operator!(const TypedValue& a) {
  switch (a.type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Bool:
      return TypedValue(!a.Value<bool>());
    default:
      throw TypedValueException("Invalid logical not operand type (!{})",
                                a.type());
  }
}

/**
 * Turns a numeric or string value into a string.
 *
 * @param value a value.
 * @return A string.
 */
std::string ValueToString(const TypedValue& value) {
  switch (value.type()) {
    case TypedValue::Type::String:
      return value.Value<std::string>();
    case TypedValue::Type::Int:
      return std::to_string(value.Value<int64_t>());
    case TypedValue::Type::Double:
      return fmt::format("{}", value.Value<double>());
    // unsupported situations
    default:
      throw TypedValueException(
          "Unsupported TypedValue::Type conversion to string");
  }
}

TypedValue operator-(const TypedValue& a) {
  switch (a.type()) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return -a.Value<int64_t>();
    case TypedValue::Type::Double:
      return -a.Value<double>();
    default:
      throw TypedValueException("Invalid unary minus operand type (-{})",
                                a.type());
  }
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
inline void EnsureArithmeticallyOk(const TypedValue& a, const TypedValue& b,
                                   bool string_ok, const std::string& op_name) {
  if (a.type() == TypedValue::Type::Bool || b.type() == TypedValue::Type::Bool)
    throw TypedValueException("Invalid {} operand types {}, {}", op_name,
                              a.type(), b.type());

  if (string_ok) return;

  if (a.type() == TypedValue::Type::String ||
      b.type() == TypedValue::Type::String)
    throw TypedValueException("Invalid subtraction operands types {}, {}",
                              a.type(), b.type());
}

TypedValue operator+(const TypedValue& a, const TypedValue& b) {
  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  if (a.type() == TypedValue::Type::List ||
      b.type() == TypedValue::Type::List) {
    std::vector<TypedValue> list;
    auto append_list = [&list](const TypedValue& v) {
      if (v.type() == TypedValue::Type::List) {
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

  if (a.type() == TypedValue::Type::String ||
      b.type() == TypedValue::Type::String)
    return ValueToString(a) + ValueToString(b);

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) + ToDouble(b);
  } else {
    return a.Value<int64_t>() + b.Value<int64_t>();
  }
}

TypedValue operator-(const TypedValue& a, const TypedValue& b) {
  EnsureArithmeticallyOk(a, b, false, "subtraction");

  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) - ToDouble(b);
  } else {
    return a.Value<int64_t>() - b.Value<int64_t>();
  }
}

TypedValue operator/(const TypedValue& a, const TypedValue& b) {
  EnsureArithmeticallyOk(a, b, false, "division");

  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) / ToDouble(b);
  } else {
    return a.Value<int64_t>() / b.Value<int64_t>();
  }
}

TypedValue operator*(const TypedValue& a, const TypedValue& b) {
  EnsureArithmeticallyOk(a, b, false, "multiplication");

  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return ToDouble(a) * ToDouble(b);
  } else {
    return a.Value<int64_t>() * b.Value<int64_t>();
  }
}

TypedValue operator%(const TypedValue& a, const TypedValue& b) {
  EnsureArithmeticallyOk(a, b, false, "modulo");

  if (a.type() == TypedValue::Type::Null || b.type() == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and double
  if (a.type() == TypedValue::Type::Double ||
      b.type() == TypedValue::Type::Double) {
    return (double)fmod(ToDouble(a), ToDouble(b));
  } else {
    return a.Value<int64_t>() % b.Value<int64_t>();
  }
}

inline bool IsLogicallyOk(const TypedValue& a) {
  return a.type() == TypedValue::Type::Bool ||
         a.type() == TypedValue::Type::Null;
}

// TODO: Fix bugs in && and ||. null or true -> true; false and null -> false
TypedValue operator&&(const TypedValue& a, const TypedValue& b) {
  if (IsLogicallyOk(a) && IsLogicallyOk(b)) {
    if (a.type() == TypedValue::Type::Null ||
        b.type() == TypedValue::Type::Null) {
      return TypedValue::Null;
    } else {
      return a.Value<bool>() && b.Value<bool>();
    }
  } else {
    throw TypedValueException("Invalid logical and operand types({} && {})",
                              a.type(), b.type());
  }
}

TypedValue operator||(const TypedValue& a, const TypedValue& b) {
  if (IsLogicallyOk(a) && IsLogicallyOk(b)) {
    if (a.type() == TypedValue::Type::Null ||
        b.type() == TypedValue::Type::Null) {
      return TypedValue::Null;
    } else {
      return a.Value<bool>() || b.Value<bool>();
    }
  } else {
    throw TypedValueException("Invalid logical and operand types({} && {})",
                              a.type(), b.type());
  }
}

TypedValue operator^(const TypedValue& a, const TypedValue& b) {
  if (IsLogicallyOk(a) && IsLogicallyOk(b)) {
    if (a.type() == TypedValue::Type::Null ||
        b.type() == TypedValue::Type::Null) {
      return TypedValue::Null;
    } else {
      return static_cast<bool>(a.Value<bool>() ^ b.Value<bool>());
    }
  } else {
    throw TypedValueException("Invalid logical and operand types({} && {})",
                              a.type(), b.type());
  }
}
