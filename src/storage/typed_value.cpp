#include <memory>
#include <iostream>
#include <cmath>
#include <fmt/format.h>

#include "storage/typed_value.hpp"
#include "utils/assert.hpp"

// Value extraction template instantiations
template<>
bool TypedValue::Value<bool>() const {
  runtime_assert(type_ == TypedValue::Type::Bool, "Incompatible template param and type");
  return bool_v;
}

template<>
std::string TypedValue::Value<std::string>() const {
  runtime_assert(type_ == TypedValue::Type::String, "Incompatible template param and type");
  return *string_v;
}

template<>
int TypedValue::Value<int>() const {
  runtime_assert(type_ == TypedValue::Type::Int, "Incompatible template param and type");
  return int_v;
}

template<>
float TypedValue::Value<float>() const {
  runtime_assert(type_ == TypedValue::Type::Float, "Incompatible template param and type");
  return float_v;
}

TypedValue::TypedValue(const TypedValue &other) : type_(other.type_) {
  switch (other.type_) {
    case TypedValue::Type::Null:
      return;

    case TypedValue::Type::Bool:
      this->bool_v = other.bool_v;
      return;

    case TypedValue::Type::String:
      new(&string_v) std::shared_ptr<std::string>(other.string_v);
      return;

    case Type::Int:
      this->int_v = other.int_v;
      return;

    case Type::Float:
      this->float_v = other.float_v;
      return;
  }

  permanent_fail("Unsupported TypedValue::Type");
}

std::ostream &operator<<(std::ostream &os, const TypedValue::Type type) {
  switch (type) {
    case TypedValue::Type::Null:
      return os << "null";
    case TypedValue::Type::Bool:
      return os << "bool";
    case TypedValue::Type::String:
      return os << "string";
    case TypedValue::Type::Int:
      return os << "int";
    case TypedValue::Type::Float:
      return os << "float";
  }
  permanent_fail("Unsupported TypedValue::Type");
}

std::ostream &operator<<(std::ostream &os, const TypedValue &value) {
  switch (value.type_) {
    case TypedValue::Type::Null:
      return os << "Null";
    case TypedValue::Type::Bool:
      return os << (value.Value<bool>() ? "true" : "false");
    case TypedValue::Type::String:
      return os << value.Value<std::string>();
    case TypedValue::Type::Int:
      return os << value.Value<int>();
    case TypedValue::Type::Float:
      return os << value.Value<float>();
  }
  permanent_fail("Unsupported TypedValue::Type");
}

TypedValue &TypedValue::operator=(TypedValue &&other) {

  // set the type of this
  const_cast<Type&>(type_) = other.type_;

  if (this != &other) {
    switch (other.type_) {
      case TypedValue::Type::Null:
      case TypedValue::Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case TypedValue::Type::String:
        this->string_v = std::move(other.string_v);
        return *this;
      case TypedValue::Type::Int:
        this->int_v = other.int_v;
        return *this;
      case TypedValue::Type::Float:
        this->float_v = other.float_v;
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
    case Type::Float:
      return;

      // destructor for shared pointer must release
    case Type::String:
      string_v.~shared_ptr<std::string>();
      return;
  }
  permanent_fail("Unsupported TypedValue::Type");
}

/**
 * Returns the float value of a value.
 * The value MUST be either Float or Int.
 *
 * @param value
 * @return
 */
float ToFloat(const TypedValue& value) {
  switch (value.type_) {
    case TypedValue::Type::Int:
      return (float)value.Value<int>();
    case TypedValue::Type::Float:
      return value.Value<float>();

    default:
      permanent_fail("Unsupported TypedValue::Type");
  }
}

TypedValue operator<(const TypedValue& a, const TypedValue& b) {
  if (a.type_ == TypedValue::Type::Bool || b.type_ == TypedValue::Type::Bool)
    throw TypedValueException("Invalid 'less' operand types({} + {})", a.type_, b.type_);

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  if (a.type_ == TypedValue::Type::String || b.type_ == TypedValue::Type::String) {
    if (a.type_ != b.type_)
      throw TypedValueException("Invalid equality operand types({} + {})", a.type_, b.type_);
    else
      return a.Value<std::string>() < b.Value<std::string>();
  }

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float)
    return ToFloat(a) < ToFloat(b);
  else
    return a.Value<int>() < b.Value<int>();
}

TypedValue operator==(const TypedValue& a, const TypedValue& b) {

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  if (a.type_ == TypedValue::Type::String || b.type_ == TypedValue::Type::String) {
    if (a.type_ != b.type_)
      throw TypedValueException("Invalid equality operand types({} + {})", a.type_, b.type_);
    else
      return a.Value<std::string>() == b.Value<std::string>();
  }

  if (a.type_ == TypedValue::Type::Bool || b.type_ == TypedValue::Type::Bool) {
    if (a.type_ != b.type_)
      throw TypedValueException("Invalid equality operand types({} + {})", a.type_, b.type_);
    else
      return a.Value<bool>() == b.Value<bool>();
  }
  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return ToFloat(a) == ToFloat(b);
  } else
    return a.Value<int>() == b.Value<int>();

}

TypedValue operator!(const TypedValue& a) {
  switch (a.type_) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Bool:
      return TypedValue(!a.Value<bool>());

    default:
      throw TypedValueException("Invalid logical not operand type (!{})", a.type_);
  }
}

/**
 * Turns a numeric or string value into a string.
 *
 * @param value a value.
 * @return A string.
 */
std::string ValueToString(const TypedValue &value) {
  switch (value.type_) {
    case TypedValue::Type::String:
      return value.Value<std::string>();
    case TypedValue::Type::Int:
      return std::to_string(value.Value<int>());
    case TypedValue::Type::Float:
      return fmt::format("{}", value.Value<float>());

    // unsupported situations
    default:
      permanent_fail("Unsupported TypedValue::Type");
  }
}

TypedValue operator-(const TypedValue &a) {
  switch (a.type_) {
    case TypedValue::Type::Null:
      return TypedValue::Null;
    case TypedValue::Type::Int:
      return -a.Value<int>();
    case TypedValue::Type::Float:
      return -a.Value<float>();

    default:
      throw TypedValueException("Invalid unary minus operand type (-{})", a.type_);
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
  if (a.type_ == TypedValue::Type::Bool || b.type_ == TypedValue::Type::Bool)
    throw TypedValueException("Invalid {} operand types {}, {}", op_name, a.type_, b.type_);

  if (string_ok)
    return;

  if (a.type_ == TypedValue::Type::String || b.type_ == TypedValue::Type::String)
    throw TypedValueException("Invalid subtraction operands types {}, {}", a.type_, b.type_);
}

TypedValue operator+(const TypedValue& a, const TypedValue& b){
  EnsureArithmeticallyOk(a, b, true, "addition");
  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  // no more Bool nor Null, summing works on anything from here onward

  if (a.type_ == TypedValue::Type::String || b.type_ == TypedValue::Type::String)
    return ValueToString(a) + ValueToString(b);

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return ToFloat(a) + ToFloat(b);
  } else
    return a.Value<int>() + b.Value<int>();
}

TypedValue operator-(const TypedValue& a, const TypedValue& b){
  EnsureArithmeticallyOk(a, b, false, "subtraction");

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return ToFloat(a) - ToFloat(b);
  } else
    return a.Value<int>() - b.Value<int>();
}

TypedValue operator/(const TypedValue& a, const TypedValue& b){
  EnsureArithmeticallyOk(a, b, false, "division");

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return ToFloat(a) / ToFloat(b);
  } else
    return a.Value<int>() / b.Value<int>();
}

TypedValue operator*(const TypedValue& a, const TypedValue& b){
  EnsureArithmeticallyOk(a, b, false, "multiplication");

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return ToFloat(a) * ToFloat(b);
  } else
    return a.Value<int>() * b.Value<int>();
}

TypedValue operator%(const TypedValue& a, const TypedValue& b){
  EnsureArithmeticallyOk(a, b, false, "modulo");

  if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
    return TypedValue::Null;

  // at this point we only have int and float
  if (a.type_ == TypedValue::Type::Float || b.type_ == TypedValue::Type::Float){
    return (float)fmod(ToFloat(a), ToFloat(b));
  } else
    return a.Value<int>() % b.Value<int>();
}

inline bool IsLogicallyOk(const TypedValue& a) {
  return a.type_ == TypedValue::Type::Bool || a.type_ == TypedValue::Type::Null;
}

TypedValue operator&&(const TypedValue& a, const TypedValue& b) {
  if(IsLogicallyOk(a) && IsLogicallyOk(b)){
    if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
      return TypedValue::Null;
    else
      return a.Value<bool>() && b.Value<bool>();
  } else
    throw TypedValueException("Invalid logical and operand types({} && {})", a.type_, b.type_);
}

TypedValue operator||(const TypedValue& a, const TypedValue& b) {
  if(IsLogicallyOk(a) && IsLogicallyOk(b)){
    if (a.type_ == TypedValue::Type::Null || b.type_ == TypedValue::Type::Null)
      return TypedValue::Null;
    else
      return a.Value<bool>() || b.Value<bool>();
  } else
    throw TypedValueException("Invalid logical and operand types({} && {})", a.type_, b.type_);
}
