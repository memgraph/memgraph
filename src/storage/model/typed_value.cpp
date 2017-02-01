#include <memory>
#include <iostream>
#include <cmath>
#include <fmt/format.h>

#include "storage/model/typed_value.hpp"

// Value extraction template instantiations
template<>
bool TypedValue::Value<bool>() const {
  assert(type_ == TypedValue::Type::Bool);
  return bool_v;
}

template<>
string TypedValue::Value<string>() const {
  assert(type_ == TypedValue::Type::String);
  return *string_v;
}

template<>
int TypedValue::Value<int>() const {
  assert(type_ == TypedValue::Type::Int);
  return int_v;
}

template<>
float TypedValue::Value<float>() const {
  assert(type_ == TypedValue::Type::Float);
  return float_v;
}

TypedValue::TypedValue(const TypedValue &other) : type_(other.type_) {
  switch (other.type_) {

    case TypedValue::Type::Null:
      break;

    case TypedValue::Type::Bool:
      this->bool_v = other.bool_v;
      break;

    case TypedValue::Type::String:
      new(&string_v) std::shared_ptr<string>(other.string_v);
      break;

    case Type::Int:
      this->int_v = other.int_v;
      break;

    case Type::Float:
      this->float_v = other.float_v;
      break;
  }
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
}

std::ostream &operator<<(std::ostream &os, const TypedValue &property) {
  switch (property.type_) {
    case TypedValue::Type::Null:
      return os << "Null";
    case TypedValue::Type::Bool:
      return os << (property.Value<bool>() ? "true" : "false");
    case TypedValue::Type::String:
      return os << property.Value<std::string>();
    case TypedValue::Type::Int:
      return os << property.Value<int>();
    case TypedValue::Type::Float:
      return os << property.Value<float>();
  }
}

TypedValue &TypedValue::operator=(TypedValue &&other) {

  // set the type of this
  const_cast<Type&>(type_) = other.type_;

  if (this != &other) {
    switch (other.type_) {
      case TypedValue::Type::Null:
      case TypedValue::Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case TypedValue::Type::String:
        this->string_v = std::move(other.string_v);
        break;
      case TypedValue::Type::Int:
        this->int_v = other.int_v;
        break;
      case TypedValue::Type::Float:
        this->float_v = other.float_v;
        break;
    }
  }

  return *this;
}

const TypedValue TypedValue::Null = TypedValue();

TypedValue::~TypedValue() {

  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
      break;
    case Type::Bool:
      break;
    case Type::Int:
      break;
    case Type::Float:
      break;

      // destructor for shared pointer must release
    case Type::String:
      string_v.~shared_ptr<string>();
      break;
  }
}

/**
 * Returns the float value of a property.
 * The property MUST be either Float or Int.
 *
 * @param prop
 * @return
 */
float ToFloat(const TypedValue& prop) {
  switch (prop.type_) {
    case TypedValue::Type::Int:
      return (float)prop.Value<int>();
    case TypedValue::Type::Float:
      return prop.Value<float>();
    case TypedValue::Type::Null:
    case TypedValue::Type::String:
    case TypedValue::Type::Bool:
      // TODO switch to production-exception
      assert(false);
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
      return a.Value<string>() < b.Value<string>();
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
      return a.Value<string>() == b.Value<string>();
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
    case TypedValue::Type::Int:
    case TypedValue::Type::Float:
    case TypedValue::Type::String:
      throw TypedValueException("Invalid logical not operand type (!{})", a.type_);
  }
}


/*
 * Derived comparsion operators.
 */
//TypedValue operator!=(const TypedValue& a, const TypedValue& b) {
//  return !(a == b);
//}
//
//TypedValue operator>(const TypedValue& a, const TypedValue& b) {
//  return (a < b) && (a != b);
//}
//TypedValue operator>=(const TypedValue& a, const TypedValue& b) {
//  return operator!(a < b);
//}
//TypedValue operator<=(const TypedValue& a, const TypedValue& b){
//  return (a < b) || (a == b);
//}

/**
 * Turns a numeric or string property into a string.
 *
 * @param prop Property.
 * @return A string.
 */
std::string PropToString(const TypedValue& prop) {
  switch (prop.type_) {
    case TypedValue::Type::String:
      return prop.Value<string>();
    case TypedValue::Type::Int:
      return std::to_string(prop.Value<int>());
    case TypedValue::Type::Float:
      return fmt::format("{}", prop.Value<float>());
    default:
      // TODO change to release-assert
      // This should never happen
      assert(false);
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
    case TypedValue::Type::Bool:
    case TypedValue::Type::String:
      throw TypedValueException("Invalid unary minus operand type (-{})", a.type_);
  }
}

/**
 * Raises a PropertyException if the given properties do not support arithmetic
 * operations. If they do, nothing happens.
 *
 * @param a First prop.
 * @param b Second prop.
 * @param string_ok If or not for the given operation it's valid to work with
 *  String values (typically it's OK only for sum).
 *  @param op_name Name of the operation, used only for exception description,
 *  if raised.
 */
inline void EnsureArithmeticallyOk(const TypedValue& a, const TypedValue& b, bool string_ok, const string& op_name) {
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
    return PropToString(a) + PropToString(b);

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
