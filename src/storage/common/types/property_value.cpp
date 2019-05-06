#include "storage/common/types/property_value.hpp"

// const value extraction template instantiations
template <>
const bool &PropertyValue::Value<bool>() const {
  if (type_ != Type::Bool) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return bool_v;
}

template <>
const int64_t &PropertyValue::Value<int64_t>() const {
  if (type_ != Type::Int) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return int_v;
}

template <>
const double &PropertyValue::Value<double>() const {
  if (type_ != Type::Double) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return double_v;
}

template <>
const std::string &PropertyValue::Value<std::string>() const {
  if (type_ != Type::String) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return string_v;
}

template <>
const std::vector<PropertyValue>
    &PropertyValue::Value<std::vector<PropertyValue>>() const {
  if (type_ != Type::List) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return list_v;
}

template <>
const std::map<std::string, PropertyValue>
    &PropertyValue::Value<std::map<std::string, PropertyValue>>() const {
  if (type_ != Type::Map) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return map_v;
}

// value extraction template instantiations
template <>
bool &PropertyValue::Value<bool>() {
  if (type_ != Type::Bool) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return bool_v;
}

template <>
int64_t &PropertyValue::Value<int64_t>() {
  if (type_ != Type::Int) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return int_v;
}

template <>
double &PropertyValue::Value<double>() {
  if (type_ != Type::Double) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return double_v;
}

template <>
std::string &PropertyValue::Value<std::string>() {
  if (type_ != Type::String) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return string_v;
}

template <>
std::vector<PropertyValue> &PropertyValue::Value<std::vector<PropertyValue>>() {
  if (type_ != Type::List) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return list_v;
}

template <>
std::map<std::string, PropertyValue>
    &PropertyValue::Value<std::map<std::string, PropertyValue>>() {
  if (type_ != Type::Map) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return map_v;
}

// constructors
PropertyValue::PropertyValue(const PropertyValue &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      return;
    case Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case Type::String:
      new (&string_v) std::string(other.string_v);
      return;
    case Type::List:
      new (&list_v) std::vector<PropertyValue>(other.list_v);
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, PropertyValue>(other.map_v);
      return;
  }
}

PropertyValue::PropertyValue(PropertyValue &&other) : type_(other.type_) {
  switch (other.type_) {
    case Type::Null:
      return;
    case Type::Bool:
      this->bool_v = other.bool_v;
      return;
    case Type::Int:
      this->int_v = other.int_v;
      return;
    case Type::Double:
      this->double_v = other.double_v;
      return;
    case Type::String:
      new (&string_v) std::string(std::move(other.string_v));
      return;
    case Type::List:
      new (&list_v) std::vector<PropertyValue>(std::move(other.list_v));
      return;
    case Type::Map:
      new (&map_v) std::map<std::string, PropertyValue>(std::move(other.map_v));
      return;
  }
}

PropertyValue &PropertyValue::operator=(const PropertyValue &other) {
  if (this != &other) {
    DestroyValue();
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        break;
      case Type::List:
        new (&list_v) std::vector<PropertyValue>(other.list_v);
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, PropertyValue>(other.map_v);
        break;
    }
  }

  return *this;
}

PropertyValue &PropertyValue::operator=(PropertyValue &&other) {
  if (this != &other) {
    DestroyValue();
    type_ = other.type_;

    switch (other.type_) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<PropertyValue>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v)
            std::map<std::string, PropertyValue>(std::move(other.map_v));
        break;
    }

    // reset the type of other
    other = PropertyValue::Null;
  }

  return *this;
}

const PropertyValue PropertyValue::Null = PropertyValue();

void PropertyValue::DestroyValue() {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      return;

    // destructor for non primitive types since we used placement new
    case Type::String:
      // Clang fails to compile ~std::string. It seems it is a bug in some
      // versions of clang. Using namespace std statement solves the issue.
      using namespace std;
      string_v.~string();
      return;
    case Type::List:
      list_v.~vector();
      return;
    case Type::Map:
      map_v.~map();
      return;
  }
}

PropertyValue::~PropertyValue() { DestroyValue(); }

std::ostream &operator<<(std::ostream &os, const PropertyValue::Type type) {
  switch (type) {
    case PropertyValue::Type::Null:
      return os << "null";
    case PropertyValue::Type::Bool:
      return os << "bool";
    case PropertyValue::Type::String:
      return os << "string";
    case PropertyValue::Type::Int:
      return os << "int";
    case PropertyValue::Type::Double:
      return os << "double";
    case PropertyValue::Type::List:
      return os << "list";
    case PropertyValue::Type::Map:
      return os << "map";
  }
}

std::ostream &operator<<(std::ostream &os, const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      return os << "Null";
    case PropertyValue::Type::Bool:
      return os << (value.Value<bool>() ? "true" : "false");
    case PropertyValue::Type::String:
      return os << value.Value<std::string>();
    case PropertyValue::Type::Int:
      return os << value.Value<int64_t>();
    case PropertyValue::Type::Double:
      return os << value.Value<double>();
    case PropertyValue::Type::List:
      os << "[";
      for (const auto &x : value.Value<std::vector<PropertyValue>>()) {
        os << x << ",";
      }
      return os << "]";
    case PropertyValue::Type::Map:
      os << "{";
      for (const auto &kv :
           value.Value<std::map<std::string, PropertyValue>>()) {
        os << kv.first << ": " << kv.second << ",";
      }
      return os << "}";
  }
}

bool operator==(const PropertyValue &first, const PropertyValue &second) {
  if (first.type() != second.type()) return false;
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return true;
    case PropertyValue::Type::Bool:
      return first.Value<bool>() == second.Value<bool>();
    case PropertyValue::Type::Int:
      return first.Value<int64_t>() == second.Value<int64_t>();
    case PropertyValue::Type::Double:
      return first.Value<double>() == second.Value<double>();
    case PropertyValue::Type::String:
      return first.Value<std::string>() == second.Value<std::string>();
    case PropertyValue::Type::List:
      return first.Value<std::vector<PropertyValue>>() ==
             second.Value<std::vector<PropertyValue>>();
    case PropertyValue::Type::Map:
      return first.Value<std::map<std::string, PropertyValue>>() ==
             second.Value<std::map<std::string, PropertyValue>>();
  }
}

bool operator<(const PropertyValue &first, const PropertyValue &second) {
  if (first.type() != second.type()) return first.type() < second.type();
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return false;
    case PropertyValue::Type::Bool:
      return first.Value<bool>() < second.Value<bool>();
    case PropertyValue::Type::Int:
      return first.Value<int64_t>() < second.Value<int64_t>();
    case PropertyValue::Type::Double:
      return first.Value<double>() < second.Value<double>();
    case PropertyValue::Type::String:
      return first.Value<std::string>() < second.Value<std::string>();
    case PropertyValue::Type::List:
      return first.Value<std::vector<PropertyValue>>() <
             second.Value<std::vector<PropertyValue>>();
    case PropertyValue::Type::Map:
      return first.Value<std::map<std::string, PropertyValue>>() <
             second.Value<std::map<std::string, PropertyValue>>();
  }
}
