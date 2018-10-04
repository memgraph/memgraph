#include "storage/common/property_value.hpp"

#include <cmath>
#include <iostream>
#include <memory>
#include <utility>

#include "fmt/format.h"
#include "glog/logging.h"

// Value extraction template instantiations
template <>
bool PropertyValue::Value<bool>() const {
  if (type_ != PropertyValue::Type::Bool) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return bool_v;
}

template <>
std::string PropertyValue::Value<std::string>() const {
  if (type_ != PropertyValue::Type::String) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return *string_v;
}

template <>
int64_t PropertyValue::Value<int64_t>() const {
  if (type_ != PropertyValue::Type::Int) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return int_v;
}

template <>
double PropertyValue::Value<double>() const {
  if (type_ != PropertyValue::Type::Double) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return double_v;
}

template <>
std::vector<PropertyValue> PropertyValue::Value<std::vector<PropertyValue>>()
    const {
  if (type_ != PropertyValue::Type::List) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return *list_v;
}

template <>
std::map<std::string, PropertyValue>
PropertyValue::Value<std::map<std::string, PropertyValue>>() const {
  if (type_ != PropertyValue::Type::Map) {
    throw PropertyValueException("Incompatible template param and type");
  }
  return *map_v;
}

PropertyValue::PropertyValue(const PropertyValue &other) : type_(other.type_) {
  switch (other.type_) {
    case PropertyValue::Type::Null:
      return;

    case PropertyValue::Type::Bool:
      this->bool_v = other.bool_v;
      return;

    case PropertyValue::Type::String:
      new (&string_v)
          std::unique_ptr<std::string>(new std::string(*other.string_v));
      return;

    case Type::Int:
      this->int_v = other.int_v;
      return;

    case Type::Double:
      this->double_v = other.double_v;
      return;

    case Type::List:
      new (&list_v) std::unique_ptr<std::vector<PropertyValue>>(
          new std::vector<PropertyValue>(*other.list_v));
      return;

    case Type::Map:
      new (&map_v) std::unique_ptr<std::map<std::string, PropertyValue>>(
          new std::map<std::string, PropertyValue>(*other.map_v));
      return;
  }

  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

PropertyValue::PropertyValue(PropertyValue &&other) : type_(other.type_) {
  switch (other.type_) {
    case PropertyValue::Type::Null:
      return;

    case PropertyValue::Type::Bool:
      this->bool_v = other.bool_v;
      return;

    case PropertyValue::Type::String:
      new (&string_v) std::unique_ptr<std::string>(std::move(other.string_v));
      return;

    case Type::Int:
      this->int_v = other.int_v;
      return;

    case Type::Double:
      this->double_v = other.double_v;
      return;

    case Type::List:
      new (&list_v)
          std::unique_ptr<std::vector<PropertyValue>>(std::move(other.list_v));
      return;

    case Type::Map:
      new (&map_v) std::unique_ptr<std::map<std::string, PropertyValue>>(
          std::move(other.map_v));
      return;
  }

  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

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
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

std::ostream &operator<<(std::ostream &os, const PropertyValue &value) {
  switch (value.type_) {
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
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

PropertyValue &PropertyValue::operator=(const PropertyValue &other) {
  this->~PropertyValue();
  type_ = other.type_;

  if (this != &other) {
    switch (other.type_) {
      case PropertyValue::Type::Null:
      case PropertyValue::Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case PropertyValue::Type::String:
        new (&string_v)
            std::unique_ptr<std::string>(new std::string(*other.string_v));
        return *this;
      case PropertyValue::Type::Int:
        this->int_v = other.int_v;
        return *this;
      case PropertyValue::Type::Double:
        this->double_v = other.double_v;
        return *this;
      case PropertyValue::Type::List:
        new (&list_v) std::unique_ptr<std::vector<PropertyValue>>(
            new std::vector<PropertyValue>(*other.list_v));
        return *this;
      case PropertyValue::Type::Map:
        new (&map_v) std::unique_ptr<std::map<std::string, PropertyValue>>(
            new std::map<std::string, PropertyValue>(*other.map_v));
        return *this;
    }
  }
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

PropertyValue &PropertyValue::operator=(PropertyValue &&other) {
  this->~PropertyValue();
  type_ = other.type_;

  if (this != &other) {
    switch (other.type_) {
      case PropertyValue::Type::Null:
      case PropertyValue::Type::Bool:
        this->bool_v = other.bool_v;
        return *this;
      case PropertyValue::Type::String:
        new (&string_v) std::unique_ptr<std::string>(std::move(other.string_v));
        return *this;
      case PropertyValue::Type::Int:
        this->int_v = other.int_v;
        return *this;
      case PropertyValue::Type::Double:
        this->double_v = other.double_v;
        return *this;
      case PropertyValue::Type::List:
        new (&list_v) std::unique_ptr<std::vector<PropertyValue>>(
            std::move(other.list_v));
        return *this;
      case PropertyValue::Type::Map:
        new (&map_v) std::unique_ptr<std::map<std::string, PropertyValue>>(
            std::move(other.map_v));
        return *this;
    }
  }
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}

const PropertyValue PropertyValue::Null = PropertyValue();

PropertyValue::~PropertyValue() {
  switch (type_) {
    // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      return;

    // destructor for shared pointer must release
    case Type::String:
      string_v.~unique_ptr<std::string>();
      return;
    case Type::List:
      list_v.~unique_ptr<std::vector<PropertyValue>>();
      return;
    case Type::Map:
      map_v.~unique_ptr<std::map<std::string, PropertyValue>>();
      return;
  }
  LOG(FATAL) << "Unsupported PropertyValue::Type";
}
