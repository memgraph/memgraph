#include "storage/common/types/property_value.hpp"

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

PropertyValue::PropertyValue(PropertyValue &&other) noexcept
    : type_(other.type_) {
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

PropertyValue &PropertyValue::operator=(PropertyValue &&other) noexcept {
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
    other.DestroyValue();
    other.type_ = Type::Null;
  }

  return *this;
}

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
      return os << (value.ValueBool() ? "true" : "false");
    case PropertyValue::Type::String:
      return os << value.ValueString();
    case PropertyValue::Type::Int:
      return os << value.ValueInt();
    case PropertyValue::Type::Double:
      return os << value.ValueDouble();
    case PropertyValue::Type::List:
      os << "[";
      for (const auto &x : value.ValueList()) {
        os << x << ",";
      }
      return os << "]";
    case PropertyValue::Type::Map:
      os << "{";
      for (const auto &kv :
           value.ValueMap()) {
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
      return first.ValueBool() == second.ValueBool();
    case PropertyValue::Type::Int:
      return first.ValueInt() == second.ValueInt();
    case PropertyValue::Type::Double:
      return first.ValueDouble() == second.ValueDouble();
    case PropertyValue::Type::String:
      return first.ValueString() == second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() == second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() == second.ValueMap();
  }
}

bool operator<(const PropertyValue &first, const PropertyValue &second) {
  if (first.type() != second.type()) return first.type() < second.type();
  switch (first.type()) {
    case PropertyValue::Type::Null:
      return false;
    case PropertyValue::Type::Bool:
      return first.ValueBool() < second.ValueBool();
    case PropertyValue::Type::Int:
      return first.ValueInt() < second.ValueInt();
    case PropertyValue::Type::Double:
      return first.ValueDouble() < second.ValueDouble();
    case PropertyValue::Type::String:
      return first.ValueString() < second.ValueString();
    case PropertyValue::Type::List:
      return first.ValueList() < second.ValueList();
    case PropertyValue::Type::Map:
      return first.ValueMap() < second.ValueMap();
  }
}
