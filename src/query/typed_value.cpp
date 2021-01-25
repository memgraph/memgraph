#include "query/typed_value.hpp"

#include <fmt/format.h>
#include <cmath>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>

#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"

namespace query {

TypedValue::TypedValue(const storage::PropertyValue &value)
    // TODO: MemoryResource in storage::PropertyValue
    : TypedValue(value, utils::NewDeleteResource()) {}

TypedValue::TypedValue(const storage::PropertyValue &value,
                       utils::MemoryResource *memory)
    : memory_(memory) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      return;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = value.ValueBool();
      return;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = value.ValueInt();
      return;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = value.ValueDouble();
      return;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      new (&string_v) TString(value.ValueString(), memory_);
      return;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      const auto &vec = value.ValueList();
      new (&list_v) TVector(memory_);
      list_v.reserve(vec.size());
      for (const auto &v : vec) list_v.emplace_back(v);
      return;
    }
    case storage::PropertyValue::Type::Map: {
      type_ = Type::Map;
      const auto &map = value.ValueMap();
      new (&map_v) TMap(memory_);
      for (const auto &kv : map) map_v.emplace(kv.first, kv.second);
      return;
    }
  }
  LOG_FATAL("Unsupported type");
}

TypedValue::TypedValue(storage::PropertyValue &&other) /* noexcept */
    // TODO: MemoryResource in storage::PropertyValue, so this can be noexcept
    : TypedValue(std::move(other), utils::NewDeleteResource()) {}

TypedValue::TypedValue(storage::PropertyValue &&other,
                       utils::MemoryResource *memory)
    : memory_(memory) {
  switch (other.type()) {
    case storage::PropertyValue::Type::Null:
      type_ = Type::Null;
      break;
    case storage::PropertyValue::Type::Bool:
      type_ = Type::Bool;
      bool_v = other.ValueBool();
      break;
    case storage::PropertyValue::Type::Int:
      type_ = Type::Int;
      int_v = other.ValueInt();
      break;
    case storage::PropertyValue::Type::Double:
      type_ = Type::Double;
      double_v = other.ValueDouble();
      break;
    case storage::PropertyValue::Type::String:
      type_ = Type::String;
      new (&string_v) TString(other.ValueString(), memory_);
      break;
    case storage::PropertyValue::Type::List: {
      type_ = Type::List;
      auto &vec = other.ValueList();
      new (&list_v) TVector(memory_);
      list_v.reserve(vec.size());
      for (auto &v : vec) list_v.emplace_back(std::move(v));
      break;
    }
    case storage::PropertyValue::Type::Map: {
      type_ = Type::Map;
      auto &map = other.ValueMap();
      new (&map_v) TMap(memory_);
      for (auto &kv : map) map_v.emplace(kv.first, std::move(kv.second));
      break;
    }
  }

  other = storage::PropertyValue();
}

TypedValue::TypedValue(const TypedValue &other)
    : TypedValue(other, std::allocator_traits<utils::Allocator<TypedValue>>::
                            select_on_container_copy_construction(other.memory_)
                                .GetMemoryResource()) {}

TypedValue::TypedValue(const TypedValue &other, utils::MemoryResource *memory)
    : memory_(memory), type_(other.type_) {
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
      new (&string_v) TString(other.string_v, memory_);
      return;
    case Type::List:
      new (&list_v) TVector(other.list_v, memory_);
      return;
    case Type::Map:
      new (&map_v) TMap(other.map_v, memory_);
      return;
    case Type::Vertex:
      new (&vertex_v) VertexAccessor(other.vertex_v);
      return;
    case Type::Edge:
      new (&edge_v) EdgeAccessor(other.edge_v);
      return;
    case Type::Path:
      new (&path_v) Path(other.path_v, memory_);
      return;
  }
  LOG_FATAL("Unsupported TypedValue::Type");
}

TypedValue::TypedValue(TypedValue &&other) noexcept
    : TypedValue(std::move(other), other.memory_) {}

TypedValue::TypedValue(TypedValue &&other, utils::MemoryResource *memory)
    : memory_(memory), type_(other.type_) {
  switch (other.type_) {
    case TypedValue::Type::Null:
      break;
    case TypedValue::Type::Bool:
      this->bool_v = other.bool_v;
      break;
    case Type::Int:
      this->int_v = other.int_v;
      break;
    case Type::Double:
      this->double_v = other.double_v;
      break;
    case TypedValue::Type::String:
      new (&string_v) TString(std::move(other.string_v), memory_);
      break;
    case Type::List:
      new (&list_v) TVector(std::move(other.list_v), memory_);
      break;
    case Type::Map:
      new (&map_v) TMap(std::move(other.map_v), memory_);
      break;
    case Type::Vertex:
      new (&vertex_v) VertexAccessor(std::move(other.vertex_v));
      break;
    case Type::Edge:
      new (&edge_v) EdgeAccessor(std::move(other.edge_v));
      break;
    case Type::Path:
      new (&path_v) Path(std::move(other.path_v), memory_);
      break;
  }
  other.DestroyValue();
}

TypedValue::operator storage::PropertyValue() const {
  switch (type_) {
    case TypedValue::Type::Null:
      return storage::PropertyValue();
    case TypedValue::Type::Bool:
      return storage::PropertyValue(bool_v);
    case TypedValue::Type::Int:
      return storage::PropertyValue(int_v);
    case TypedValue::Type::Double:
      return storage::PropertyValue(double_v);
    case TypedValue::Type::String:
      return storage::PropertyValue(std::string(string_v));
    case TypedValue::Type::List:
      return storage::PropertyValue(
          std::vector<storage::PropertyValue>(list_v.begin(), list_v.end()));
    case TypedValue::Type::Map: {
      std::map<std::string, storage::PropertyValue> map;
      for (const auto &kv : map_v) map.emplace(kv.first, kv.second);
      return storage::PropertyValue(std::move(map));
    }
    default:
      break;
  }
  throw TypedValueException(
      "Unsupported conversion from TypedValue to PropertyValue");
}

#define DEFINE_VALUE_AND_TYPE_GETTERS(type_param, type_enum, field)            \
  type_param &TypedValue::Value##type_enum() {                                 \
    if (type_ != Type::type_enum)                                              \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, \
                                Type::type_enum);                              \
    return field;                                                              \
  }                                                                            \
                                                                               \
  const type_param &TypedValue::Value##type_enum() const {                     \
    if (type_ != Type::type_enum)                                              \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, \
                                Type::type_enum);                              \
    return field;                                                              \
  }                                                                            \
                                                                               \
  bool TypedValue::Is##type_enum() const { return type_ == Type::type_enum; }

DEFINE_VALUE_AND_TYPE_GETTERS(bool, Bool, bool_v)
DEFINE_VALUE_AND_TYPE_GETTERS(int64_t, Int, int_v)
DEFINE_VALUE_AND_TYPE_GETTERS(double, Double, double_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TString, String, string_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TVector, List, list_v)
DEFINE_VALUE_AND_TYPE_GETTERS(TypedValue::TMap, Map, map_v)
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

std::ostream &operator<<(std::ostream &os, const TypedValue::Type &type) {
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
  LOG_FATAL("Unsupported TypedValue::Type");
}

#define DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(type_param, typed_value_type, \
                                           member)                       \
  TypedValue &TypedValue::operator=(type_param other) {                  \
    if (this->type_ == TypedValue::Type::typed_value_type) {             \
      this->member = other;                                              \
    } else {                                                             \
      *this = TypedValue(other, memory_);                                \
    }                                                                    \
                                                                         \
    return *this;                                                        \
  }

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const char *, String, string_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int, Int, int_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(bool, Bool, bool_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int64_t, Int, int_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(double, Double, double_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const std::string_view &, String, string_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValue::TVector &, List, list_v)

TypedValue &TypedValue::operator=(const std::vector<TypedValue> &other) {
  if (type_ == Type::List) {
    list_v.reserve(other.size());
    list_v.assign(other.begin(), other.end());
  } else {
    *this = TypedValue(other, memory_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValue::TMap &, Map, map_v)

TypedValue &TypedValue::operator=(
    const std::map<std::string, TypedValue> &other) {
  if (type_ == Type::Map) {
    map_v.clear();
    for (const auto &kv : other) map_v.emplace(kv.first, kv.second);
  } else {
    *this = TypedValue(other, memory_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const VertexAccessor &, Vertex, vertex_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const EdgeAccessor &, Edge, edge_v)
DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const Path &, Path, path_v)

#undef DEFINE_TYPED_VALUE_COPY_ASSIGNMENT

#define DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(type_param, typed_value_type, \
                                           member)                       \
  TypedValue &TypedValue::operator=(type_param &&other) {                \
    if (this->type_ == TypedValue::Type::typed_value_type) {             \
      this->member = std::move(other);                                   \
    } else {                                                             \
      *this = TypedValue(std::move(other), memory_);                     \
    }                                                                    \
    return *this;                                                        \
  }

DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValue::TString, String, string_v)
DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValue::TVector, List, list_v)

TypedValue &TypedValue::operator=(std::vector<TypedValue> &&other) {
  if (type_ == Type::List) {
    list_v.reserve(other.size());
    list_v.assign(std::make_move_iterator(other.begin()),
                  std::make_move_iterator(other.end()));
  } else {
    *this = TypedValue(std::move(other), memory_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TMap, Map, map_v)

TypedValue &TypedValue::operator=(std::map<std::string, TypedValue> &&other) {
  if (type_ == Type::Map) {
    map_v.clear();
    for (auto &kv : other) map_v.emplace(kv.first, std::move(kv.second));
  } else {
    *this = TypedValue(std::move(other), memory_);
  }
  return *this;
}

DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(Path, Path, path_v)

#undef DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT

TypedValue &TypedValue::operator=(const TypedValue &other) {
  if (this != &other) {
    // NOTE: STL uses
    // std::allocator_traits<>::propagate_on_container_copy_assignment to
    // determine whether to take the allocator from `other`, or use the one in
    // `this`. Our utils::Allocator never propagates, so we use the allocator
    // from `this`.
    static_assert(!std::allocator_traits<utils::Allocator<TypedValue>>::
                      propagate_on_container_copy_assignment::value,
                  "Allocator propagation not implemented");
    DestroyValue();
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
        new (&string_v) TString(other.string_v, memory_);
        return *this;
      case TypedValue::Type::List:
        new (&list_v) TVector(other.list_v, memory_);
        return *this;
      case TypedValue::Type::Map:
        new (&map_v) TMap(other.map_v, memory_);
        return *this;
      case TypedValue::Type::Vertex:
        new (&vertex_v) VertexAccessor(other.vertex_v);
        return *this;
      case TypedValue::Type::Edge:
        new (&edge_v) EdgeAccessor(other.edge_v);
        return *this;
      case TypedValue::Type::Path:
        new (&path_v) Path(other.path_v, memory_);
        return *this;
    }
    LOG_FATAL("Unsupported TypedValue::Type");
  }
  return *this;
}

TypedValue &TypedValue::operator=(TypedValue &&other) noexcept(false) {
  if (this != &other) {
    DestroyValue();
    // NOTE: STL uses
    // std::allocator_traits<>::propagate_on_container_move_assignment to
    // determine whether to take the allocator from `other`, or use the one in
    // `this`. Our utils::Allocator never propagates, so we use the allocator
    // from `this`.
    static_assert(!std::allocator_traits<utils::Allocator<TypedValue>>::
                      propagate_on_container_move_assignment::value,
                  "Allocator propagation not implemented");
    type_ = other.type_;
    switch (other.type_) {
      case TypedValue::Type::Null:
        break;
      case TypedValue::Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case TypedValue::Type::Int:
        this->int_v = other.int_v;
        break;
      case TypedValue::Type::Double:
        this->double_v = other.double_v;
        break;
      case TypedValue::Type::String:
        new (&string_v) TString(std::move(other.string_v), memory_);
        break;
      case TypedValue::Type::List:
        new (&list_v) TVector(std::move(other.list_v), memory_);
        break;
      case TypedValue::Type::Map:
        new (&map_v) TMap(std::move(other.map_v), memory_);
        break;
      case TypedValue::Type::Vertex:
        new (&vertex_v) VertexAccessor(std::move(other.vertex_v));
        break;
      case TypedValue::Type::Edge:
        new (&edge_v) EdgeAccessor(std::move(other.edge_v));
        break;
      case TypedValue::Type::Path:
        new (&path_v) Path(std::move(other.path_v), memory_);
        break;
    }
    other.DestroyValue();
  }
  return *this;
}

void TypedValue::DestroyValue() {
  switch (type_) {
      // destructor for primitive types does nothing
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
      break;

      // we need to call destructors for non primitive types since we used
      // placement new
    case Type::String:
      string_v.~TString();
      break;
    case Type::List:
      list_v.~TVector();
      break;
    case Type::Map:
      map_v.~TMap();
      break;
    case Type::Vertex:
      vertex_v.~VertexAccessor();
      break;
    case Type::Edge:
      edge_v.~EdgeAccessor();
      break;
    case Type::Path:
      path_v.~Path();
      break;
  }

  type_ = TypedValue::Type::Null;
}

TypedValue::~TypedValue() { DestroyValue(); }

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
      return (double)value.ValueInt();
    case TypedValue::Type::Double:
      return value.ValueDouble();
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

  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());

  if (a.IsString() || b.IsString()) {
    if (a.type() != b.type()) {
      throw TypedValueException("Invalid 'less' operand types({} + {})",
                                a.type(), b.type());
    } else {
      return TypedValue(a.ValueString() < b.ValueString(),
                        a.GetMemoryResource());
    }
  }

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) < ToDouble(b), a.GetMemoryResource());
  } else {
    return TypedValue(a.ValueInt() < b.ValueInt(), a.GetMemoryResource());
  }
}

TypedValue operator==(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());

  // check we have values that can be compared
  // this means that either they're the same type, or (int, double) combo
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())))
    return TypedValue(false, a.GetMemoryResource());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return TypedValue(a.ValueBool() == b.ValueBool(), a.GetMemoryResource());
    case TypedValue::Type::Int:
      if (b.IsDouble())
        return TypedValue(ToDouble(a) == ToDouble(b), a.GetMemoryResource());
      else
        return TypedValue(a.ValueInt() == b.ValueInt(), a.GetMemoryResource());
    case TypedValue::Type::Double:
      return TypedValue(ToDouble(a) == ToDouble(b), a.GetMemoryResource());
    case TypedValue::Type::String:
      return TypedValue(a.ValueString() == b.ValueString(),
                        a.GetMemoryResource());
    case TypedValue::Type::Vertex:
      return TypedValue(a.ValueVertex() == b.ValueVertex(),
                        a.GetMemoryResource());
    case TypedValue::Type::Edge:
      return TypedValue(a.ValueEdge() == b.ValueEdge(), a.GetMemoryResource());
    case TypedValue::Type::List: {
      // We are not compatible with neo4j at this point. In neo4j 2 = [2]
      // compares
      // to true. That is not the end of unselfishness of developers at neo4j so
      // they allow us to use as many braces as we want to get to the truth in
      // list comparison, so [[2]] = [[[[[[2]]]]]] compares to true in neo4j as
      // well. Because, why not?
      // At memgraph we prefer sanity so [1,2] = [1,2] compares to true and
      // 2 = [2] compares to false.
      const auto &list_a = a.ValueList();
      const auto &list_b = b.ValueList();
      if (list_a.size() != list_b.size())
        return TypedValue(false, a.GetMemoryResource());
      // two arrays are considered equal (by neo) if all their
      // elements are bool-equal. this means that:
      //    [1] == [null] -> false
      //    [null] == [null] -> true
      // in that sense array-comparison never results in Null
      return TypedValue(std::equal(list_a.begin(), list_a.end(), list_b.begin(),
                                   TypedValue::BoolEqual{}),
                        a.GetMemoryResource());
    }
    case TypedValue::Type::Map: {
      const auto &map_a = a.ValueMap();
      const auto &map_b = b.ValueMap();
      if (map_a.size() != map_b.size())
        return TypedValue(false, a.GetMemoryResource());
      for (const auto &kv_a : map_a) {
        auto found_b_it = map_b.find(kv_a.first);
        if (found_b_it == map_b.end())
          return TypedValue(false, a.GetMemoryResource());
        TypedValue comparison = kv_a.second == found_b_it->second;
        if (comparison.IsNull() || !comparison.ValueBool())
          return TypedValue(false, a.GetMemoryResource());
      }
      return TypedValue(true, a.GetMemoryResource());
    }
    case TypedValue::Type::Path:
      return TypedValue(a.ValuePath() == b.ValuePath(), a.GetMemoryResource());
    default:
      LOG_FATAL("Unhandled comparison for types");
  }
}

TypedValue operator!(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.GetMemoryResource());
  if (a.IsBool()) return TypedValue(!a.ValueBool(), a.GetMemoryResource());
  throw TypedValueException("Invalid logical not operand type (!{})", a.type());
}

/**
 * Turns a numeric or string value into a string.
 *
 * @param value a value.
 * @return A string.
 */
std::string ValueToString(const TypedValue &value) {
  // TODO: Should this allocate a string through value.GetMemoryResource()?
  if (value.IsString()) return std::string(value.ValueString());
  if (value.IsInt()) return std::to_string(value.ValueInt());
  if (value.IsDouble()) return fmt::format("{}", value.ValueDouble());
  // unsupported situations
  throw TypedValueException(
      "Unsupported TypedValue::Type conversion to string");
}

TypedValue operator-(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.GetMemoryResource());
  if (a.IsInt()) return TypedValue(-a.ValueInt(), a.GetMemoryResource());
  if (a.IsDouble()) return TypedValue(-a.ValueDouble(), a.GetMemoryResource());
  throw TypedValueException("Invalid unary minus operand type (-{})", a.type());
}

TypedValue operator+(const TypedValue &a) {
  if (a.IsNull()) return TypedValue(a.GetMemoryResource());
  if (a.IsInt()) return TypedValue(+a.ValueInt(), a.GetMemoryResource());
  if (a.IsDouble()) return TypedValue(+a.ValueDouble(), a.GetMemoryResource());
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
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());

  if (a.IsList() || b.IsList()) {
    TypedValue::TVector list(a.GetMemoryResource());
    auto append_list = [&list](const TypedValue &v) {
      if (v.IsList()) {
        auto list2 = v.ValueList();
        list.insert(list.end(), list2.begin(), list2.end());
      } else {
        list.push_back(v);
      }
    };
    append_list(a);
    append_list(b);
    return TypedValue(std::move(list), a.GetMemoryResource());
  }

  EnsureArithmeticallyOk(a, b, true, "addition");
  // no more Bool nor Null, summing works on anything from here onward

  if (a.IsString() || b.IsString())
    return TypedValue(ValueToString(a) + ValueToString(b),
                      a.GetMemoryResource());

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) + ToDouble(b), a.GetMemoryResource());
  } else {
    return TypedValue(a.ValueInt() + b.ValueInt(), a.GetMemoryResource());
  }
}

TypedValue operator-(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  EnsureArithmeticallyOk(a, b, false, "subtraction");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) - ToDouble(b), a.GetMemoryResource());
  } else {
    return TypedValue(a.ValueInt() - b.ValueInt(), a.GetMemoryResource());
  }
}

TypedValue operator/(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  EnsureArithmeticallyOk(a, b, false, "division");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) / ToDouble(b), a.GetMemoryResource());
  } else {
    if (b.ValueInt() == 0LL) throw TypedValueException("Division by zero");
    return TypedValue(a.ValueInt() / b.ValueInt(), a.GetMemoryResource());
  }
}

TypedValue operator*(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  EnsureArithmeticallyOk(a, b, false, "multiplication");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(ToDouble(a) * ToDouble(b), a.GetMemoryResource());
  } else {
    return TypedValue(a.ValueInt() * b.ValueInt(), a.GetMemoryResource());
  }
}

TypedValue operator%(const TypedValue &a, const TypedValue &b) {
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  EnsureArithmeticallyOk(a, b, false, "modulo");

  // at this point we only have int and double
  if (a.IsDouble() || b.IsDouble()) {
    return TypedValue(static_cast<double>(fmod(ToDouble(a), ToDouble(b))),
                      a.GetMemoryResource());
  } else {
    if (b.ValueInt() == 0LL) throw TypedValueException("Mod with zero");
    return TypedValue(a.ValueInt() % b.ValueInt(), a.GetMemoryResource());
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
  if (a.IsBool() && !a.ValueBool())
    return TypedValue(false, a.GetMemoryResource());
  if (b.IsBool() && !b.ValueBool())
    return TypedValue(false, a.GetMemoryResource());
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  // neither is false, neither is null, thus both are true
  return TypedValue(true, a.GetMemoryResource());
}

TypedValue operator||(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical OR");
  // at this point we only have null and bool
  // if either operand is true, the result is true
  if (a.IsBool() && a.ValueBool())
    return TypedValue(true, a.GetMemoryResource());
  if (b.IsBool() && b.ValueBool())
    return TypedValue(true, a.GetMemoryResource());
  if (a.IsNull() || b.IsNull()) return TypedValue(a.GetMemoryResource());
  // neither is true, neither is null, thus both are false
  return TypedValue(false, a.GetMemoryResource());
}

TypedValue operator^(const TypedValue &a, const TypedValue &b) {
  EnsureLogicallyOk(a, b, "logical XOR");
  // at this point we only have null and bool
  if (a.IsNull() || b.IsNull())
    return TypedValue(a.GetMemoryResource());
  else
    return TypedValue(static_cast<bool>(a.ValueBool() ^ b.ValueBool()),
                      a.GetMemoryResource());
}

bool TypedValue::BoolEqual::operator()(const TypedValue &lhs,
                                       const TypedValue &rhs) const {
  if (lhs.IsNull() && rhs.IsNull()) return true;
  TypedValue equality_result = lhs == rhs;
  switch (equality_result.type()) {
    case TypedValue::Type::Bool:
      return equality_result.ValueBool();
    case TypedValue::Type::Null:
      return false;
    default:
      LOG_FATAL(
          "Equality between two TypedValues resulted in something other "
          "than Null or bool");
  }
}

size_t TypedValue::Hash::operator()(const TypedValue &value) const {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return 31;
    case TypedValue::Type::Bool:
      return std::hash<bool>{}(value.ValueBool());
    case TypedValue::Type::Int:
      // we cast int to double for hashing purposes
      // to be consistent with TypedValue equality
      // in which (2.0 == 2) returns true
      return std::hash<double>{}((double)value.ValueInt());
    case TypedValue::Type::Double:
      return std::hash<double>{}(value.ValueDouble());
    case TypedValue::Type::String:
      return std::hash<std::string_view>{}(value.ValueString());
    case TypedValue::Type::List: {
      return utils::FnvCollection<TypedValue::TVector, TypedValue, Hash>{}(
          value.ValueList());
    }
    case TypedValue::Type::Map: {
      size_t hash = 6543457;
      for (const auto &kv : value.ValueMap()) {
        hash ^= std::hash<std::string_view>{}(kv.first);
        hash ^= this->operator()(kv.second);
      }
      return hash;
    }
    case TypedValue::Type::Vertex:
      return value.ValueVertex().Gid().AsUint();
    case TypedValue::Type::Edge:
      return value.ValueEdge().Gid().AsUint();
    case TypedValue::Type::Path: {
      const auto &vertices = value.ValuePath().vertices();
      const auto &edges = value.ValuePath().edges();
      return utils::FnvCollection<decltype(vertices), VertexAccessor>{}(
                 vertices) ^
             utils::FnvCollection<decltype(edges), EdgeAccessor>{}(edges);
    }
  }
  LOG_FATAL("Unhandled TypedValue.type() in hash function");
}

}  // namespace query
