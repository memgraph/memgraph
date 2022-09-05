// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <fmt/format.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/temporal.hpp"

namespace memgraph::expr {

/**
 * An exception raised by the TypedValue system. Typically when
 * trying to perform operations (such as addition) on TypedValues
 * of incompatible Types.
 */
class TypedValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

// TODO: Neo4j does overflow checking. Should we also implement it?
/**
 * Stores a query runtime value and its type.
 *
 * Values can be of a number of predefined types that are enumerated in
 * TypedValueT::Type. Each such type corresponds to exactly one C++ type.
 *
 * Non-primitive value types perform additional memory allocations. To tune the
 * allocation scheme, each TypedValue stores a MemoryResource for said
 * allocations. When copying and moving TypedValue instances, take care that the
 * appropriate MemoryResource is used.
 */
template <typename TVertexAccessor, typename TEdgeAccessor, typename TPathT>
class TypedValueT {
 public:
  /** Custom TypedValue equality function that returns a bool
   * (as opposed to returning TypedValue as the default equality does).
   * This implementation treats two nulls as being equal and null
   * not being equal to everything else.
   */
  struct BoolEqual {
    bool operator()(const TypedValueT &lhs, const TypedValueT &rhs) const {
      if (lhs.IsNull() && rhs.IsNull()) return true;
      TypedValueT equality_result = lhs == rhs;
      switch (equality_result.type()) {
        case TypedValueT::Type::Bool:
          return equality_result.ValueBool();
        case TypedValueT::Type::Null:
          return false;
        default:
          LOG_FATAL(
              "Equality between two TypedValues resulted in something other "
              "than Null or bool");
      }
    }
  };

  /** Hash operator for TypedValue.
   *
   * Not injecting into std
   * due to linking problems. If the implementation is in this header,
   * then it implicitly instantiates TypedValue::Value<T> before
   * explicit instantiation in .cpp file. If the implementation is in
   * the .cpp file, it won't link.
   * TODO: No longer the case as Value<T> was removed.
   */
  struct Hash {
    size_t operator()(const TypedValueT &value) const {
      switch (value.type()) {
        case TypedValueT::Type::Null:
          return 31;
        case TypedValueT::Type::Bool:
          return std::hash<bool>{}(value.ValueBool());
        case TypedValueT::Type::Int:
          // we cast int to double for hashing purposes
          // to be consistent with TypedValueT equality
          // in which (2.0 == 2) returns true
          return std::hash<double>{}((double)value.ValueInt());
        case TypedValueT::Type::Double:
          return std::hash<double>{}(value.ValueDouble());
        case TypedValueT::Type::String:
          return std::hash<std::string_view>{}(value.ValueString());
        case TypedValueT::Type::List: {
          return utils::FnvCollection<TypedValueT::TVector, TypedValueT, Hash>{}(value.ValueList());
        }
        case TypedValueT::Type::Map: {
          size_t hash = 6543457;
          for (const auto &kv : value.ValueMap()) {
            hash ^= std::hash<std::string_view>{}(kv.first);
            hash ^= this->operator()(kv.second);
          }
          return hash;
        }
        case TypedValueT::Type::Vertex:
        case TypedValueT::Type::Edge: {
          return 0;
        }
        case TypedValueT::Type::Path: {
          const auto &vertices = value.ValuePath().vertices();
          const auto &edges = value.ValuePath().edges();
          return utils::FnvCollection<decltype(vertices), TVertexAccessor>{}(vertices) ^
                 utils::FnvCollection<decltype(edges), TEdgeAccessor>{}(edges);
        }
        case TypedValueT::Type::Date:
          return utils::DateHash{}(value.ValueDate());
        case TypedValueT::Type::LocalTime:
          return utils::LocalTimeHash{}(value.ValueLocalTime());
        case TypedValueT::Type::LocalDateTime:
          return utils::LocalDateTimeHash{}(value.ValueLocalDateTime());
        case TypedValueT::Type::Duration:
          return utils::DurationHash{}(value.ValueDuration());
          break;
      }
      LOG_FATAL("Unhandled TypedValue.type() in hash function");
    }
  };

  /** A value type. Each type corresponds to exactly one C++ type */
  enum class Type : unsigned {
    Null,
    Bool,
    Int,
    Double,
    String,
    List,
    Map,
    Vertex,
    Edge,
    Path,
    Date,
    LocalTime,
    LocalDateTime,
    Duration
  };

  // TypedValue at this exact moment of compilation is an incomplete type, and
  // the standard says that instantiating a container with an incomplete type
  // invokes undefined behaviour. The libstdc++-8.3.0 we are using supports
  // std::map with incomplete type, but this is still murky territory. Note that
  // since C++17, std::vector is explicitly said to support incomplete types.

  using TString = utils::pmr::string;
  using TVector = utils::pmr::vector<TypedValueT>;
  using TMap = utils::pmr::map<utils::pmr::string, TypedValueT>;

  /** Allocator type so that STL containers are aware that we need one */
  using allocator_type = utils::Allocator<TypedValueT>;

  /** Construct a Null value with default utils::NewDeleteResource(). */
  TypedValueT() : type_(Type::Null) {}

  /** Construct a Null value with given utils::MemoryResource. */
  explicit TypedValueT(utils::MemoryResource *memory) : memory_(memory), type_(Type::Null) {}

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::select_on_container_copy_construction(other.memory_).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  TypedValueT(const TypedValueT &other)
      : TypedValueT(other, std::allocator_traits<utils::Allocator<TypedValueT>>::select_on_container_copy_construction(
                               other.memory_)
                               .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  TypedValueT(const TypedValueT &other, utils::MemoryResource *memory) : memory_(memory), type_(other.type_) {
    switch (other.type_) {
      case TypedValueT::Type::Null:
        return;
      case TypedValueT::Type::Bool:
        this->bool_v = other.bool_v;
        return;
      case Type::Int:
        this->int_v = other.int_v;
        return;
      case Type::Double:
        this->double_v = other.double_v;
        return;
      case TypedValueT::Type::String:
        new (&string_v) TString(other.string_v, memory_);
        return;
      case Type::List:
        new (&list_v) TVector(other.list_v, memory_);
        return;
      case Type::Map:
        new (&map_v) TMap(other.map_v, memory_);
        return;
      case Type::Vertex:
        new (&vertex_v) TVertexAccessor(other.vertex_v);
        return;
      case Type::Edge:
        new (&edge_v) TEdgeAccessor(other.edge_v);
        return;
      case Type::Path:
        new (&path_v) TPathT(other.path_v, memory_);
        return;
      case Type::Date:
        new (&date_v) utils::Date(other.date_v);
        return;
      case Type::LocalTime:
        new (&local_time_v) utils::LocalTime(other.local_time_v);
        return;
      case Type::LocalDateTime:
        new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
        return;
      case Type::Duration:
        new (&duration_v) utils::Duration(other.duration_v);
        return;
    }
    LOG_FATAL("Unsupported TypedValueT::Type");
  }

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * set to Null.
   */
  TypedValueT(TypedValueT &&other) noexcept : TypedValueT(std::move(other), other.memory_) {}

  /**
   * Construct with the value of other, but use the given utils::MemoryResource.
   * After the move, other will be set to Null.
   * If `*memory != *other.GetMemoryResource()`, then a copy is made instead of
   * a move.
   */
  TypedValueT(TypedValueT &&other, utils::MemoryResource *memory) : memory_(memory), type_(other.type_) {
    switch (other.type_) {
      case TypedValueT::Type::Null:
        break;
      case TypedValueT::Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case TypedValueT::Type::String:
        new (&string_v) TString(std::move(other.string_v), memory_);
        break;
      case Type::List:
        new (&list_v) TVector(std::move(other.list_v), memory_);
        break;
      case Type::Map:
        new (&map_v) TMap(std::move(other.map_v), memory_);
        break;
      case Type::Vertex:
        new (&vertex_v) TVertexAccessor(std::move(other.vertex_v));
        break;
      case Type::Edge:
        new (&edge_v) TEdgeAccessor(std::move(other.edge_v));
        break;
      case Type::Path:
        new (&path_v) TPathT(std::move(other.path_v), memory_);
        break;
      case Type::Date:
        new (&date_v) utils::Date(other.date_v);
        break;
      case Type::LocalTime:
        new (&local_time_v) utils::LocalTime(other.local_time_v);
        break;
      case Type::LocalDateTime:
        new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
        break;
      case Type::Duration:
        new (&duration_v) utils::Duration(other.duration_v);
        break;
    }
    other.DestroyValue();
  }

  explicit TypedValueT(bool value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Bool) {
    bool_v = value;
  }

  explicit TypedValueT(int value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Int) {
    int_v = value;
  }

  explicit TypedValueT(int64_t value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Int) {
    int_v = value;
  }

  explicit TypedValueT(double value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Double) {
    double_v = value;
  }

  explicit TypedValueT(const utils::Date &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Date) {
    date_v = value;
  }

  explicit TypedValueT(const utils::LocalTime &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::LocalTime) {
    local_time_v = value;
  }

  explicit TypedValueT(const utils::LocalDateTime &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::LocalDateTime) {
    local_date_time_v = value;
  }

  explicit TypedValueT(const utils::Duration &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Duration) {
    duration_v = value;
  }

  // copy constructors for non-primitive types
  explicit TypedValueT(const std::string &value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::String) {
    new (&string_v) TString(value, memory_);
  }

  explicit TypedValueT(const char *value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::String) {
    new (&string_v) TString(value, memory_);
  }

  explicit TypedValueT(const std::string_view value, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::String) {
    new (&string_v) TString(value, memory_);
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit TypedValueT(const TString &other)
      : TypedValueT(other, std::allocator_traits<utils::Allocator<TypedValueT>>::select_on_container_copy_construction(
                               other.get_allocator())
                               .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  TypedValueT(const TString &other, utils::MemoryResource *memory) : memory_(memory), type_(Type::String) {
    new (&string_v) TString(other, memory_);
  }

  /** Construct a copy using the given utils::MemoryResource */
  explicit TypedValueT(const std::vector<TypedValueT> &value,
                       utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::List) {
    new (&list_v) TVector(memory_);
    list_v.reserve(value.size());
    list_v.assign(value.begin(), value.end());
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit TypedValueT(const TVector &other)
      : TypedValueT(other, std::allocator_traits<utils::Allocator<TypedValueT>>::select_on_container_copy_construction(
                               other.get_allocator())
                               .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  TypedValueT(const TVector &value, utils::MemoryResource *memory) : memory_(memory), type_(Type::List) {
    new (&list_v) TVector(value, memory_);
  }

  /** Construct a copy using the given utils::MemoryResource */
  explicit TypedValueT(const std::map<std::string, TypedValueT> &value,
                       utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Map) {
    new (&map_v) TMap(memory_);
    for (const auto &kv : value) map_v.emplace(kv.first, kv.second);
  }

  /**
   * Construct a copy of other.
   * utils::MemoryResource is obtained by calling
   * std::allocator_traits<>::
   *     select_on_container_copy_construction(other.get_allocator()).
   * Since we use utils::Allocator, which does not propagate, this means that
   * memory_ will be the default utils::NewDeleteResource().
   */
  explicit TypedValueT(const TMap &other)
      : TypedValueT(other, std::allocator_traits<utils::Allocator<TypedValueT>>::select_on_container_copy_construction(
                               other.get_allocator())
                               .GetMemoryResource()) {}

  /** Construct a copy using the given utils::MemoryResource */
  TypedValueT(const TMap &value, utils::MemoryResource *memory) : memory_(memory), type_(Type::Map) {
    new (&map_v) TMap(value, memory_);
  }

  explicit TypedValueT(const TVertexAccessor &vertex, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Vertex) {
    new (&vertex_v) TVertexAccessor(vertex);
  }

  explicit TypedValueT(const TEdgeAccessor &edge, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Edge) {
    new (&edge_v) TEdgeAccessor(edge);
  }

  explicit TypedValueT(const TPathT &path, utils::MemoryResource *memory = utils::NewDeleteResource())
      : memory_(memory), type_(Type::Path) {
    new (&path_v) TPathT(path, memory_);
  }

  // move constructors for non-primitive types

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left in unspecified state.
   */
  explicit TypedValueT(TString &&other) noexcept
      : TypedValueT(std::move(other), other.get_allocator().GetMemoryResource()) {}

  /**
   * Construct with the value of other and use the given MemoryResource
   * After the move, other will be left in unspecified state.
   */
  TypedValueT(TString &&other, utils::MemoryResource *memory) : memory_(memory), type_(Type::String) {
    new (&string_v) TString(std::move(other), memory_);
  }

  /**
   * Perform an element-wise move using default utils::NewDeleteResource().
   * Other will be not be empty, though elements may be Null.
   */
  explicit TypedValueT(std::vector<TypedValueT> &&other) : TypedValueT(std::move(other), utils::NewDeleteResource()) {}

  /**
   * Perform an element-wise move of the other and use the given MemoryResource.
   * Other will be not be left empty, though elements may be Null.
   */
  TypedValueT(std::vector<TypedValueT> &&other, utils::MemoryResource *memory) : memory_(memory), type_(Type::List) {
    new (&list_v) TVector(memory_);
    list_v.reserve(other.size());
    // std::vector<TypedValueT> has std::allocator and there's no move
    // constructor for std::vector using different allocator types. Since
    // std::allocator is not propagated to elements, it is possible that some
    // TypedValueT elements have a MemoryResource that is the same as the one we
    // are given. In such a case we would like to move those TypedValueT
    // instances, so we use move_iterator.
    list_v.assign(std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()));
  }

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left empty.
   */
  explicit TypedValueT(TVector &&other) noexcept
      : TypedValueT(std::move(other), other.get_allocator().GetMemoryResource()) {}

  /**
   * Construct with the value of other and use the given MemoryResource.
   * If `other.get_allocator() != *memory`, this call will perform an
   * element-wise move and other is not guaranteed to be empty.
   */
  TypedValueT(TVector &&other, utils::MemoryResource *memory) : memory_(memory), type_(Type::List) {
    new (&list_v) TVector(std::move(other), memory_);
  }

  /**
   * Perform an element-wise move using default utils::NewDeleteResource().
   * Other will not be left empty, i.e. keys will exist but their values may
   * be Null.
   */
  explicit TypedValueT(std::map<std::string, TypedValueT> &&other)
      : TypedValueT(std::move(other), utils::NewDeleteResource()) {}

  /**
   * Perform an element-wise move using the given MemoryResource.
   * Other will not be left empty, i.e. keys will exist but their values may
   * be Null.
   */
  TypedValueT(std::map<std::string, TypedValueT> &&other, utils::MemoryResource *memory)
      : memory_(memory), type_(Type::Map) {
    new (&map_v) TMap(memory_);
    for (auto &kv : other) map_v.emplace(kv.first, std::move(kv.second));
  }

  /**
   * Construct with the value of other.
   * utils::MemoryResource is obtained from other. After the move, other will be
   * left empty.
   */
  explicit TypedValueT(TMap &&other) noexcept
      : TypedValueT(std::move(other), other.get_allocator().GetMemoryResource()) {}

  /**
   * Construct with the value of other and use the given MemoryResource.
   * If `other.get_allocator() != *memory`, this call will perform an
   * element-wise move and other is not guaranteed to be empty, i.e. keys may
   * exist but their values may be Null.
   */
  TypedValueT(TMap &&other, utils::MemoryResource *memory) : memory_(memory), type_(Type::Map) {
    new (&map_v) TMap(std::move(other), memory_);
  }

  explicit TypedValueT(TVertexAccessor &&vertex, utils::MemoryResource *memory = utils::NewDeleteResource()) noexcept
      : memory_(memory), type_(Type::Vertex) {
    new (&vertex_v) TVertexAccessor(std::move(vertex));
  }

  explicit TypedValueT(TEdgeAccessor &&edge, utils::MemoryResource *memory = utils::NewDeleteResource()) noexcept
      : memory_(memory), type_(Type::Edge) {
    new (&edge_v) TEdgeAccessor(std::move(edge));
  }

  /**
   * Construct with the value of path.
   * utils::MemoryResource is obtained from path. After the move, path will be
   * left empty.
   */
  explicit TypedValueT(TPathT &&path) noexcept : TypedValueT(std::move(path), path.GetMemoryResource()) {}

  /**
   * Construct with the value of path and use the given MemoryResource.
   * If `*path.GetMemoryResource() != *memory`, this call will perform an
   * element-wise move and path is not guaranteed to be empty.
   */
  TypedValueT(TPathT &&path, utils::MemoryResource *memory) : memory_(memory), type_(Type::Path) {
    new (&path_v) TPathT(std::move(path), memory_);
  }

  // copy assignment operators
#define DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(type_param, typed_value_type, member) \
  TypedValueT &operator=(type_param other) {                                     \
    if (this->type_ == TypedValueT::Type::typed_value_type) {                    \
      this->member = other;                                                      \
    } else {                                                                     \
      *this = TypedValueT(other, memory_);                                       \
    }                                                                            \
                                                                                 \
    return *this;                                                                \
  }

  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const char *, String, string_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int, Int, int_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(bool, Bool, bool_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(int64_t, Int, int_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(double, Double, double_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const std::string_view, String, string_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValueT::TVector &, List, list_v)

  TypedValueT &operator=(const std::vector<TypedValueT> &other) {
    if (type_ == Type::List) {
      list_v.reserve(other.size());
      list_v.assign(other.begin(), other.end());
    } else {
      *this = TypedValueT(other, memory_);
    }
    return *this;
  }

  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TypedValueT::TMap &, Map, map_v)

  TypedValueT &operator=(const std::map<std::string, TypedValueT> &other) {
    if (type_ == Type::Map) {
      map_v.clear();
      for (const auto &kv : other) map_v.emplace(kv.first, kv.second);
    } else {
      *this = TypedValueT(other, memory_);
    }
    return *this;
  }

  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TVertexAccessor &, Vertex, vertex_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TEdgeAccessor &, Edge, edge_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const TPathT &, Path, path_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::Date &, Date, date_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::LocalTime &, LocalTime, local_time_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::LocalDateTime &, LocalDateTime, local_date_time_v)
  DEFINE_TYPED_VALUE_COPY_ASSIGNMENT(const utils::Duration &, Duration, duration_v)

#undef DEFINE_TYPED_VALUE_COPY_ASSIGNMENT

  /** Move assign other, utils::MemoryResource of `this` is used. */
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage, bugprone-macro-parentheses)
#define DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(type_param, typed_value_type, member) \
  TypedValueT &operator=(type_param &&other) {                                   \
    if (this->type_ == TypedValueT::Type::typed_value_type) {                    \
      this->member = std::move(other);                                           \
    } else {                                                                     \
      *this = TypedValueT(std::move(other), memory_);                            \
    }                                                                            \
    return *this;                                                                \
  }

  DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValueT::TString, String, string_v)
  DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TypedValueT::TVector, List, list_v)

  TypedValueT &operator=(std::vector<TypedValueT> &&other) {
    if (type_ == Type::List) {
      list_v.reserve(other.size());
      list_v.assign(std::make_move_iterator(other.begin()), std::make_move_iterator(other.end()));
    } else {
      *this = TypedValueT(std::move(other), memory_);
    }
    return *this;
  }

  DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TMap, Map, map_v)

  TypedValueT &operator=(std::map<std::string, TypedValueT> &&other) {
    if (type_ == Type::Map) {
      map_v.clear();
      for (auto &kv : other) map_v.emplace(kv.first, std::move(kv.second));
    } else {
      *this = TypedValueT(std::move(other), memory_);
    }
    return *this;
  }

  DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT(TPathT, Path, path_v)

#undef DEFINE_TYPED_VALUE_MOVE_ASSIGNMENT

  TypedValueT &operator=(const TypedValueT &other) {
    if (this != &other) {
      // NOTE: STL uses
      // std::allocator_traits<>::propagate_on_container_copy_assignment to
      // determine whether to take the allocator from `other`, or use the one in
      // `this`. Our utils::Allocator never propagates, so we use the allocator
      // from `this`.
      static_assert(
          !std::allocator_traits<utils::Allocator<TypedValueT>>::propagate_on_container_copy_assignment::value,
          "Allocator propagation not implemented");
      DestroyValue();
      type_ = other.type_;
      switch (other.type_) {
        case TypedValueT::Type::Null:
          return *this;
        case TypedValueT::Type::Bool:
          this->bool_v = other.bool_v;
          return *this;
        case TypedValueT::Type::Int:
          this->int_v = other.int_v;
          return *this;
        case TypedValueT::Type::Double:
          this->double_v = other.double_v;
          return *this;
        case TypedValueT::Type::String:
          new (&string_v) TString(other.string_v, memory_);
          return *this;
        case TypedValueT::Type::List:
          new (&list_v) TVector(other.list_v, memory_);
          return *this;
        case TypedValueT::Type::Map:
          new (&map_v) TMap(other.map_v, memory_);
          return *this;
        case TypedValueT::Type::Vertex:
          new (&vertex_v) TVertexAccessor(other.vertex_v);
          return *this;
        case TypedValueT::Type::Edge:
          new (&edge_v) TEdgeAccessor(other.edge_v);
          return *this;
        case TypedValueT::Type::Path:
          new (&path_v) TPathT(other.path_v, memory_);
          return *this;
        case Type::Date:
          new (&date_v) utils::Date(other.date_v);
          return *this;
        case Type::LocalTime:
          new (&local_time_v) utils::LocalTime(other.local_time_v);
          return *this;
        case Type::LocalDateTime:
          new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
          return *this;
        case Type::Duration:
          new (&duration_v) utils::Duration(other.duration_v);
          return *this;
      }
      LOG_FATAL("Unsupported TypedValueT::Type");
    }
    return *this;
  }

  TypedValueT &operator=(TypedValueT &&other) noexcept(false) {
    if (this != &other) {
      DestroyValue();
      // NOTE: STL uses
      // std::allocator_traits<>::propagate_on_container_move_assignment to
      // determine whether to take the allocator from `other`, or use the one in
      // `this`. Our utils::Allocator never propagates, so we use the allocator
      // from `this`.
      static_assert(
          !std::allocator_traits<utils::Allocator<TypedValueT>>::propagate_on_container_move_assignment::value,
          "Allocator propagation not implemented");
      type_ = other.type_;
      switch (other.type_) {
        case TypedValueT::Type::Null:
          break;
        case TypedValueT::Type::Bool:
          this->bool_v = other.bool_v;
          break;
        case TypedValueT::Type::Int:
          this->int_v = other.int_v;
          break;
        case TypedValueT::Type::Double:
          this->double_v = other.double_v;
          break;
        case TypedValueT::Type::String:
          new (&string_v) TString(std::move(other.string_v), memory_);
          break;
        case TypedValueT::Type::List:
          new (&list_v) TVector(std::move(other.list_v), memory_);
          break;
        case TypedValueT::Type::Map:
          new (&map_v) TMap(std::move(other.map_v), memory_);
          break;
        case TypedValueT::Type::Vertex:
          new (&vertex_v) TVertexAccessor(std::move(other.vertex_v));
          break;
        case TypedValueT::Type::Edge:
          new (&edge_v) TEdgeAccessor(std::move(other.edge_v));
          break;
        case TypedValueT::Type::Path:
          new (&path_v) TPathT(std::move(other.path_v), memory_);
          break;
        case Type::Date:
          new (&date_v) utils::Date(other.date_v);
          break;
        case Type::LocalTime:
          new (&local_time_v) utils::LocalTime(other.local_time_v);
          break;
        case Type::LocalDateTime:
          new (&local_date_time_v) utils::LocalDateTime(other.local_date_time_v);
          break;
        case Type::Duration:
          new (&duration_v) utils::Duration(other.duration_v);
          break;
      }
      other.DestroyValue();
    }
    return *this;
  }

  ~TypedValueT() { DestroyValue(); }

  Type type() const { return type_; }

  // TODO consider adding getters for primitives by value (and not by ref)

  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define DEFINE_VALUE_AND_TYPE_GETTERS(type_param, type_enum, field)                              \
  type_param &Value##type_enum() {                                                               \
    if (type_ != Type::type_enum)                                                                \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
                                                                                                 \
  const type_param &Value##type_enum() const {                                                   \
    if (type_ != Type::type_enum)                                                                \
      throw TypedValueException("TypedValue is of type '{}', not '{}'", type_, Type::type_enum); \
    return field;                                                                                \
  }                                                                                              \
                                                                                                 \
  bool Is##type_enum() const { return type_ == Type::type_enum; }

  DEFINE_VALUE_AND_TYPE_GETTERS(bool, Bool, bool_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(int64_t, Int, int_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(double, Double, double_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TString, String, string_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TVector, List, list_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TMap, Map, map_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TVertexAccessor, Vertex, vertex_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TEdgeAccessor, Edge, edge_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(TPathT, Path, path_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(utils::Date, Date, date_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(utils::LocalTime, LocalTime, local_time_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(utils::LocalDateTime, LocalDateTime, local_date_time_v)
  DEFINE_VALUE_AND_TYPE_GETTERS(utils::Duration, Duration, duration_v)

#undef DEFINE_VALUE_AND_TYPE_GETTERS

  /**  Checks if value is a TypedValueT::Null. */
  bool IsNull() const { return type_ == Type::Null; }

  /** Convenience function for checking if this TypedValueT is either
   * an integer or double */
  bool IsNumeric() const { return IsInt() || IsDouble(); }

  utils::MemoryResource *GetMemoryResource() const { return memory_; }

  // binary bool operators

  /**
   * Perform logical 'and' on TypedValues.
   *
   * If any of the values is false, return false. Otherwise checks if any value is
   * Null and return Null. All other cases return true. The resulting value uses
   * the same MemoryResource as the left hand side arguments.
   *
   * @throw TypedValueException if arguments are not boolean or Null.
   */
  friend TypedValueT operator&&(const TypedValueT &a, const TypedValueT &b) {
    EnsureLogicallyOk(a, b, "logical AND");
    // at this point we only have null and bool
    // if either operand is false, the result is false
    if (a.IsBool() && !a.ValueBool()) return TypedValueT(false, a.GetMemoryResource());
    if (b.IsBool() && !b.ValueBool()) return TypedValueT(false, a.GetMemoryResource());
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    // neither is false, neither is null, thus both are true
    return TypedValueT(true, a.GetMemoryResource());
  }

  /**
   * Perform logical 'or' on TypedValues.
   *
   * If any of the values is true, return true. Otherwise checks if any value is
   * Null and return Null. All other cases return false. The resulting value uses
   * the same MemoryResource as the left hand side arguments.
   *
   * @throw TypedValueException if arguments are not boolean or Null.
   */
  friend TypedValueT operator||(const TypedValueT &a, const TypedValueT &b) {
    EnsureLogicallyOk(a, b, "logical OR");
    // at this point we only have null and bool
    // if either operand is true, the result is true
    if (a.IsBool() && a.ValueBool()) return TypedValueT(true, a.GetMemoryResource());
    if (b.IsBool() && b.ValueBool()) return TypedValueT(true, a.GetMemoryResource());
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    // neither is true, neither is null, thus both are false
    return TypedValueT(false, a.GetMemoryResource());
  }

  /**
   * Logically negate a TypedValueT.
   *
   * Negating Null value returns Null. Values other than null raise an exception.
   * The resulting value uses the same MemoryResource as the argument.
   *
   * @throw TypedValueException if TypedValueT is not a boolean or Null.
   */
  friend TypedValueT operator!(const TypedValueT &a) {
    if (a.IsNull()) return TypedValueT(a.GetMemoryResource());
    if (a.IsBool()) return TypedValueT(!a.ValueBool(), a.GetMemoryResource());
    throw TypedValueException("Invalid logical not operand type (!{})", a.type());
  }

  // binary bool xor, not power operator
  // Be careful: since ^ is binary operator and || and && are logical operators
  // they have different priority in c++.
  friend TypedValueT operator^(const TypedValueT &a, const TypedValueT &b) {
    EnsureLogicallyOk(a, b, "logical XOR");
    // at this point we only have null and bool
    if (a.IsNull() || b.IsNull()) {
      return TypedValueT(a.GetMemoryResource());
    }

    return TypedValueT(static_cast<bool>(a.ValueBool() ^ b.ValueBool()), a.GetMemoryResource());
  }

  // comparison operators

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * Since each TypedValueT may have a different MemoryResource for allocations,
   * the results is allocated using MemoryResource obtained from the left hand
   * side.
   */
  friend TypedValueT operator==(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());

    // check we have values that can be compared
    // this means that either they're the same type, or (int, double) combo
    if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric()))) return TypedValueT(false, a.GetMemoryResource());

    switch (a.type()) {
      case TypedValueT::Type::Bool:
        return TypedValueT(a.ValueBool() == b.ValueBool(), a.GetMemoryResource());
      case TypedValueT::Type::Int:
        if (b.IsDouble())
          return TypedValueT(ToDouble(a) == ToDouble(b), a.GetMemoryResource());
        else
          return TypedValueT(a.ValueInt() == b.ValueInt(), a.GetMemoryResource());
      case TypedValueT::Type::Double:
        return TypedValueT(ToDouble(a) == ToDouble(b), a.GetMemoryResource());
      case TypedValueT::Type::String:
        return TypedValueT(a.ValueString() == b.ValueString(), a.GetMemoryResource());
      case TypedValueT::Type::Vertex:
        return TypedValueT(a.ValueVertex() == b.ValueVertex(), a.GetMemoryResource());
      case TypedValueT::Type::Edge:
        return TypedValueT(a.ValueEdge() == b.ValueEdge(), a.GetMemoryResource());
      case TypedValueT::Type::List: {
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
        if (list_a.size() != list_b.size()) return TypedValueT(false, a.GetMemoryResource());
        // two arrays are considered equal (by neo) if all their
        // elements are bool-equal. this means that:
        //    [1] == [null] -> false
        //    [null] == [null] -> true
        // in that sense array-comparison never results in Null
        return TypedValueT(std::equal(list_a.begin(), list_a.end(), list_b.begin(), TypedValueT::BoolEqual{}),
                           a.GetMemoryResource());
      }
      case TypedValueT::Type::Map: {
        const auto &map_a = a.ValueMap();
        const auto &map_b = b.ValueMap();
        if (map_a.size() != map_b.size()) return TypedValueT(false, a.GetMemoryResource());
        for (const auto &kv_a : map_a) {
          auto found_b_it = map_b.find(kv_a.first);
          if (found_b_it == map_b.end()) return TypedValueT(false, a.GetMemoryResource());
          TypedValueT comparison = kv_a.second == found_b_it->second;
          if (comparison.IsNull() || !comparison.ValueBool()) return TypedValueT(false, a.GetMemoryResource());
        }
        return TypedValueT(true, a.GetMemoryResource());
      }
      case TypedValueT::Type::Path:
        return TypedValueT(a.ValuePath() == b.ValuePath(), a.GetMemoryResource());
      case TypedValueT::Type::Date:
        return TypedValueT(a.ValueDate() == b.ValueDate(), a.GetMemoryResource());
      case TypedValueT::Type::LocalTime:
        return TypedValueT(a.ValueLocalTime() == b.ValueLocalTime(), a.GetMemoryResource());
      case TypedValueT::Type::LocalDateTime:
        return TypedValueT(a.ValueLocalDateTime() == b.ValueLocalDateTime(), a.GetMemoryResource());
      case TypedValueT::Type::Duration:
        return TypedValueT(a.ValueDuration() == b.ValueDuration(), a.GetMemoryResource());
      default:
        LOG_FATAL("Unhandled comparison for types");
    }
  }

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * Since each TypedValueT may have a different MemoryResource for allocations,
   * the results is allocated using MemoryResource obtained from the left hand
   * side.
   */
  friend TypedValueT operator!=(const TypedValueT &a, const TypedValueT &b) { return !(a == b); }

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values cannot be compared, i.e. they are
   *        not either Null, numeric or a character string type.
   */
  friend TypedValueT operator<(const TypedValueT &a, const TypedValueT &b) {
    auto is_legal = [](TypedValueT::Type type) {
      switch (type) {
        case TypedValueT::Type::Null:
        case TypedValueT::Type::Int:
        case TypedValueT::Type::Double:
        case TypedValueT::Type::String:
        case TypedValueT::Type::Date:
        case TypedValueT::Type::LocalTime:
        case TypedValueT::Type::LocalDateTime:
        case TypedValueT::Type::Duration:
          return true;
        default:
          return false;
      }
    };
    if (!is_legal(a.type()) || !is_legal(b.type()))
      throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(), b.type());

    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());

    if (a.IsString() || b.IsString()) {
      if (a.type() != b.type()) {
        throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(), b.type());
      }
      return TypedValueT(a.ValueString() < b.ValueString(), a.GetMemoryResource());
    }

    if (IsTemporalType(a.type()) || IsTemporalType(b.type())) {
      if (a.type() != b.type()) {
        throw TypedValueException("Invalid 'less' operand types({} + {})", a.type(), b.type());
      }

      switch (a.type()) {
        case TypedValueT::Type::Date:
          // NOLINTNEXTLINE(modernize-use-nullptr)
          return TypedValueT(a.ValueDate() < b.ValueDate(), a.GetMemoryResource());
        case TypedValueT::Type::LocalTime:
          // NOLINTNEXTLINE(modernize-use-nullptr)
          return TypedValueT(a.ValueLocalTime() < b.ValueLocalTime(), a.GetMemoryResource());
        case TypedValueT::Type::LocalDateTime:
          // NOLINTNEXTLINE(modernize-use-nullptr)
          return TypedValueT(a.ValueLocalDateTime() < b.ValueLocalDateTime(), a.GetMemoryResource());
        case TypedValueT::Type::Duration:
          // NOLINTNEXTLINE(modernize-use-nullptr)
          return TypedValueT(a.ValueDuration() < b.ValueDuration(), a.GetMemoryResource());
        default:
          LOG_FATAL("Invalid temporal type");
      }
    }

    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(ToDouble(a) < ToDouble(b), a.GetMemoryResource());
    }
    return TypedValueT(a.ValueInt() < b.ValueInt(), a.GetMemoryResource());
  }

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values cannot be compared, i.e. they are
   *        not either Null, numeric or a character string type.
   */
  friend TypedValueT operator<=(const TypedValueT &a, const TypedValueT &b) { return a < b || a == b; }

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values cannot be compared, i.e. they are
   *        not either Null, numeric or a character string type.
   */
  friend TypedValueT operator>(const TypedValueT &a, const TypedValueT &b) { return !(a <= b); }

  /**
   * Compare TypedValueTs and return true, false or Null.
   *
   * Null is returned if either of the two values is Null.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values cannot be compared, i.e. they are
   *        not either Null, numeric or a character string type.
   */
  friend TypedValueT operator>=(const TypedValueT &a, const TypedValueT &b) { return !(a < b); }

  // arithmetic operators

  /**
   * Arithmetically negate a value.
   *
   * If the value is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the argument.
   *
   * @throw TypedValueException if the value is not numeric or Null.
   */
  friend TypedValueT operator-(const TypedValueT &a) {
    if (a.IsNull()) return TypedValueT(a.GetMemoryResource());
    if (a.IsInt()) return TypedValueT(-a.ValueInt(), a.GetMemoryResource());
    if (a.IsDouble()) return TypedValueT(-a.ValueDouble(), a.GetMemoryResource());
    if (a.IsDuration()) return TypedValueT(-a.ValueDuration(), a.GetMemoryResource());
    throw TypedValueException("Invalid unary minus operand type (-{})", a.type());
  }

  /**
   * Apply the unary plus operator to a value.
   *
   * If the value is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the argument.
   *
   * @throw TypedValueException if the value is not numeric or Null.
   */
  friend TypedValueT operator+(const TypedValueT &a) {
    if (a.IsNull()) return TypedValueT(a.GetMemoryResource());
    if (a.IsInt()) return TypedValueT(+a.ValueInt(), a.GetMemoryResource());
    if (a.IsDouble()) return TypedValueT(+a.ValueDouble(), a.GetMemoryResource());
    throw TypedValueException("Invalid unary plus operand type (+{})", a.type());
  }

  /**
   * Perform addition or concatenation on two values.
   *
   * Numeric values are summed, while lists and character strings are
   * concatenated. If either value is Null, then Null is returned. The resulting
   * value uses the same MemoryResource as the left hand side argument.
   *
   * @throw TypedValueException if values cannot be summed or concatenated.
   */
  friend TypedValueT operator+(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());

    if (a.IsList() || b.IsList()) {
      TypedValueT::TVector list(a.GetMemoryResource());
      auto append_list = [&list](const TypedValueT &v) {
        if (v.IsList()) {
          auto list2 = v.ValueList();
          list.insert(list.end(), list2.begin(), list2.end());
        } else {
          list.push_back(v);
        }
      };
      append_list(a);
      append_list(b);
      return TypedValueT(std::move(list), a.GetMemoryResource());
    }

    if (const auto maybe_add = MaybeDoTemporalTypeAddition(a, b); maybe_add) {
      return *maybe_add;
    }

    EnsureArithmeticallyOk(a, b, true, "addition");
    // no more Bool nor Null, summing works on anything from here onward

    if (a.IsString() || b.IsString()) return TypedValueT(ValueToString(a) + ValueToString(b), a.GetMemoryResource());

    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(ToDouble(a) + ToDouble(b), a.GetMemoryResource());
    }
    return TypedValueT(a.ValueInt() + b.ValueInt(), a.GetMemoryResource());
  }

  /**
   * Subtract two values.
   *
   * If any of the values is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values are not numeric or Null.
   */
  friend TypedValueT operator-(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    if (const auto maybe_sub = MaybeDoTemporalTypeSubtraction(a, b); maybe_sub) {
      return *maybe_sub;
    }
    EnsureArithmeticallyOk(a, b, true, "subraction");
    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(ToDouble(a) - ToDouble(b), a.GetMemoryResource());
    }
    return TypedValueT(a.ValueInt() - b.ValueInt(), a.GetMemoryResource());
  }

  /**
   * Divide two values.
   *
   * If any of the values is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values are not numeric or Null, or if
   *        dividing two integer values by zero.
   */
  friend TypedValueT operator/(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    EnsureArithmeticallyOk(a, b, false, "division");

    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(ToDouble(a) / ToDouble(b), a.GetMemoryResource());
    }
    if (b.ValueInt() == 0LL) {
      throw TypedValueException("Division by zero");
    }
    return TypedValueT(a.ValueInt() / b.ValueInt(), a.GetMemoryResource());
  }

  /**
   * Multiply two values.
   *
   * If any of the values is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values are not numeric or Null.
   */
  friend TypedValueT operator*(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    EnsureArithmeticallyOk(a, b, false, "multiplication");

    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(ToDouble(a) * ToDouble(b), a.GetMemoryResource());
    }
    return TypedValueT(a.ValueInt() * b.ValueInt(), a.GetMemoryResource());
  }

  /**
   * Perform modulo operation on two values.
   *
   * If any of the values is Null, then Null is returned.
   * The resulting value uses the same MemoryResource as the left hand side
   * argument.
   *
   * @throw TypedValueException if the values are not numeric or Null.
   */
  friend TypedValueT operator%(const TypedValueT &a, const TypedValueT &b) {
    if (a.IsNull() || b.IsNull()) return TypedValueT(a.GetMemoryResource());
    EnsureArithmeticallyOk(a, b, false, "modulo");

    // at this point we only have int and double
    if (a.IsDouble() || b.IsDouble()) {
      return TypedValueT(static_cast<double>(fmod(ToDouble(a), ToDouble(b))), a.GetMemoryResource());
    }
    if (b.ValueInt() == 0LL) {
      throw TypedValueException("Mod with zero");
    }
    return TypedValueT(a.ValueInt() % b.ValueInt(), a.GetMemoryResource());
  }

  /** Output the TypedValueT::Type value as a string */
  friend std::ostream &operator<<(std::ostream &os, const TypedValueT::Type &type) {
    switch (type) {
      case TypedValueT::Type::Null:
        return os << "null";
      case TypedValueT::Type::Bool:
        return os << "bool";
      case TypedValueT::Type::Int:
        return os << "int";
      case TypedValueT::Type::Double:
        return os << "double";
      case TypedValueT::Type::String:
        return os << "string";
      case TypedValueT::Type::List:
        return os << "list";
      case TypedValueT::Type::Map:
        return os << "map";
      case TypedValueT::Type::Vertex:
        return os << "vertex";
      case TypedValueT::Type::Edge:
        return os << "edge";
      case TypedValueT::Type::Path:
        return os << "path";
      case TypedValueT::Type::Date:
        return os << "date";
      case TypedValueT::Type::LocalTime:
        return os << "local_time";
      case TypedValueT::Type::LocalDateTime:
        return os << "local_date_time";
      case TypedValueT::Type::Duration:
        return os << "duration";
    }
    LOG_FATAL("Unsupported TypedValueT::Type");
  }

  friend std::ostream &operator<<(std::ostream &os, const TypedValueT &val) {
    switch (val.type()) {
      case TypedValueT::Type::Null:
        return os << "null";
      case TypedValueT::Type::Bool:
        return os << (val.ValueBool() ? "true" : "false");
      case TypedValueT::Type::Int:
        return os << val.ValueInt();
      case TypedValueT::Type::Double:
        return os << val.ValueDouble();
      case TypedValueT::Type::String:
        return os << val.ValueString();
      case TypedValueT::Type::List:
        os << "[";
        utils::PrintIterable(os, val.ValueList());
        return os << "]";
      case TypedValueT::Type::Map:
        os << "{";
        utils::PrintIterable(os, val.ValueMap(), ", ",
                             [](auto &strm, const auto &pr) { strm << pr.first << ": " << pr.second; });
        return os << "}";
      case TypedValueT::Type::Date:
        return os << val.ValueDate();
      case TypedValueT::Type::LocalTime:
        return os << val.ValueLocalTime();
      case TypedValueT::Type::LocalDateTime:
        return os << val.ValueLocalDateTime();
      case TypedValueT::Type::Duration:
        return os << val.ValueDuration();
      default:
        LOG_FATAL("Unsupported printing: TVertexAccessor || TEdgeAccessor || TPathT");
    }
  }

 private:
  void DestroyValue() {
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
        vertex_v.~TVertexAccessor();
        break;
      case Type::Edge:
        edge_v.~TEdgeAccessor();
        break;
      case Type::Path:
        path_v.~TPathT();
        break;
      case Type::Date:
      case Type::LocalTime:
      case Type::LocalDateTime:
      case Type::Duration:
        break;
    }

    type_ = TypedValueT::Type::Null;
  }

  friend void EnsureLogicallyOk(const TypedValueT &a, const TypedValueT &b, const std::string &op_name) {
    if (!((a.IsBool() || a.IsNull()) && (b.IsBool() || b.IsNull())))
      throw TypedValueException("Invalid {} operand types({} && {})", op_name, a.type(), b.type());
  }

  /**
   * Turns a numeric or string value into a string.
   *
   * @param value a value.
   * @return A string.
   */
  friend std::string ValueToString(const TypedValueT &value) {
    // TODO: Should this allocate a string through value.GetMemoryResource()?
    if (value.IsString()) return std::string(value.ValueString());
    if (value.IsInt()) return std::to_string(value.ValueInt());
    if (value.IsDouble()) return fmt::format("{}", value.ValueDouble());
    // unsupported situations
    throw TypedValueException("Unsupported TypedValueT::Type conversion to string");
  }
  /**
   * Raises a TypedValueTException if the given values do not support arithmetic
   * operations. If they do, nothing happens.
   *
   * @param a First value.
   * @param b Second value.
   * @param string_ok If or not for the given operation it's valid to work with
   *  String values (typically it's OK only for sum).
   *  @param op_name Name of the operation, used only for exception description,
   *  if raised.
   */
  friend void EnsureArithmeticallyOk(const TypedValueT &a, const TypedValueT &b, bool string_ok,
                                     const std::string &op_name) {
    auto is_legal = [string_ok](const TypedValueT &value) {
      return value.IsNumeric() || (string_ok && value.type() == TypedValueT::Type::String);
    };

    // Note that List and Null can also be valid in arithmetic ops. They are not
    // checked here because they are handled before this check is performed in
    // arithmetic op implementations.

    if (!is_legal(a) || !is_legal(b))
      throw TypedValueException("Invalid {} operand types {}, {}", op_name, a.type(), b.type());
  }

  friend bool IsTemporalType(const TypedValueT::Type type) {
    static constexpr std::array temporal_types{TypedValueT::Type::Date, TypedValueT::Type::LocalTime,
                                               TypedValueT::Type::LocalDateTime, TypedValueT::Type::Duration};
    return std::any_of(temporal_types.begin(), temporal_types.end(),
                       [type](const auto temporal_type) { return temporal_type == type; });
  }

  friend double ToDouble(const TypedValueT &value) {
    switch (value.type()) {
      case TypedValueT::Type::Int:
        return (double)value.ValueInt();
      case TypedValueT::Type::Double:
        return value.ValueDouble();
      default:
        throw TypedValueException("Unsupported TypedValueT::Type conversion to double");
    }
  }

  friend std::optional<TypedValueT> MaybeDoTemporalTypeAddition(const TypedValueT &a, const TypedValueT &b) {
    // Duration
    if (a.IsDuration() && b.IsDuration()) {
      return TypedValueT(a.ValueDuration() + b.ValueDuration());
    }
    // Date
    if (a.IsDate() && b.IsDuration()) {
      return TypedValueT(a.ValueDate() + b.ValueDuration());
    }
    if (a.IsDuration() && b.IsDate()) {
      return TypedValueT(a.ValueDuration() + b.ValueDate());
    }
    // LocalTime
    if (a.IsLocalTime() && b.IsDuration()) {
      return TypedValueT(a.ValueLocalTime() + b.ValueDuration());
    }
    if (a.IsDuration() && b.IsLocalTime()) {
      return TypedValueT(a.ValueDuration() + b.ValueLocalTime());
    }
    // LocalDateTime
    if (a.IsLocalDateTime() && b.IsDuration()) {
      return TypedValueT(a.ValueLocalDateTime() + b.ValueDuration());
    }
    if (a.IsDuration() && b.IsLocalDateTime()) {
      return TypedValueT(a.ValueDuration() + b.ValueLocalDateTime());
    }
    return std::nullopt;
  }

  friend std::optional<TypedValueT> MaybeDoTemporalTypeSubtraction(const TypedValueT &a, const TypedValueT &b) {
    // Duration
    if (a.IsDuration() && b.IsDuration()) {
      return TypedValueT(a.ValueDuration() - b.ValueDuration());
    }
    // Date
    if (a.IsDate() && b.IsDuration()) {
      return TypedValueT(a.ValueDate() - b.ValueDuration());
    }
    if (a.IsDate() && b.IsDate()) {
      return TypedValueT(a.ValueDate() - b.ValueDate());
    }
    // LocalTime
    if (a.IsLocalTime() && b.IsDuration()) {
      return TypedValueT(a.ValueLocalTime() - b.ValueDuration());
    }
    if (a.IsLocalTime() && b.IsLocalTime()) {
      return TypedValueT(a.ValueLocalTime() - b.ValueLocalTime());
    }
    // LocalDateTime
    if (a.IsLocalDateTime() && b.IsDuration()) {
      return TypedValueT(a.ValueLocalDateTime() - b.ValueDuration());
    }
    if (a.IsLocalDateTime() && b.IsLocalDateTime()) {
      return TypedValueT(a.ValueLocalDateTime() - b.ValueLocalDateTime());
    }
    return std::nullopt;
  }

  // Memory resource for allocations of non primitive values
  utils::MemoryResource *memory_{utils::NewDeleteResource()};

  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    // Since this is used in query runtime, size of union is not critical so
    // string and vector are used instead of pointers. It requires copy of data,
    // but most of algorithms (concatenations, serialisation...) has linear time
    // complexity so it shouldn't be a problem. This is maybe even faster
    // because of data locality.
    TString string_v;
    TVector list_v;
    TMap map_v;
    TVertexAccessor vertex_v;
    TEdgeAccessor edge_v;
    TPathT path_v;
    utils::Date date_v;
    utils::LocalTime local_time_v;
    utils::LocalDateTime local_date_time_v;
    utils::Duration duration_v;
  };

  /**
   * The Type of property.
   */
  Type type_;
};

}  // namespace memgraph::expr
