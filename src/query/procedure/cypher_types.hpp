// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include "mg_procedure.h"

#include <functional>
#include <memory>
#include <string_view>

#include "query/procedure/cypher_type_ptr.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"

namespace query::procedure {

class ListType;
class NullableType;

/// Interface for all supported types in openCypher type system.
class CypherType {
 public:
  CypherType() = default;
  virtual ~CypherType() = default;

  CypherType(const CypherType &) = delete;
  CypherType(CypherType &&) = delete;
  CypherType &operator=(const CypherType &) = delete;
  CypherType &operator=(CypherType &&) = delete;

  /// Get name of the type as it should be presented to the user.
  virtual std::string_view GetPresentableName() const = 0;

  /// Return true if given mgp_value is of the type as described by `this`.
  virtual bool SatisfiesType(const mgp_value &) const = 0;

  /// Return true if given TypedValue is of the type as described by `this`.
  virtual bool SatisfiesType(const query::TypedValue &) const = 0;

  // The following methods are a simple replacement for RTTI because we have
  // some special cases we need to handle.
  virtual const ListType *AsListType() const { return nullptr; }
  virtual const NullableType *AsNullableType() const { return nullptr; }
};

// Simple Types

class AnyType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "ANY"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type != MGP_VALUE_TYPE_NULL; }

  bool SatisfiesType(const query::TypedValue &value) const override { return !value.IsNull(); }
};

class BoolType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "BOOLEAN"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_BOOL; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsBool(); }
};

class StringType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "STRING"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_STRING; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsString(); }
};

class IntType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "INTEGER"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_INT; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsInt(); }
};

class FloatType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "FLOAT"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_DOUBLE; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsDouble(); }
};

class NumberType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "NUMBER"; }

  bool SatisfiesType(const mgp_value &value) const override {
    return value.type == MGP_VALUE_TYPE_INT || value.type == MGP_VALUE_TYPE_DOUBLE;
  }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsInt() || value.IsDouble(); }
};

class NodeType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "NODE"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_VERTEX; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsVertex(); }
};

class RelationshipType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "RELATIONSHIP"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_EDGE; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsEdge(); }
};

class PathType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "PATH"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_PATH; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsPath(); }
};

// You'd think that MapType would be a composite type like ListType, but nope.
// Why? No-one really knows. It's defined like that in "CIP2015-09-16 Public
// Type System and Type Annotations"
// Additionally, MapType also covers NodeType and RelationshipType because
// values of that type have property *maps*.
class MapType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "MAP"; }

  bool SatisfiesType(const mgp_value &value) const override {
    return value.type == MGP_VALUE_TYPE_MAP || value.type == MGP_VALUE_TYPE_VERTEX || value.type == MGP_VALUE_TYPE_EDGE;
  }

  bool SatisfiesType(const query::TypedValue &value) const override {
    return value.IsMap() || value.IsVertex() || value.IsEdge();
  }
};

// Temporal Types

class DateType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "DATE"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_DATE; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsDate(); }
};

class LocalTimeType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "LOCAL_TIME"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_LOCAL_TIME; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsLocalTime(); }
};

class LocalDateTimeType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "LOCAL_DATE_TIME"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_LOCAL_DATE_TIME; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsLocalDateTime(); }
};

class DurationType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "DURATION"; }

  bool SatisfiesType(const mgp_value &value) const override { return value.type == MGP_VALUE_TYPE_DURATION; }

  bool SatisfiesType(const query::TypedValue &value) const override { return value.IsDuration(); }
};

// Composite Types

class ListType : public CypherType {
 public:
  CypherTypePtr element_type_;
  utils::pmr::string presentable_name_;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  explicit ListType(CypherTypePtr element_type, utils::MemoryResource *memory)
      : element_type_(std::move(element_type)), presentable_name_("LIST OF ", memory) {
    presentable_name_.append(element_type_->GetPresentableName());
  }

  std::string_view GetPresentableName() const override { return presentable_name_; }

  bool SatisfiesType(const mgp_value &value) const override {
    if (value.type != MGP_VALUE_TYPE_LIST) {
      return false;
    }
    auto *list = value.list_v;
    const auto list_size = list->elems.size();
    for (size_t i = 0; i < list_size; ++i) {
      if (!element_type_->SatisfiesType(list->elems[i])) {
        return false;
      };
    }
    return true;
  }

  bool SatisfiesType(const query::TypedValue &value) const override {
    if (!value.IsList()) return false;
    for (const auto &elem : value.ValueList()) {
      if (!element_type_->SatisfiesType(elem)) return false;
    }
    return true;
  }

  const ListType *AsListType() const override { return this; }
};

class NullableType : public CypherType {
  CypherTypePtr type_;
  utils::pmr::string presentable_name_;

  // Constructor is private, because we use a factory method Create to prevent
  // nesting NullableType on top of each other.
  // @throw std::bad_alloc
  // @throw std::length_error
  explicit NullableType(CypherTypePtr type, utils::MemoryResource *memory)
      : type_(std::move(type)), presentable_name_(memory) {
    const auto *list_type = type_->AsListType();
    // ListType is specially formatted
    if (list_type) {
      presentable_name_.assign("LIST? OF ").append(list_type->element_type_->GetPresentableName());
    } else {
      presentable_name_.assign(type_->GetPresentableName()).append("?");
    }
  }

 public:
  /// Create a NullableType of some CypherType.
  /// If passed in `type` is already a NullableType, it is returned intact.
  /// Otherwise, `type` is wrapped in a new instance of NullableType.
  /// @throw std::bad_alloc
  /// @throw std::length_error
  static CypherTypePtr Create(CypherTypePtr type, utils::MemoryResource *memory) {
    if (type->AsNullableType()) return type;
    utils::Allocator<NullableType> alloc(memory);
    auto *nullable = alloc.allocate(1);
    try {
      new (nullable) NullableType(std::move(type), memory);
    } catch (...) {
      alloc.deallocate(nullable, 1);
      throw;
    }
    return CypherTypePtr(nullable, [alloc](CypherType *base_ptr) mutable {
      alloc.delete_object(static_cast<NullableType *>(base_ptr));
    });
  }

  std::string_view GetPresentableName() const override { return presentable_name_; }

  bool SatisfiesType(const mgp_value &value) const override {
    return value.type == MGP_VALUE_TYPE_NULL || type_->SatisfiesType(value);
  }

  bool SatisfiesType(const query::TypedValue &value) const override {
    return value.IsNull() || type_->SatisfiesType(value);
  }

  const NullableType *AsNullableType() const override { return this; }
};

}  // namespace query::procedure
