/// @file
#pragma once

#include <functional>
#include <memory>
#include <string_view>

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

  // TODO: Type checking
  // virtual bool SatisfiesType(const mgp_value &) const = 0;

  // The following methods are a simple replacement for RTTI because we have
  // some special cases we need to handle.
  virtual const ListType *AsListType() const { return nullptr; }
  virtual const NullableType *AsNullableType() const { return nullptr; }
};

using CypherTypePtr =
    std::unique_ptr<CypherType, std::function<void(CypherType *)>>;

// Simple Types

class AnyType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "ANY"; }
};

class BoolType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "BOOLEAN"; }
};

class StringType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "STRING"; }
};

class IntType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "INTEGER"; }
};

class FloatType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "FLOAT"; }
};

class NumberType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "NUMBER"; }
};

class NodeType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "NODE"; }
};

class RelationshipType : public CypherType {
 public:
  std::string_view GetPresentableName() const override {
    return "RELATIONSHIP";
  }
};

class PathType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "PATH"; }
};

// TODO: There's also Temporal Types, but we currently do not support those.

// You'd think that MapType would be a composite type like ListType, but nope.
// Why? No-one really knows. It's defined like that in "CIP2015-09-16 Public
// Type System and Type Annotations"
// Additionally, MapType also covers NodeType and RelationshipType because
// values of that type have property *maps*.
class MapType : public CypherType {
 public:
  std::string_view GetPresentableName() const override { return "MAP"; }
};

// Composite Types

class ListType : public CypherType {
 public:
  CypherTypePtr type_;
  utils::pmr::string presentable_name_;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  explicit ListType(CypherTypePtr type, utils::MemoryResource *memory)
      : type_(std::move(type)), presentable_name_("LIST OF ", memory) {
    presentable_name_.append(type_->GetPresentableName());
  }

  std::string_view GetPresentableName() const override {
    return presentable_name_;
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
      presentable_name_.assign("LIST? OF ")
          .append(list_type->type_->GetPresentableName());
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
  static CypherTypePtr Create(CypherTypePtr type,
                              utils::MemoryResource *memory) {
    if (type->AsNullableType()) return type;
    utils::Allocator<NullableType> alloc(memory);
    auto *nullable = alloc.allocate(1);
    try {
      new (nullable) NullableType(std::move(type), memory);
    } catch (...) {
      alloc.deallocate(nullable, 1);
      throw;
    }
    return CypherTypePtr(nullable, [memory](CypherType *base_ptr) {
      utils::Allocator<NullableType> alloc(memory);
      alloc.delete_object(static_cast<NullableType *>(base_ptr));
    });
  }

  std::string_view GetPresentableName() const override {
    return presentable_name_;
  }

  const NullableType *AsNullableType() const override { return this; }
};

}  // namespace query::procedure
