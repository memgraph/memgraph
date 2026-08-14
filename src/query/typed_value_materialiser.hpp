// Copyright 2026 Memgraph Ltd.
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

#include <cstddef>
#include <utility>

#include "query/typed_value.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/memory.hpp"
#include "utils/temporal.hpp"

namespace memgraph::query {

/// Builds a `TypedValue` from each thing a property read hands out, and gives it to the derived
/// class to put somewhere.
///
/// A read used to produce a `storage::PropertyValue` per property, which the query layer then
/// rebuilt as the `TypedValue` it actually wanted. Both are a switch over a wide tagged union, and
/// the pair of them was one of the largest things in an aggregation's profile. The store will hand
/// each value over as what it is instead, so it gets built once - this is the half of that which
/// knows about query values.
///
/// Two callers want it and differ only in where the value goes: a `CacheProperties` row writes each
/// value to a frame slot, and a single property lookup keeps its one value as its result. They
/// supply `Write(index, TypedValue &&)`.
template <typename Derived>
class TypedValueMaterialiser {
 public:
  void EmitNull(std::size_t index) { Give(index, TypedValue{memory_}); }

  void Emit(std::size_t index, bool value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, int64_t value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, double value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, std::string_view value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, storage::Enum value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, storage::Point2d value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, storage::Point3d value) { Give(index, TypedValue{value, memory_}); }

  void Emit(std::size_t index, storage::TemporalData value) {
    switch (value.type) {
      case storage::TemporalType::Date:
        return Give(index, TypedValue{utils::Date{std::chrono::microseconds{value.microseconds}}, memory_});
      case storage::TemporalType::LocalTime:
        return Give(index, TypedValue{utils::LocalTime{value.microseconds}, memory_});
      case storage::TemporalType::LocalDateTime:
        return Give(index, TypedValue{utils::LocalDateTime{value.microseconds}, memory_});
      case storage::TemporalType::Duration:
        return Give(index, TypedValue{utils::Duration{value.microseconds}, memory_});
    }
  }

  void Emit(std::size_t index, storage::ZonedTemporalData value) {
    Give(index, TypedValue{utils::ZonedDateTime{value.microseconds, value.timezone}, memory_});
  }

  /// The fallback: a list, a map or a vector-index handle still arrives as a storage value.
  void Emit(std::size_t index, storage::PropertyValue &&value) {
    Give(index, TypedValue{std::move(value), name_id_mapper_, memory_});
  }

 protected:
  TypedValueMaterialiser(storage::NameIdMapper *name_id_mapper, utils::MemoryResource *memory)
      : name_id_mapper_{name_id_mapper}, memory_{memory} {}

  utils::MemoryResource *memory() const { return memory_; }

 private:
  void Give(std::size_t index, TypedValue &&value) { static_cast<Derived *>(this)->Write(index, std::move(value)); }

  storage::NameIdMapper *name_id_mapper_;
  utils::MemoryResource *memory_;
};

/// Keeps the one value it is given, for a caller reading a single property.
class SingleValueMaterialiser : public TypedValueMaterialiser<SingleValueMaterialiser> {
 public:
  SingleValueMaterialiser(storage::NameIdMapper *name_id_mapper, utils::MemoryResource *memory)
      : TypedValueMaterialiser{name_id_mapper, memory}, value_{memory} {}

  /// The read may hand over a value more than once - the retry below a missing object reads
  /// again - so the last one is the answer.
  void Write(std::size_t /*index*/, TypedValue &&value) { value_ = std::move(value); }

  auto Take() -> TypedValue && { return std::move(value_); }

 private:
  TypedValue value_;
};

}  // namespace memgraph::query
