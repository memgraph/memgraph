// Copyright 2023 Memgraph Ltd.
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

#include "storage/v2/constraints/unique_constraints.hpp"

namespace memgraph::storage {

/// Utility class to store data in a fixed size array. The array is used
/// instead of `std::vector` to avoid `std::bad_alloc` exception where not
/// necessary.
template <class T>
struct FixedCapacityArray {
  size_t size;
  T values[kUniqueConstraintsMaxProperties];

  explicit FixedCapacityArray(size_t array_size) : size(array_size) {
    MG_ASSERT(size <= kUniqueConstraintsMaxProperties, "Invalid array size!");
  }
};

using PropertyIdArray = FixedCapacityArray<PropertyId>;

class InMemoryUniqueConstraints : public UniqueConstraints {
 private:
  struct Entry {
    std::vector<PropertyValue> values;
    const Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) const;
    bool operator==(const Entry &rhs) const;

    bool operator<(const std::vector<PropertyValue> &rhs) const;
    bool operator==(const std::vector<PropertyValue> &rhs) const;
  };

 public:
  /// Indexes the given vertex for relevant labels and properties.
  /// This method should be called before committing and validating vertices
  /// against unique constraints.
  /// @throw std::bad_alloc
  void UpdateBeforeCommit(const Vertex *vertex, const Transaction &tx);

  /// Creates unique constraint on the given `label` and a list of `properties`.
  /// Returns constraint violation if there are multiple vertices with the same
  /// label and property values. Returns `CreationStatus::ALREADY_EXISTS` if
  /// constraint already existed, `CreationStatus::EMPTY_PROPERTIES` if the
  /// given list of properties is empty,
  /// `CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the list of properties
  /// exceeds the maximum allowed number of properties, and
  /// `CreationStatus::SUCCESS` on success.
  /// @throw std::bad_alloc
  utils::BasicResult<ConstraintViolation, CreationStatus> CreateConstraint(LabelId label,
                                                                           const std::set<PropertyId> &properties,
                                                                           utils::SkipList<Vertex>::Accessor vertices);

  /// Deletes the specified constraint. Returns `DeletionStatus::NOT_FOUND` if
  /// there is not such constraint in the storage,
  /// `DeletionStatus::EMPTY_PROPERTIES` if the given set of `properties` is
  /// empty, `DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED` if the given set
  /// of `properties` exceeds the maximum allowed number of properties, and
  /// `DeletionStatus::SUCCESS` on success.
  DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) override;

  bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const override;

  void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                           const uint64_t transaction_start_timestamp) override {}

  void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                        uint64_t transaction_start_timestamp) override{};

  /// Validates the given vertex against unique constraints before committing.
  /// This method should be called while commit lock is active with
  /// `commit_timestamp` being a potential commit timestamp of the transaction.
  /// @throw std::bad_alloc
  std::optional<ConstraintViolation> Validate(const Vertex &vertex, const Transaction &tx,
                                              uint64_t commit_timestamp) const;

  std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const override;

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor constraint_accessor, LabelId label, PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
             Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator constraint_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return constraint_iterator_ == other.constraint_iterator_; }
      bool operator!=(const Iterator &other) const { return constraint_iterator_ != other.constraint_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator constraint_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Entry>::Accessor constraint_accessor_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    bool bounds_valid_{true};
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property) const override;
  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                  const PropertyValue &value) const override;
  uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

  /// GC method that removes outdated entries from constraints' storages.
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  void Clear() override;

  Iterable Vertices(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                    Transaction *transaction);

 private:
  std::map<std::pair<LabelId, std::set<PropertyId>>, utils::SkipList<Entry>> constraints_;
  std::map<LabelId, std::map<std::set<PropertyId>, utils::SkipList<Entry> *>> constraints_by_label_;
};

}  // namespace memgraph::storage
