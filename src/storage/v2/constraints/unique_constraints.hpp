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

#include <set>

#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/result.hpp"

namespace memgraph::storage {

// NOLINTNEXTLINE(misc-definitions-in-headers)
const size_t kUniqueConstraintsMaxProperties = 32;

class UniqueConstraints {
 public:
  UniqueConstraints() = default;
  UniqueConstraints(const UniqueConstraints &) = delete;
  UniqueConstraints(UniqueConstraints &&) = delete;
  UniqueConstraints &operator=(const UniqueConstraints &) = delete;
  UniqueConstraints &operator=(UniqueConstraints &&) = delete;
  virtual ~UniqueConstraints() = default;

  enum class CreationStatus {
    SUCCESS,
    ALREADY_EXISTS,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  enum class DeletionStatus {
    SUCCESS,
    NOT_FOUND,
    EMPTY_PROPERTIES,
    PROPERTIES_SIZE_LIMIT_EXCEEDED,
  };

  virtual DeletionStatus DropConstraint(LabelId label, const std::set<PropertyId> &properties) = 0;

  virtual bool ConstraintExists(LabelId label, const std::set<PropertyId> &properties) const = 0;

  virtual void UpdateOnRemoveLabel(LabelId removed_label, const Vertex &vertex_before_update,
                                   uint64_t transaction_start_timestamp) = 0;

  virtual void UpdateOnAddLabel(LabelId added_label, const Vertex &vertex_before_update,
                                uint64_t transaction_start_timestamp) = 0;

  virtual std::vector<std::pair<LabelId, std::set<PropertyId>>> ListConstraints() const = 0;

  virtual uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property) const = 0;
  virtual uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                          const PropertyValue &value) const = 0;
  virtual uint64_t ApproximateVertexCount(const LabelId &label, const PropertyId &property,
                                          const std::optional<utils::Bound<PropertyValue>> &,
                                          const std::optional<utils::Bound<PropertyValue>> &) const = 0;

  virtual void Clear() = 0;

 protected:
  static DeletionStatus CheckPropertiesBeforeDeletion(const std::set<PropertyId> &properties) {
    if (properties.empty()) {
      return UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES;
    }
    if (properties.size() > kUniqueConstraintsMaxProperties) {
      return UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED;
    }
    return UniqueConstraints::DeletionStatus::SUCCESS;
  }
};

}  // namespace memgraph::storage
