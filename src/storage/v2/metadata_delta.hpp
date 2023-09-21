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

#include <atomic>
#include <cstdint>

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

// NOTE: MetadataDeltas are not supported in a multi command transaction (single query only)
struct MetadataDelta {
  enum class Action {
    LABEL_INDEX_CREATE,
    LABEL_INDEX_DROP,
    LABEL_PROPERTY_INDEX_CREATE,
    LABEL_PROPERTY_INDEX_DROP,
    EXISTENCE_CONSTRAINT_CREATE,
    EXISTENCE_CONSTRAINT_DROP,
    UNIQUE_CONSTRAINT_CREATE,
    UNIQUE_CONSTRAINT_DROP,
  };

  static constexpr struct LabelIndexCreate {
  } label_index_create;
  static constexpr struct LabelIndexDrop {
  } label_index_drop;
  static constexpr struct LabelPropertyIndexCreate {
  } label_property_index_create;
  static constexpr struct LabelPropertyIndexDrop {
  } label_property_index_drop;
  static constexpr struct ExistenceConstraintCreate {
  } existence_constraint_create;
  static constexpr struct ExistenceConstraintDrop {
  } existence_constraint_drop;
  static constexpr struct UniqueConstraintCreate {
  } unique_constraint_create;
  static constexpr struct UniqueConstraintDrop {
  } unique_constraint_drop;

  MetadataDelta(LabelIndexCreate /*tag*/, LabelId label) : action(Action::LABEL_INDEX_CREATE), label(label) {}

  MetadataDelta(LabelIndexDrop /*tag*/) : action(Action::LABEL_INDEX_DROP) {}

  MetadataDelta(LabelPropertyIndexCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::LABEL_PROPERTY_INDEX_CREATE), label_property{label, property} {}

  MetadataDelta(LabelPropertyIndexDrop /*tag*/) : action(Action::LABEL_PROPERTY_INDEX_DROP) {}

  MetadataDelta(ExistenceConstraintCreate /*tag*/) : action(Action::EXISTENCE_CONSTRAINT_CREATE) {}

  MetadataDelta(ExistenceConstraintDrop /*tag*/) : action(Action::EXISTENCE_CONSTRAINT_DROP) {}

  MetadataDelta(UniqueConstraintCreate /*tag*/) : action(Action::UNIQUE_CONSTRAINT_CREATE) {}

  MetadataDelta(UniqueConstraintDrop /*tag*/) : action(Action::UNIQUE_CONSTRAINT_DROP) {}

  MetadataDelta(const MetadataDelta &) = delete;
  MetadataDelta(MetadataDelta &&) = delete;
  MetadataDelta &operator=(const MetadataDelta &) = delete;
  MetadataDelta &operator=(MetadataDelta &&) = delete;

  ~MetadataDelta() = default;

  Action action;

  union {
    LabelId label;
    struct {
      LabelId label;
      PropertyId property;
    } label_property;
  };
};

static_assert(alignof(MetadataDelta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
