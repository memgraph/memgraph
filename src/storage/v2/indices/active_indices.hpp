// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/indices/label_property_index.hpp"

#include <memory>

namespace memgraph::storage {

struct IndicesCollection {
  std::vector<storage::LabelId> label_;
  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> label_properties_;
};

struct ActiveIndices {
  bool CheckIndicesAreReady(IndicesCollection const &required_indices) {
    // label
    for ([[maybe_unused]] auto const &label : required_indices.label_) {
      // TODO: when we have concurrent index creation for labels
    }

    // label + properties
    for (auto const &[label, properties] : required_indices.label_properties_) {
      if (!label_properties_->IndexReady(label, properties)) return false;
    }

    return true;
  }

  std::unique_ptr<LabelPropertyIndex::ActiveIndices> label_properties_;
};
}  // namespace memgraph::storage
