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

#include "storage/v2/constraints/unique_constraints.hpp"

namespace memgraph::storage {

void UniqueConstraints::AbortProcessor::Collect(Vertex const *vertex) {
  vertex->labels.for_each([&](uint32_t id) {
    auto label = LabelId::FromUint(id);
    auto it = abortable_info_.find(label);
    if (it == abortable_info_.end()) {
      return;
    }

    for (auto &[props, collection] : it->second) {
      auto values = vertex->properties.ExtractPropertyValues(props);
      if (!values) {
        continue;
      }
      collection.emplace_back(std::move(*values), vertex);
    }
  });
}

}  // namespace memgraph::storage
