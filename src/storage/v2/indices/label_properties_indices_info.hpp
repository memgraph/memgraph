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

#include <cstdint>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/property_path.hpp"

namespace memgraph::storage {

// These positions are in reference to the // labels + properties passed into
// `RelevantLabelPropertiesIndicesInfo`
struct LabelPropertiesIndicesInfo {
  std::size_t label_pos_;
  std::vector<int64_t> properties_pos_;  // -1 means missing
  LabelId label_;
  std::vector<PropertyPath> properties_;
  IndexOrder order_{IndexOrder::ASC};
};

}  // namespace memgraph::storage
