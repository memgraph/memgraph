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

#include <compare>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/property_path.hpp"

namespace memgraph::storage {

struct LabelPropertyIndexEntry {
  LabelId label;
  std::vector<PropertyPath> properties;
  IndexOrder order{IndexOrder::ASC};

  friend auto operator<=>(LabelPropertyIndexEntry const &, LabelPropertyIndexEntry const &) = default;
  friend bool operator==(LabelPropertyIndexEntry const &, LabelPropertyIndexEntry const &) = default;
};

}  // namespace memgraph::storage
