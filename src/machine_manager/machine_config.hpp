// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::machine_manager {

using memgraph::storage::v3::SchemaProperty;
using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;

struct InitialLabelSpace {
  std::string label_name;
  std::vector<SchemaProperty> schema;
  std::vector<CompoundKey> split_points;
};

struct MachineConfig {
  std::vector<InitialLabelSpaces> initial_label_spaces;
};

}  // namespace memgraph::machine_manager
