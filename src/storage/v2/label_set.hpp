// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

// custom datastructure to hold LabelIds
// design goals:
// - 16B, so we are smaller than std::vector<LabelId> (24B)
// - small representation to avoid allocation
// layout:
//  Heap allocation
//  ┌─────────┬─────────┐
//  │SIZE     │CAPACITY │
//  ├─────────┴─────────┤    ┌────┬────┬────┬─
//  │PTR                ├───►│    │    │    │ ...
//  └───────────────────┘    └────┴────┴────┴─
//  Small representation
//  ┌─────────┬─────────┐
//  │<=2      │2        │
//  ├─────────┼─────────┤
//  │Label1   │Label2   │
//  └─────────┴─────────┘

using old_label_set = std::vector<LabelId>;

namespace in_progress {
struct label_set {};

static_assert(sizeof(label_set) < sizeof(old_label_set));

}  // namespace in_progress

using label_set = old_label_set;

}  // namespace memgraph::storage
