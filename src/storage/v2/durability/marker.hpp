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

// Canonical definition is in wire_format/marker.hpp.
// Re-exported here for backward compatibility.
#include "wire_format/marker.hpp"

namespace memgraph::storage::durability {
using wire_format::kMarkersAll;
using wire_format::Marker;
}  // namespace memgraph::storage::durability
