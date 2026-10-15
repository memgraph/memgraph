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

namespace memgraph::storage {

/// Which kinds of object a transaction set properties on. A property delta records which property
/// was written but not what it was written on, and only the code creating the delta knows that.
/// Whoever reads the deltas later needs it: a property written on a vertex can only leave a vertex
/// index needing cleanup, and one written on an edge only an edge index.
struct PropertyWriteTargets {
  bool vertices{false};
  bool edges{false};
};

}  // namespace memgraph::storage
