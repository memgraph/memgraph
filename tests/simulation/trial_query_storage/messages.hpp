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

#include <optional>
#include <string>
#include <vector>

// header

namespace memgraph::tests::simulation {

struct Vertex {
  std::string key;
};

struct ScanVerticesRequest {
  int64_t count;
  std::optional<int64_t> continuation;
};

struct VerticesResponse {
  std::vector<Vertex> vertices;
  std::optional<int64_t> continuation;
};

}  // namespace memgraph::tests::simulation
