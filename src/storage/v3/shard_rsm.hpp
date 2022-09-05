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

#include <openssl/ec.h>
#include "query/v2/requests.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"

// struct CreateVerticesRequest {
//   Hlc transaction_id;
//   std::vector<NewVertex> new_vertices;
// };

// struct CreateVerticesResponse {
//   bool success;
// };

// using ReadRequests = std::variant<ExpandOneRequest, GetPropertiesRequest, ScanVerticesRequest>;
// using ReadResponses = std::variant<ExpandOneResponse, GetPropertiesResponse, ScanVerticesResponse>;

// using WriteRequests = CreateVerticesRequest;
// using WriteResponses = CreateVerticesResponse;

// ResultSchema<VertexAccessor> CreateVertexAndValidate(
//         LabelId primary_label, const std::vector<LabelId> &labels,
//         const std::vector<std::pair<PropertyId, PropertyValue>> &properties);

namespace memgraph::storage::v3 {

template <typename>
constexpr auto kAlwaysFalse = false;

class ShardMessageHandler {
  Shard shard_;

  WriteResponses ApplyWrite(CreateVerticesRequest &&req);

 public:
};

}  // namespace memgraph::storage::v3
