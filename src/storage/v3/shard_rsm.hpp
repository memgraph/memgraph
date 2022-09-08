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

#include <variant>

#include <openssl/ec.h>
#include "query/v2/requests.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace memgraph::storage::v3 {

template <typename>
constexpr auto kAlwaysFalse = false;

class ShardRsm {
  Shard shard_;

  ReadResponses HandleRead(ExpandOneRequest &&req);
  ReadResponses HandleRead(GetPropertiesRequest &&req);
  ReadResponses HandleRead(ScanVerticesRequest &&req);

  WriteResponses ApplyWrite(CreateVerticesRequest &&req);
  WriteResponses ApplyWrite(DeleteVerticesRequest &&req);
  WriteResponses ApplyWrite(UpdateVerticesRequest &&req);

  WriteResponses ApplyWrite(CreateEdgesRequest &&req);
  WriteResponses ApplyWrite(DeleteEdgesRequest &&req);
  WriteResponses ApplyWrite(UpdateEdgesRequest &&req);

 public:
  explicit ShardRsm(LabelId primary_label, PrimaryKey min_primary_key, std::optional<PrimaryKey> max_primary_key,
                    Config config = Config())
      : shard_(primary_label, min_primary_key, max_primary_key, config){};

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static
  ReadResponses Read(ReadRequests requests) {
    return std::visit([&](auto &&request) mutable { return HandleRead(std::forward<decltype(request)>(request)); },
                      std::move(requests));  // NOLINT(hicpp-move-const-arg,performance-move-const-arg)
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static
  WriteResponses Apply(WriteRequests requests) {
    return std::visit([&](auto &&request) mutable { return ApplyWrite(std::forward<decltype(request)>(request)); },
                      std::move(requests));  // NOLINT(hicpp-move-const-arg,performance-move-const-arg)
  }
};

}  // namespace memgraph::storage::v3
