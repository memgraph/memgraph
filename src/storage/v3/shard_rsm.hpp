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

#include <memory>
#include <variant>

#include <openssl/ec.h>
#include "query/v2/requests.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace memgraph::storage::v3 {

using namespace memgraph::messages;

template <typename>
constexpr auto kAlwaysFalse = false;

class ShardRsm {
  std::unique_ptr<Shard> shard_;

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
  explicit ShardRsm(std::unique_ptr<Shard> &&shard) : shard_(std::move(shard)){};

  ReadResponses Read(ReadRequests requests) {
    return std::visit([&](auto &&request) mutable { return HandleRead(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }

  WriteResponses Apply(WriteRequests requests) {
    return std::visit([&](auto &&request) mutable { return ApplyWrite(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }
};

}  // namespace memgraph::storage::v3
