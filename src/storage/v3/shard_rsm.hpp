// Copyright 2023 Memgraph Ltd.
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
#include <optional>
#include <variant>

#include <openssl/ec.h>
#include "query/v2/requests.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace memgraph::storage::v3 {

class ShardRsm {
  std::unique_ptr<Shard> shard_;

  msgs::ReadResponses HandleRead(msgs::ExpandOneRequest &&req);
  msgs::ReadResponses HandleRead(msgs::GetPropertiesRequest &&req);
  msgs::ReadResponses HandleRead(msgs::ScanVerticesRequest &&req);

  msgs::WriteResponses ApplyWrite(msgs::CreateVerticesRequest &&req);
  msgs::WriteResponses ApplyWrite(msgs::DeleteVerticesRequest &&req);
  msgs::WriteResponses ApplyWrite(msgs::UpdateVerticesRequest &&req);

  msgs::WriteResponses ApplyWrite(msgs::CreateExpandRequest &&req);
  msgs::WriteResponses ApplyWrite(msgs::DeleteEdgesRequest &&req);
  msgs::WriteResponses ApplyWrite(msgs::UpdateEdgesRequest &&req);

  msgs::WriteResponses ApplyWrite(msgs::CommitRequest &&req);

 public:
  explicit ShardRsm(std::unique_ptr<Shard> &&shard) : shard_(std::move(shard)){};

  std::optional<msgs::SplitInfo> ShouldSplit() const noexcept {
    auto split_info = shard_->ShouldSplit();
    if (split_info) {
      return msgs::SplitInfo{conversions::ConvertValueVector(split_info->split_point), split_info->shard_version};
    }
    return std::nullopt;
  }

  std::unique_ptr<Shard> PerformSplit(msgs::PerformSplitDataInfo perform_split) const noexcept {
    return Shard::FromSplitData(
        shard_->PerformSplit(conversions::ConvertPropertyVector(perform_split.split_key), perform_split.shard_version));
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  msgs::ReadResponses Read(msgs::ReadRequests requests) {
    return std::visit([&](auto &&request) mutable { return HandleRead(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  msgs::WriteResponses Apply(msgs::WriteRequests requests) {
    return std::visit([&](auto &&request) mutable { return ApplyWrite(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }
};

}  // namespace memgraph::storage::v3
