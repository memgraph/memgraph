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

#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "interface/gen-cpp2/Storage.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/v2/storage.hpp"

namespace manual::storage {
// TODO(antaljanosbenjamin):
// - Check out different approaches of how Thrift message members can be read/write
class StorageServiceHandler final : public interface::storage::StorageSvIf {
 public:
  explicit StorageServiceHandler(::storage::Storage &db) : db_{db} {}

  int64_t startTransaction() override;
  void commitTransaction(::interface::storage::Result &result, int64_t transaction_id) override;
  void abortTransaction(int64_t transaction_id) override;

  void createVertices(::interface::storage::Result &result,
                      const ::interface::storage::CreateVerticesRequest &req) override;

  void scanVertices(::interface::storage::ScanVerticesResponse &resp,
                    const ::interface::storage::ScanVerticesRequest &req) override;

 private:
  ::storage::Storage &db_;
  folly::ConcurrentHashMap<int64_t, std::shared_ptr<::storage::Storage::Accessor>> active_transactions_;
};
}  // namespace manual::storage
