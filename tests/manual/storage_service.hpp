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

#include "interface/storage.hpp"
#include "storage/v2/storage.hpp"

namespace manual::storage {

class StorageServiceHandler final : public interface::storage::StorageSvIf {
 public:
  explicit StorageServiceHandler(::storage::Storage &db) : db_{db} {}

  void createVertices(::interface::storage::Result &result,
                      std::unique_ptr<::interface::storage::CreateVerticesRequest> req) override;

 private:
  ::storage::Storage &db_;
};
}  // namespace manual::storage
