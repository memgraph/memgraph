// Copyright 2025 Memgraph Ltd.
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

#include <condition_variable>
#include <mutex>
#include <thread>
#include <variant>
#include "storage/v2/id_types.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

struct AutoIndexer {
  AutoIndexer(std::stop_token stop_token, Storage *storage);

  ~AutoIndexer();

  void Enqueue(LabelId label);

  void Enqueue(EdgeTypeId edge_type);

  void RunGC();

  void Clear();

 private:
  utils::SkipList<std::variant<LabelId, EdgeTypeId>> request_queue_{};
  std::mutex mutex_{};
  std::condition_variable cv_{};
  std::jthread index_creator_thread_{};
};

}  // namespace memgraph::storage
