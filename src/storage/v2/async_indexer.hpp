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
#include "storage/v2/indices/property_path.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

struct AsyncIndexer {
  AsyncIndexer() = default;

  ~AsyncIndexer();

  void Enqueue(LabelId label);

  void Enqueue(EdgeTypeId edge_type);

  void Enqueue(LabelId label, PropertiesPaths properties);

  void Enqueue(PropertyId property);

  void RunGC();

  void Clear();

  /// Check if the async indexer is idle (no pending work)
  /// @return true if no work is queued and thread is waiting, false otherwise
  bool IsIdle() const;

  /// Check if the async indexer thread has stopped (due to null protector or stop request)
  /// @return true if the background thread is no longer running, false otherwise
  bool HasThreadStopped() const;

  /// After storage recovery we can Start then async task
  void Start(std::stop_token stop_token, Storage *storage);

  void Shutdown();

 private:
  struct LabelProperties {
    LabelId label;
    PropertiesPaths properties;

    friend auto operator<=>(LabelProperties const &, LabelProperties const &) = default;
  };

  // Label, EdgeType, Composite, Edge Property
  utils::SkipList<std::variant<LabelId, EdgeTypeId, LabelProperties, PropertyId>> request_queue_{};
  mutable std::mutex mutex_{};
  std::condition_variable cv_{};
  std::jthread index_creator_thread_{};
  mutable std::atomic<bool> is_processing_{false};       // Track if thread is actively processing
  mutable std::atomic<bool> thread_has_stopped_{false};  // Track if thread has exited
};

}  // namespace memgraph::storage
