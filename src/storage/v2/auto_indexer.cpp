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

#include "storage/v2/auto_indexer.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

AutoIndexer::AutoIndexer(std::stop_token stop_token, Storage *storage) {
  index_creator_thread_ = std::jthread{[this, stop_token, storage]() {
    auto const cancel_check = [&]() { return stop_token.stop_requested(); };
    std::unique_lock<std::mutex> lock(mutex_);

    while (!stop_token.stop_requested()) {
      cv_.wait(lock, [&, this] { return request_queue_.size() != 0 || stop_token.stop_requested(); });
      if (stop_token.stop_requested()) {
        return;
      }

      auto access = request_queue_.access();
      auto it_end = access.end();
      auto backoff = std::chrono::milliseconds(100);
      for (auto it = access.begin(); it != it_end;) {
        try {
          // If we wait forever for the read only lock it will block new writes
          auto const storage_acc =
              storage->ReadOnlyAccess(IsolationLevel::SNAPSHOT_ISOLATION, std::chrono::milliseconds(1000));

          // Creating an index can only fail due to db shutdown or if the index manually got created
          // so there is not need to handle errors
          auto create_index = utils::Overloaded{
              [&](LabelId label) { [[maybe_unused]] auto result = storage_acc->CreateIndex(label, cancel_check); },
              [&](EdgeTypeId edge_type) {
                [[maybe_unused]] auto result = storage_acc->CreateIndex(edge_type, cancel_check);
              }};

          std::visit(create_index, *it);

          // Need to commit to publish the new index
          [[maybe_unused]] auto result = storage_acc->PrepareForCommitPhase();

          auto const next_it = std::next(it);
          access.remove(*it);
          it = next_it;
          backoff = std::chrono::milliseconds(100);
        } catch (ReadOnlyAccessTimeout &) {
          spdlog::info("Auto-indexing async creation, was blocked by other transactions. Retrying in {} ms.",
                       backoff.count());
          std::this_thread::sleep_for(backoff);
          backoff = std::min(backoff * 3 / 2, std::chrono::milliseconds(2000));  // 1.5x multiplier, max 2s
        }
      }
    }
  }};
}

AutoIndexer::~AutoIndexer() { cv_.notify_one(); }

void AutoIndexer::Enqueue(LabelId label) {
  request_queue_.access().insert(label);
  cv_.notify_one();
}

void AutoIndexer::Enqueue(EdgeTypeId edge_type) {
  request_queue_.access().insert(edge_type);
  cv_.notify_one();
}

void AutoIndexer::RunGC() { request_queue_.run_gc(); }

void AutoIndexer::Clear() {
  // SkipList clear is not thread safe
  // need to make sure it is not being scanned
  auto lock = std::unique_lock{mutex_};
  request_queue_.clear();
}
}  // namespace memgraph::storage
