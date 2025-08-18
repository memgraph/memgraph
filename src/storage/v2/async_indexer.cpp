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

#include "storage/v2/async_indexer.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::storage {

AsyncIndexer::~AsyncIndexer() { cv_.notify_one(); }

void AsyncIndexer::Enqueue(LabelId label) {
  request_queue_.access().insert(label);
  cv_.notify_one();
}

void AsyncIndexer::Enqueue(EdgeTypeId edge_type) {
  request_queue_.access().insert(edge_type);
  cv_.notify_one();
}

void AsyncIndexer::Enqueue(LabelId label, PropertiesPaths properties) {
  request_queue_.access().insert(LabelProperties{label, std::move(properties)});
  cv_.notify_one();
}

void AsyncIndexer::Enqueue(PropertyId property) {
  request_queue_.access().insert(property);
  cv_.notify_one();
}

void AsyncIndexer::RunGC() { request_queue_.run_gc(); }

void AsyncIndexer::Clear() {
  // SkipList clear is not thread safe
  // need to make sure it is not being scanned
  auto lock = std::unique_lock{mutex_};
  request_queue_.clear();
}

bool AsyncIndexer::IsIdle() const {
  std::lock_guard const lock(mutex_);
  // no work to pickup and not currently working
  // OR we have stopped the worker thread
  return (request_queue_.size() == 0 && !is_processing_.load()) || HasThreadStopped();
}

bool AsyncIndexer::HasThreadStopped() const { return thread_has_stopped_.load(); }

void AsyncIndexer::Start(std::stop_token stop_token, Storage *storage) {
  index_creator_thread_ = std::jthread{[this, stop_token, storage]() {
    auto on_exit = utils::OnScopeExit{[this] { thread_has_stopped_ = true; }};

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

      // Mark as processing when we have work to do
      is_processing_ = true;
      auto processing_exit = utils::OnScopeExit{[this] {
        // Mark as not processing when done with current batch
        is_processing_ = false;
      }};

      for (auto it = access.begin(); it != it_end;) {
        try {
          auto protector = storage->make_database_protector();
          if (!protector) [[unlikely]] {
            // database has been dropped, we should aim to stop this worker ASAP
            return;
          }

          // If we wait forever for the read only lock it will block new writes
          auto const storage_acc = std::invoke([&]() {
            return storage->GetStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL
                       ? storage->ReadOnlyAccess(IsolationLevel::SNAPSHOT_ISOLATION, std::chrono::milliseconds(1000))
                       : storage->UniqueAccess(IsolationLevel::SNAPSHOT_ISOLATION, std::chrono::milliseconds(1000));
          });

          // Creating an index can only fail due to db shutdown or if the index manually got created
          // so there is not need to handle errors
          auto create_index = utils::Overloaded{
              [&](LabelId label) { [[maybe_unused]] auto result = storage_acc->CreateIndex(label, cancel_check); },
              [&](EdgeTypeId edge_type) {
                [[maybe_unused]] auto result = storage_acc->CreateIndex(edge_type, cancel_check);
              },
              [&](LabelProperties &lp) {
                [[maybe_unused]] auto result = storage_acc->CreateIndex(lp.label, lp.properties, cancel_check);
              },
              [&](PropertyId property) {
                [[maybe_unused]] auto result = storage_acc->CreateGlobalEdgeIndex(property, cancel_check);
              }};

          std::visit(create_index, *it);

          // Need to commit to publish the new index
          // NOTE: only MAIN will enqueue async index creation, hence PrepareForCommitPhase will always be from MAIN

          [[maybe_unused]] auto result =
              storage_acc->PrepareForCommitPhase(CommitArgs::make_main(std::move(protector)));

          auto const next_it = std::next(it);
          access.remove(*it);
          it = next_it;
          backoff = std::chrono::milliseconds(100);
        } catch (ReadOnlyAccessTimeout &) {
          spdlog::info("Async index creation, was blocked by other transactions. Retrying in {} ms.", backoff.count());
          std::this_thread::sleep_for(backoff);
          backoff = std::min(backoff * 3 / 2, std::chrono::milliseconds(10'000));  // 1.5x multiplier, max 10s
        }
      }
    }
  }};
}

void AsyncIndexer::Shutdown() {
  index_creator_thread_.request_stop();
  auto guard = std::unique_lock{mutex_};
  cv_.wait(guard, [this] { return !index_creator_thread_.joinable() || thread_has_stopped_.load(); });
}

}  // namespace memgraph::storage
