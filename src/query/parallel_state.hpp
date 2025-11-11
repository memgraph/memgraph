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

#include <mutex>
#include <optional>

#include "query/db_accessor.hpp"

namespace memgraph::query {

/// Shared state for coordinating parallel execution branches.
/// Created by ParallelStart, accessed by ChunkReader and ParallelMerge.
struct ParallelState {
  explicit ParallelState(size_t num_threads) : num_threads_(num_threads) {}

  void SetChunks(VerticesChunkedIterable chunks) {
    std::lock_guard lock(mutex_);
    chunks_ = std::move(chunks);
    chunk_index_ = 0;
    initialized_ = true;
  }

  bool IsInitialized() const {
    std::lock_guard lock(mutex_);
    return initialized_;
  }

  /// Get the next chunk for the given branch.
  /// Returns nullopt when all chunks have been distributed.
  std::optional<VerticesChunkedIterable::Chunk> GetNextChunk() {
    std::lock_guard lock(mutex_);
    if (!initialized_ || !chunks_.has_value()) {
      return std::nullopt;
    }
    if (chunk_index_ >= chunks_->size()) {
      return std::nullopt;
    }
    // Distribute chunks round-robin
    size_t chunk_idx = chunk_index_++;
    if (chunk_idx >= chunks_->size()) {
      return std::nullopt;
    }
    return chunks_->get_chunk(chunk_idx);
  }

  size_t GetNumThreads() const { return num_threads_; }

 private:
  mutable std::mutex mutex_;
  size_t num_threads_;
  std::optional<VerticesChunkedIterable> chunks_;
  size_t chunk_index_{0};
  bool initialized_{false};
};

}  // namespace memgraph::query
