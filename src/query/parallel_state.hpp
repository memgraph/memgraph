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

#include <optional>

#include "interpret/frame.hpp"
#include "query/db_accessor.hpp"

namespace memgraph::query {

class ParallelStateOnFrame {
 public:
  // Constructor for vertices
  ParallelStateOnFrame(std::shared_ptr<VerticesChunkedIterable> chunks, size_t chunk_index)
      : vertices_chunks_(std::move(chunks)), chunk_index_(chunk_index) {}

  // Constructor for edges
  ParallelStateOnFrame(std::shared_ptr<EdgesChunkedIterable> chunks, size_t chunk_index)
      : edges_chunks_(std::move(chunks)), chunk_index_(chunk_index) {}

  std::optional<VerticesChunkedIterable::Chunk> GetVerticesChunk() {
    if (!vertices_chunks_ || chunk_index_ >= vertices_chunks_->size()) {
      return std::nullopt;
    }
    return vertices_chunks_->get_chunk(chunk_index_);
  }

  std::optional<EdgesChunkedIterable::Chunk> GetEdgesChunk() {
    if (!edges_chunks_ || chunk_index_ >= edges_chunks_->size()) {
      return std::nullopt;
    }
    return edges_chunks_->get_chunk(chunk_index_);
  }

  std::shared_ptr<VerticesChunkedIterable> vertices_chunks_;
  std::shared_ptr<EdgesChunkedIterable> edges_chunks_;
  size_t chunk_index_;

  template <typename ChunksType>
  static void PushToFrame(FrameWriter &frame_writer, utils::MemoryResource *res, const Symbol &symbol,
                          std::shared_ptr<ChunksType> chunks, size_t index) {
    auto *state = new ParallelStateOnFrame(std::move(chunks), index);
    TypedValue tv(reinterpret_cast<int64_t>(state), res);
    frame_writer.Write(symbol, std::move(tv));
  }

  static auto PopFromFrame(Frame &frame, const Symbol &symbol) -> std::unique_ptr<ParallelStateOnFrame> {
    auto tv = frame.at(symbol);
    DMG_ASSERT(tv.type() == TypedValue::Type::Int, "ParallelStateOnFrame must be an int");
    auto *state = reinterpret_cast<ParallelStateOnFrame *>(tv.ValueInt());
    return std::unique_ptr<ParallelStateOnFrame>(state);
  }
};

}  // namespace memgraph::query
