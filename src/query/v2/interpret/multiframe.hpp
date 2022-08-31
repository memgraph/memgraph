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
#include "query/v2/interpret/frame.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query::v2 {
class Frame;
struct ExecutionContext;
}  // namespace memgraph::query::v2

namespace memgraph::query::v2 {

class MultiFrame {
 public:
  explicit MultiFrame(ExecutionContext *context, size_t number_of_frames);
  ~MultiFrame();

  MultiFrame(const MultiFrame &other) = delete;      // copy constructor
  MultiFrame(MultiFrame &&other) noexcept = delete;  // move constructor

  // #NoCommit rule of 5

  // #NoCommit think about noexcept everywhere

  // #NoCommit We can work with iterator on MultiFrame that would iterate over frames_ directly
  // and we remove invalid frame on the go (instead of making them invalid)
  // Then the ResetAll would simply reset the iterator to the whole frames_memory_owner;
  const memgraph::utils::pmr::vector<Frame *> &GetFrames() const;

  Frame &GetFrame(size_t idx) const;

  /// Make all created frames valid and reset the number of available frames.
  void ResetAll();

  size_t Size() const noexcept;

  /// Shrink the available frames.
  ///
  /// @param count How many frames should we keep
  void Resize(size_t count);

  // #NoCommit do we need this method?
  bool Empty() const noexcept;

  bool HasValidFrames() const;

  size_t GetOriginalBatchSize() const noexcept;

 private:
  memgraph::utils::pmr::vector<std::unique_ptr<Frame>> frames_memory_owner_ =
      memgraph::utils::pmr::vector<std::unique_ptr<Frame>>(0, memgraph::utils::NewDeleteResource());
  memgraph::utils::pmr::vector<Frame *> frames_ =
      memgraph::utils::pmr::vector<Frame *>(0, memgraph::utils::NewDeleteResource());
};

}  // namespace memgraph::query::v2
