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

#include "query/v2/interpret/multiframe.hpp"

#include "query/v2/context.hpp"
#include "query/v2/interpret/frame.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/vector.hpp"

using namespace memgraph::query::v2;

MultiFrame::MultiFrame(ExecutionContext *context, size_t number_of_frames) {
  frames_memory_owner_ = memgraph::utils::pmr::vector<std::unique_ptr<Frame>>(0, memgraph::utils::NewDeleteResource());
  frames_memory_owner_.reserve(number_of_frames);
  std::generate_n(std::back_inserter(frames_memory_owner_), number_of_frames,
                  [&context] { return std::make_unique<Frame>(context->symbol_table.max_position()); });

  frames_ = memgraph::utils::pmr::vector<Frame *>(0, memgraph::utils::NewDeleteResource());
  frames_.reserve(number_of_frames);
  std::transform(frames_memory_owner_.begin(), frames_memory_owner_.end(), std::back_inserter(frames_),
                 [](std::unique_ptr<Frame> &frame_uptr) { return frame_uptr.get(); });
}

MultiFrame::~MultiFrame() = default;

const memgraph::utils::pmr::vector<Frame *> &MultiFrame::GetFrames() const { return frames_; }

Frame &MultiFrame::GetFrame(size_t idx) const {
  MG_ASSERT(idx < frames_.size());
  return *frames_[idx];
}

void MultiFrame::ResetAll() {
  frames_ = memgraph::utils::pmr::vector<Frame *>(0, memgraph::utils::NewDeleteResource());
  frames_.reserve(frames_memory_owner_.size());

  for (auto &frame_uptr : frames_memory_owner_) {
    frame_uptr->MakeValid();
    frames_.emplace_back(frame_uptr.get());
  }
}

size_t MultiFrame::Size() const noexcept { return frames_.size(); }

void MultiFrame::Resize(size_t count) {
  MG_ASSERT(frames_.size() >= count);
  frames_.resize(count);
}

bool MultiFrame::Empty() const noexcept { return frames_.empty(); }

bool MultiFrame::HasValidFrames() const {
  return std::any_of(frames_.begin(), frames_.end(), [](auto *frame) { return frame->IsValid(); });
}

size_t MultiFrame::GetOriginalBatchSize() const noexcept { return frames_memory_owner_.size(); }
