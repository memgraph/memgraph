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

#include "query/v2/multiframe.hpp"

#include "query/v2/bindings/frame.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::query::v2 {

// #NoCommit uncomment https://github.com/memgraph/memgraph/pull/676#discussion_r1035704661
// static_assert(std::forward_iterator<ValidFramesReader::Iterator> &&
//               std::equality_comparable<ValidFramesReader::Iterator>);
// static_assert(std::forward_iterator<ValidFramesModifier::Iterator> &&
//               std::equality_comparable<ValidFramesModifier::Iterator>);
// static_assert(std::forward_iterator<ValidFramesConsumer::Iterator> &&
//               std::equality_comparable<ValidFramesConsumer::Iterator>);
// static_assert(std::forward_iterator<InvalidFramesPopulator::Iterator> &&
//               std::equality_comparable<InvalidFramesPopulator::Iterator>);

MultiFrame::MultiFrame(int64_t size_of_frame, size_t number_of_frames, utils::MemoryResource *execution_memory)
    : frames_(utils::pmr::vector<FrameWithValidity>(
          number_of_frames, FrameWithValidity(size_of_frame, execution_memory), execution_memory)) {
  MG_ASSERT(number_of_frames > 0);
}

MultiFrame::MultiFrame(const MultiFrame &other) {
  frames_.reserve(other.frames_.size());
  std::transform(other.frames_.begin(), other.frames_.end(), std::back_inserter(frames_),
                 [](const auto &other_frame) { return other_frame; });
}

// NOLINTNEXTLINE (bugprone-exception-escape)
MultiFrame::MultiFrame(MultiFrame &&other) noexcept : frames_(std::move(other.frames_)) {}

FrameWithValidity &MultiFrame::GetFirstFrame() {
  MG_ASSERT(!frames_.empty());
  return frames_.front();
}

void MultiFrame::MakeAllFramesInvalid() noexcept {
  std::for_each(frames_.begin(), frames_.end(), [](auto &frame) { frame.MakeInvalid(); });
}

bool MultiFrame::HasValidFrame() const noexcept {
  return std::any_of(frames_.begin(), frames_.end(), [](auto &frame) { return frame.IsValid(); });
}

// NOLINTNEXTLINE (bugprone-exception-escape)
void MultiFrame::DefragmentValidFrames() noexcept {
  /*
  from: https://en.cppreference.com/w/cpp/algorithm/remove
  "Removing is done by shifting (by means of copy assignment (until C++11)move assignment (since C++11)) the elements
  in the range in such a way that the elements that are not to be removed appear in the beginning of the range.
  Relative order of the elements that remain is preserved and the physical size of the container is unchanged."
  */
  [[maybe_unused]] const auto it =
      std::remove_if(frames_.begin(), frames_.end(), [](auto &frame) { return !frame.IsValid(); });
}

ValidFramesReader MultiFrame::GetValidFramesReader() { return ValidFramesReader{*this}; }

ValidFramesModifier MultiFrame::GetValidFramesModifier() { return ValidFramesModifier{*this}; }

ValidFramesConsumer MultiFrame::GetValidFramesConsumer() { return ValidFramesConsumer{*this}; }

InvalidFramesPopulator MultiFrame::GetInvalidFramesPopulator() { return InvalidFramesPopulator{*this}; }

ValidFramesReader::ValidFramesReader(MultiFrame &multiframe) : multiframe_(multiframe) {}

ValidFramesReader::Iterator ValidFramesReader::begin() { return Iterator{&multiframe_.frames_[0], *this}; }
ValidFramesReader::Iterator ValidFramesReader::end() {
  return Iterator{multiframe_.frames_.data() + multiframe_.frames_.size(), *this};
}

ValidFramesModifier::ValidFramesModifier(MultiFrame &multiframe) : multiframe_(multiframe) {}

ValidFramesModifier::Iterator ValidFramesModifier::begin() { return Iterator{&multiframe_.frames_[0], *this}; }
ValidFramesModifier::Iterator ValidFramesModifier::end() {
  return Iterator{multiframe_.frames_.data() + multiframe_.frames_.size(), *this};
}

ValidFramesConsumer::ValidFramesConsumer(MultiFrame &multiframe) : multiframe_(multiframe) {}

// NOLINTNEXTLINE (bugprone-exception-escape)
ValidFramesConsumer::~ValidFramesConsumer() noexcept {
  // TODO Possible optimisation: only DefragmentValidFrames if one frame has been invalidated? Only if does not
  // cost too much to store it
  multiframe_.DefragmentValidFrames();
}

ValidFramesConsumer::Iterator ValidFramesConsumer::begin() { return Iterator{&multiframe_.frames_[0], *this}; }

ValidFramesConsumer::Iterator ValidFramesConsumer::end() {
  return Iterator{multiframe_.frames_.data() + multiframe_.frames_.size(), *this};
}

InvalidFramesPopulator::InvalidFramesPopulator(MultiFrame &multiframe) : multiframe_(multiframe) {}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::begin() {
  for (auto &frame : multiframe_.frames_) {
    if (!frame.IsValid()) {
      return Iterator{&frame};
    }
  }
  return end();
}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::end() {
  return Iterator{multiframe_.frames_.data() + multiframe_.frames_.size()};
}

}  // namespace memgraph::query::v2
