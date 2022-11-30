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

MultiFrame::MultiFrame(FrameWithValidity default_frame, size_t number_of_frames,
                       utils::MemoryResource *execution_memory)
    : default_frame_(default_frame),
      frames_(utils::pmr::vector<FrameWithValidity>(number_of_frames, default_frame, execution_memory)) {
  MG_ASSERT(number_of_frames > 0);
  MG_ASSERT(!default_frame.IsValid());
}

MultiFrame::MultiFrame(const MultiFrame &other) : default_frame_(other.default_frame_) {
  /*
  TODO
  Do we just copy all frames or do we make distinctions between valid and not valid frames? Does it make any
  difference?
  */
  frames_.reserve(other.frames_.size());
  std::transform(other.frames_.begin(), other.frames_.end(), std::back_inserter(frames_),
                 [&default_frame = default_frame_](const auto &other_frame) {
                   if (other_frame.IsValid()) {
                     return other_frame;
                   }
                   return default_frame;
                 });
}

// NOLINTNEXTLINE (bugprone-exception-escape)
MultiFrame::MultiFrame(MultiFrame &&other) noexcept
    : default_frame_(std::move(other.default_frame_)), frames_(std::move(other.frames_)) {}

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
  return Iterator{&multiframe_.frames_[multiframe_.frames_.size()], *this};
}

ValidFramesModifier::ValidFramesModifier(MultiFrame &multiframe) : multiframe_(multiframe) {}

ValidFramesModifier::Iterator ValidFramesModifier::begin() { return Iterator{&multiframe_.frames_[0], *this}; }
ValidFramesModifier::Iterator ValidFramesModifier::end() {
  return Iterator{&multiframe_.frames_[multiframe_.frames_.size()], *this};
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
  return Iterator{&multiframe_.frames_[multiframe_.frames_.size()], *this};
}

InvalidFramesPopulator::InvalidFramesPopulator(MultiFrame &multiframe) : multiframe_(multiframe) {}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::begin() {
  for (auto idx = 0UL; idx < multiframe_.frames_.size(); ++idx) {
    if (!multiframe_.frames_[idx].IsValid()) {
      return Iterator{&multiframe_.frames_[idx]};
    }
  }

  return end();
}

InvalidFramesPopulator::Iterator InvalidFramesPopulator::end() {
  return Iterator{&multiframe_.frames_[multiframe_.frames_.size()]};
}

}  // namespace memgraph::query::v2
