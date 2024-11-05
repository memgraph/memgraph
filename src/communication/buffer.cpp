// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/buffer.hpp"

#include "utils/logging.hpp"

#include <cstddef>
#include <vector>

namespace memgraph::communication {

Buffer::Buffer() : data_(kBufferInitialSize, 0), read_end_(this), write_end_(this) {}

Buffer::ReadEnd::ReadEnd(Buffer *buffer) : buffer_(buffer) {}

uint8_t *Buffer::ReadEnd::data() { return buffer_->data(); }

size_t Buffer::ReadEnd::size() const { return buffer_->size(); }

void Buffer::ReadEnd::Shift(size_t len) { buffer_->Shift(len); }

void Buffer::ReadEnd::Resize(size_t len) { buffer_->Resize(len); }

void Buffer::ReadEnd::Clear() { buffer_->Clear(); }

void Buffer::ReadEnd::ShrinkBuffer(size_t size) { buffer_->ShrinkBuffer(size); }

Buffer::WriteEnd::WriteEnd(Buffer *buffer) : buffer_(buffer) {}

io::network::StreamBuffer Buffer::WriteEnd::Allocate() { return buffer_->Allocate(); }

void Buffer::WriteEnd::Written(size_t len) { buffer_->Written(len); }

void Buffer::WriteEnd::Resize(size_t len) { buffer_->Resize(len); }

void Buffer::WriteEnd::Clear() { buffer_->Clear(); }

Buffer::ReadEnd *Buffer::read_end() { return &read_end_; }

Buffer::WriteEnd *Buffer::write_end() { return &write_end_; }

uint8_t *Buffer::data() { return data_.data(); }

size_t Buffer::size() const { return have_; }

void Buffer::Shift(size_t len) {
  DMG_ASSERT(len <= have_, "Tried to shift more data than the buffer has!");
  if (len == have_) {
    have_ = 0;
  } else {
    memmove(data_.data(), data_.data() + len, have_ - len);
    have_ -= len;
  }
}

io::network::StreamBuffer Buffer::Allocate() {
  DMG_ASSERT(have_ <= data_.size(),
             "The buffer thinks that there is more data "
             "in the buffer than there is underlying "
             "storage space!");
  return {data_.data() + have_, data_.size() - have_};
}

void Buffer::Written(size_t len) {
  have_ += len;
  DMG_ASSERT(have_ <= data_.size(), "Written more than storage has space!");
}

void Buffer::Resize(size_t len) {
  if (len <= data_.size()) return;
  data_.resize(len, 0);
}

void Buffer::Clear() { have_ = 0; }

void Buffer::ShrinkBuffer(size_t new_size) {
  if (data_.size() <= new_size) return;
  if (new_size < have_) return;

  data_.resize(new_size);
  data_.shrink_to_fit();
}

}  // namespace memgraph::communication
