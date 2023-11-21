// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "slk/streams.hpp"

#include <cstring>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::slk {

Builder::Builder(std::function<void(const uint8_t *, size_t, bool)> write_func) : write_func_(std::move(write_func)) {}

void Builder::Save(const uint8_t *data, uint64_t size) {
  size_t offset = 0;
  while (size > 0) {
    FlushSegment(false);

    size_t to_write = size;
    if (to_write > kSegmentMaxDataSize - pos_) {
      to_write = kSegmentMaxDataSize - pos_;
    }

    memcpy(segment_ + sizeof(SegmentSize) + pos_, data + offset, to_write);

    size -= to_write;
    pos_ += to_write;

    offset += to_write;
  }
}

void Builder::Finalize() { FlushSegment(true); }

void Builder::FlushSegment(bool final_segment) {
  if (!final_segment && pos_ < kSegmentMaxDataSize) return;
  MG_ASSERT(pos_ > 0, "Trying to flush out a segment that has no data in it!");

  size_t total_size = sizeof(SegmentSize) + pos_;

  SegmentSize size = pos_;
  memcpy(segment_, &size, sizeof(SegmentSize));

  if (final_segment) {
    SegmentSize footer = 0;
    memcpy(segment_ + total_size, &footer, sizeof(SegmentSize));
    total_size += sizeof(SegmentSize);
  }

  write_func_(segment_, total_size, !final_segment);

  pos_ = 0;
}

Reader::Reader(const uint8_t *data, size_t size) : data_(data), size_(size) {}

void Reader::Load(uint8_t *data, uint64_t size) {
  size_t offset = 0;
  while (size > 0) {
    GetSegment();
    size_t to_read = size;
    if (to_read > have_) {
      to_read = have_;
    }
    memcpy(data + offset, data_ + pos_, to_read);
    pos_ += to_read;
    have_ -= to_read;
    offset += to_read;
    size -= to_read;
  }
}

void Reader::Finalize() { GetSegment(true); }

void Reader::GetSegment(bool should_be_final) {
  if (have_ != 0) {
    if (should_be_final) {
      throw SlkReaderException("There is still leftover data in the SLK stream!");
    }
    return;
  }

  // Load new segment.
  SegmentSize len = 0;
  if (pos_ + sizeof(SegmentSize) > size_) {
    throw SlkReaderException("Size data missing in SLK stream!");
  }
  memcpy(&len, data_ + pos_, sizeof(SegmentSize));

  if (should_be_final && len != 0) {
    throw SlkReaderException("Got a non-empty SLK segment when expecting the final segment!");
  }
  if (!should_be_final && len == 0) {
    throw SlkReaderException("Got an empty SLK segment when expecting a non-empty segment!");
  }

  // The position is incremented after the checks above so that the new
  // segment can be reread if some of the above checks fail.
  pos_ += sizeof(SegmentSize);

  if (pos_ + len > size_) {
    throw SlkReaderException("There isn't enough data in the SLK stream!");
  }
  have_ = len;
}

StreamInfo CheckStreamComplete(const uint8_t *data, size_t size) {
  size_t found_segments = 0;
  size_t data_size = 0;

  size_t pos = 0;
  while (true) {
    SegmentSize len = 0;
    if (pos + sizeof(SegmentSize) > size) {
      return {StreamStatus::PARTIAL, pos + kSegmentMaxTotalSize, data_size};
    }
    memcpy(&len, data + pos, sizeof(SegmentSize));
    pos += sizeof(SegmentSize);
    if (len == 0) {
      break;
    }

    if (pos + len > size) {
      return {StreamStatus::PARTIAL, pos + kSegmentMaxTotalSize, data_size};
    }
    pos += len;

    ++found_segments;
    data_size += len;
  }
  if (found_segments < 1) {
    return {StreamStatus::INVALID, 0, 0};
  }
  return {StreamStatus::COMPLETE, pos, data_size};
}

}  // namespace memgraph::slk
