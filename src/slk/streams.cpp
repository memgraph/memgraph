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

#include "slk/streams.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::slk {

Builder::Builder(std::function<void(const uint8_t *, size_t, bool)> write_func) : write_func_(std::move(write_func)) {}

bool Builder::IsEmpty() const { return pos_ == 0; }

void Builder::Save(const uint8_t *data, uint64_t size) {
  size_t offset = 0;
  while (size > 0) {
    FlushSegment(false);
    size_t const to_write = std::min(size, kSegmentMaxDataSize - pos_);

    if (file_data_) {
      memcpy(segment_.data() + pos_, data + offset, to_write);
    } else {
      memcpy(segment_.data() + sizeof(SegmentSize) + pos_, data + offset, to_write);
    }

    size -= to_write;
    pos_ += to_write;

    offset += to_write;
  }
}

// Differs from saving normal buffer by not leaving space of 4B at the beginning of the buffer for size
void Builder::SaveFileBuffer(const uint8_t *data, uint64_t size) {
  size_t offset = 0;
  while (size > 0) {
    FlushFileSegment();
    size_t const to_write = std::min(size, kSegmentMaxDataSize - pos_);
    memcpy(segment_.data() + pos_, data + offset, to_write);
    size -= to_write;
    pos_ += to_write;
    offset += to_write;
  }
}

// This should be invoked before preparing every file. The function writes kFileSegmentMask at the current position
void Builder::PrepareForFileSending() {
  memcpy(segment_.data() + pos_, &kFileSegmentMask, sizeof(SegmentSize));
  pos_ += sizeof(SegmentSize);
  file_data_ = true;
}

void Builder::Finalize() { FlushSegment(true); }

void Builder::FlushInternal(size_t const size, bool const has_more) {
  write_func_(segment_.data(), size, has_more);
  pos_ = 0;
}

// Flushes data and resets position
void Builder::FlushFileSegment() {
  if (pos_ < kSegmentMaxDataSize) return;
  MG_ASSERT(pos_ > 0, "Trying to flush out a segment that has no data in it!");
  FlushInternal(pos_, true);
}

void Builder::SaveFooter(uint64_t const total_size) {
  memcpy(segment_.data() + total_size, &kFooter, sizeof(SegmentSize));
}

void Builder::FlushSegment(bool const final_segment, bool const force_flush) {
  if (!force_flush && !final_segment && pos_ < kSegmentMaxDataSize) return;
  MG_ASSERT(pos_ > 0, "Trying to flush out a segment that has no data in it!");

  auto total_size = std::invoke([&]() -> size_t {
    if (!file_data_) {
      return sizeof(SegmentSize) + pos_;
    }
    return pos_;
  });

  if (!file_data_) {
    SegmentSize const data_size = pos_;
    memcpy(segment_.data(), &data_size, sizeof(SegmentSize));
  }

  if (final_segment) {
    SaveFooter(total_size);
    total_size += sizeof(SegmentSize);
  }

  FlushInternal(total_size, !final_segment);
}

bool Builder::GetFileData() const { return file_data_; }

Reader::Reader(const uint8_t *data, size_t const size) : data_(data), size_(size) {}

Reader::Reader(const uint8_t *data, size_t const size, size_t const have) : data_(data), size_(size), have_(have) {}

void Reader::Load(uint8_t *data, uint64_t size) {
  size_t offset = 0;
  while (size > 0) {
    GetSegment();
    size_t to_read = size;
    to_read = std::min(to_read, have_);
    memcpy(data + offset, data_ + pos_, to_read);
    pos_ += to_read;
    have_ -= to_read;
    offset += to_read;
    size -= to_read;
  }
}

size_t Reader::GetPos() const { return pos_; }

void Reader::Finalize() { GetSegment(true); }

void Reader::GetSegment(bool should_be_final) {
  if (have_ != 0) {
    if (should_be_final) {
      throw SlkReaderLeftoverDataException("There is still leftover data in the SLK stream!");
    }
    return;
  }

  // Load new segment.
  SegmentSize len = 0;
  if (pos_ + sizeof(SegmentSize) > size_) {
    throw SlkReaderException("Size data missing in SLK stream!");
  }
  memcpy(&len, data_ + pos_, sizeof(SegmentSize));

  // 4B after header and request could be file mask for WalFilesRpc, CurrentWalRpc and SnapshotRpc
  if (len == kFileSegmentMask) {
    if (should_be_final) {
      have_ = 0;
      pos_ += sizeof(SegmentSize);
      return;
    }
    throw SlkReaderException("Read kFileSegmentMask but the segment should not be final");
  }

  if (should_be_final && len != 0) {
    throw SlkReaderException(
        "Got a non-empty SLK segment when expecting the final segment! Have_: {}, Pos: {}, Size_: {}. Should be final: "
        "{}, Len: {}",
        have_, pos_, size_, should_be_final, len);
  }

  if (!should_be_final && len == 0) {
    throw SlkReaderException("Got an empty SLK segment when expecting a non-empty segment!");
  }

  // The position is incremented after the checks above so that the new
  // segment can be reread if some of the above checks fail.
  pos_ += sizeof(SegmentSize);

  if (pos_ + len > size_) {
    throw SlkReaderException("There isn't enough data in the SLK stream! Pos_ {}, len: {}, size_: {}", pos_, len,
                             size_);
  }
  have_ = len;
}

StreamInfo CheckStreamStatus(const uint8_t *data, size_t const size, std::optional<uint64_t> const &remaining_file_size,
                             size_t const processed_bytes) {
  size_t found_segments = 0;
  size_t data_size = 0;
  size_t pos = 0;

  while (true) {
    // This block handles 2 situations. The first one is if the whole buffer should be written into the file. In that
    // case remaining_file_size_val will be >= size, and we return FILE_DATA/ If not whole buffer should be written into
    // the file, then we remember the pos, increment found_segments and data_size and fallthrough
    if (remaining_file_size.has_value()) {
      auto const remaining_file_size_val = *remaining_file_size;
      if (remaining_file_size_val == 0) {
        return {.status = StreamStatus::INVALID, .stream_size = 0, .encoded_data_size = 0, .pos = pos};
      }

      if (remaining_file_size_val >= size) {
        return {.status = StreamStatus::FILE_DATA, .stream_size = size, .encoded_data_size = data_size, .pos = 0};
      }

      pos += remaining_file_size_val;
      ++found_segments;
      data_size += remaining_file_size_val;
    }

    // There are 2 possible situations in which we return partial status. The first one is improbable, and it happens
    // when the header+message request take more than 64KiB
    SegmentSize len = 0;
    if (pos + sizeof(SegmentSize) > size) {
      return {.status = StreamStatus::PARTIAL,
              .stream_size = pos + kSegmentMaxTotalSize,
              .encoded_data_size = data_size,
              .pos = pos};
    }

    memcpy(&len, data + pos, sizeof(SegmentSize));
    pos += sizeof(SegmentSize);

    // Start of the new segment
    if (len == kFileSegmentMask) {
      // Pos is important here and it points to the byte after the mask
      return {.status = StreamStatus::NEW_FILE, .stream_size = size, .encoded_data_size = data_size, .pos = pos};
    }

    if (len == kFooter) {
      break;
    }

    if (pos + len > size) {
      return {.status = StreamStatus::PARTIAL,
              .stream_size = pos + kSegmentMaxTotalSize,
              .encoded_data_size = data_size,
              .pos = pos};
    }

    pos += len;
    ++found_segments;
    data_size += len;
  }

  if (found_segments < 1 && processed_bytes == 0) {
    return {.status = StreamStatus::INVALID, .stream_size = 0, .encoded_data_size = 0, .pos = pos};
  }

  return {.status = StreamStatus::COMPLETE, .stream_size = pos, .encoded_data_size = data_size, .pos = pos};
}

}  // namespace memgraph::slk
