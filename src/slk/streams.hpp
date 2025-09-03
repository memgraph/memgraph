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

#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <utility>

#include "utils/exceptions.hpp"

namespace memgraph::slk {

using SegmentSize = uint32_t;

// The maximum allowed size of a segment is set to `kSegmentMaxDataSize`. The
// value of 256 KiB was chosen so that the segment buffer will always fit on the
// stack (it mustn't be too large) and that it isn't too small so that most SLK
// messages fit into a single segment.
constexpr uint64_t kSegmentMaxDataSize = 262144;
constexpr uint64_t kSegmentMaxTotalSize =
    kSegmentMaxDataSize + sizeof(SegmentSize) + sizeof(SegmentSize);  // segment size + footer size
// We require that the segment which contains file data (also file name and file size) starts in its own segment
// To annotate that, we use mask 0xFFFFFFFF
constexpr SegmentSize kFileSegmentMask = std::numeric_limits<SegmentSize>::max();

static_assert(kSegmentMaxDataSize <= std::numeric_limits<SegmentSize>::max(),
              "The SLK segment can't be larger than the type used to store its size!");

/// SLK splits binary data into segments. Segments are used to avoid the need to
/// have all the encoded data in memory at once during the building process.
/// That enables streaming during the building process and makes the whole
/// process make zero memory allocations because only one static buffer is used.
/// During the reading process you must have all of the data in memory.
///
/// SLK segments are just chunks of binary data. They start with a `size` field
/// and then are followed by the binary data itself. The segments have a maximum
/// size of `kSegmentMaxDataSize`. The `size` field itself has a size of
/// `sizeof(SegmentSize)`. A segment of size 0 indicates that we have reached
/// the end of a stream and that there is no more data to be read/written.

class SlkBuilderException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(SlkBuilderException)
};

/// Builder used to create a SLK segment stream.
class Builder {
 public:
  explicit Builder(std::function<void(const uint8_t *, size_t, bool)> write_func);
  Builder(Builder &&other, std::function<void(const uint8_t *, size_t, bool)> write_func)
      : write_func_{std::move(write_func)}, pos_{std::exchange(other.pos_, 0)}, segment_{other.segment_} {
    other.write_func_ = [](const uint8_t *, size_t, bool) { /* Moved builder is defunct, no write possible */ };
  }

  /// Function used internally by SLK to serialize the data.
  void Save(const uint8_t *data, uint64_t size);

  void SaveFileBuffer(const uint8_t *data, uint64_t size);

  // Flushes the previous segment because sending the file requires that we start with the segment start. File data
  // cannot start in the middle of the segment.
  void PrepareForFileSending();

  /// Function that should be called after all `slk::Save` operations are done. This should be called only once,
  /// at the end of the message
  void Finalize();

  bool IsEmpty() const;

  size_t GetPos() const;

 private:
  /// @param final_segment: Whether the flush should finalize the message
  void FlushSegment(bool final_segment);
  void FlushFileSegment();
  bool file_data_{false};

  std::function<void(const uint8_t *, size_t, bool)> write_func_;
  size_t pos_{0};
  std::array<uint8_t, kSegmentMaxTotalSize> segment_;
};

/// Exception that will be thrown if segments can't be decoded from the byte
/// stream.
class SlkReaderException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(SlkReaderException)
};

class SlkReaderLeftoverDataException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(SlkReaderLeftoverDataException)
};

/// Reader used to read data from a SLK segment stream.
class Reader {
 public:
  Reader(const uint8_t *data, size_t size);

  Reader(const uint8_t *data, size_t size, size_t have);

  /// Function used internally by SLK to deserialize the data.
  void Load(uint8_t *data, uint64_t size);

  /// Function that should be called after all `slk::Load` operations are done.
  void Finalize();

  size_t GetPos() const;

  const uint8_t *GetData() const { return data_; }

 private:
  void GetSegment(bool should_be_final = false);

  const uint8_t *data_;
  size_t size_;

  size_t pos_{0};
  size_t have_{0};
};

/// Stream status that is returned by the `CheckStreamComplete` function.
enum class StreamStatus : uint8_t { PARTIAL, COMPLETE, INVALID, FILE_DATA };

/// Stream information retuned by the `CheckStreamComplete` function.
struct StreamInfo {
  StreamStatus status;
  size_t stream_size;
  size_t encoded_data_size;
  size_t pos;
};

/// This function checks the binary stream to see whether it contains a fully
/// received SLK segment stream. The function returns a `StreamInfo` structure
/// that has three members. The `status` member indicates in which state is the
/// received data (partially received, completely received or invalid), the
/// `stream_size` member indicates the size of the data stream (see NOTE) and
/// the `encoded_data_size` member indicates the size of the SLK encoded data in
/// the stream (so far).
/// NOTE: If the stream is partial, the size of the data stream returned will
/// not be the exact size of the received data. It will be a maximum expected
/// size of the data stream. It is used to indicate to the network stack how
/// much data it should receive before it makes sense to retry decoding of the
/// segment data.
StreamInfo CheckStreamStatus(const uint8_t *data, size_t size,
                             std::optional<uint64_t> remaining_file_size = std::nullopt);

}  // namespace memgraph::slk
