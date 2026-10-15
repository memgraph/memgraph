// Copyright 2026 Memgraph Ltd.
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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <functional>
#include <streambuf>
#include <utility>
#include <vector>

#include "utils/byte_source.hpp"
#include "utils/prefetched.hpp"

namespace memgraph::utils {

/// Turns a transfer that pushes bytes into a source that can be pulled from, gathering what it is
/// handed into blocks of a known size.
///
/// For a source that decides when to hand over data, such as a download, being read by code that
/// wants to ask for the next so many bytes. The thread, the queue and the failure channel belong to
/// Prefetched; what is here is the byte bookkeeping.
///
/// A block reaches the reader once it is full, or when the transfer ends, whichever comes first. A
/// transfer that trickles therefore keeps the first read waiting until it has handed over a whole
/// block. That is a wait on the reading side only: a caller that wants to stop the transfer does so
/// through the check it gave the sink, which runs on every write.
class QueuedByteSource final : public ByteSource {
 public:
  /// Runs on the transfer's own thread. `push` returns false once the reader has stopped, which is
  /// the signal to abandon the transfer.
  using Push = std::function<bool(char const *, std::size_t)>;
  using Transfer = std::function<void(Push const &)>;

  static constexpr std::size_t kBlockBytes = 256U * 1024U;

  QueuedByteSource(std::size_t queued_blocks, Transfer transfer)
      : blocks_{queued_blocks,
                [transfer = std::move(transfer)](auto const &push_block) { GatherIntoBlocks(transfer, push_block); }} {}

  auto Read(char *out, std::size_t size) -> std::size_t override {
    std::size_t taken = 0;
    while (taken < size) {
      if (offset_ == block_.size()) {
        offset_ = 0;
        block_.clear();
        // Wait only for the first block. Handing back the blocks that have arrived keeps rows
        // flowing instead of stalling until the buffer is full, which on a slow transfer is a wait
        // a cancel would sit behind.
        auto const got = taken == 0 ? blocks_.Next(block_) : blocks_.TryNext(block_);
        if (!got) break;
      }
      auto const take = std::min(size - taken, block_.size() - offset_);
      std::memcpy(out + taken, block_.data() + offset_, take);
      offset_ += take;
      taken += take;
    }
    return taken;
  }

 private:
  // A source chooses how much it hands over at a time, and a transfer's is a good deal smaller than
  // a parse chunk. Gathering writes into blocks of a known size makes the queue's depth mean a
  // predictable number of bytes, and saves an allocation for every small write a source makes.
  static void GatherIntoBlocks(Transfer const &transfer, Prefetched<std::vector<char>>::Push const &push_block) {
    std::vector<char> gathered;
    gathered.reserve(kBlockBytes);

    auto const hand_over = [&gathered, &push_block]() {
      if (gathered.empty()) return true;
      auto const accepted = push_block(std::move(gathered));
      gathered.clear();
      gathered.reserve(kBlockBytes);
      return accepted;
    };

    transfer([&gathered, &hand_over](char const *data, std::size_t size) {
      while (size > 0) {
        auto const take = std::min(size, kBlockBytes - gathered.size());
        gathered.insert(gathered.end(), data, data + take);
        data += take;
        size -= take;
        if (gathered.size() == kBlockBytes && !hand_over()) return false;
      }
      return true;
    });
    hand_over();
  }

  std::vector<char> block_;
  std::size_t offset_{0};
  // Declared last so everything the reading thread touches is constructed before that thread starts,
  // and destroyed only after it has been joined.
  Prefetched<std::vector<char>> blocks_;
};

/// Lets a library that writes into a stream feed a QueuedByteSource. Only the write side is used, so
/// there is nothing to put back and nowhere to seek.
class PushStreambuf final : public std::streambuf {
 public:
  PushStreambuf(QueuedByteSource::Push push, std::function<void()> abort_check)
      : push_{std::move(push)}, abort_check_{std::move(abort_check)} {}

  /// Reports why the transfer ended, if the caller's check ended it. A refused write is how that
  /// check reaches a library writing into this, so the transfer's own failure will say only that a
  /// write was refused; this is the reason behind it and takes precedence. Call it before reporting
  /// anything the transfer returned. Reports once.
  void RethrowIfStopped() {
    if (stopped_) {
      std::rethrow_exception(std::exchange(stopped_, nullptr));
    }
  }

 protected:
  auto xsputn(char const *s, std::streamsize n) -> std::streamsize override {
    if (n <= 0 || !KeepGoing()) return 0;
    return push_(s, static_cast<std::size_t>(n)) ? n : 0;
  }

  auto overflow(int_type ch) -> int_type override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) return traits_type::not_eof(ch);
    if (!KeepGoing()) return traits_type::eof();
    auto const byte = traits_type::to_char_type(ch);
    return push_(&byte, 1) ? ch : traits_type::eof();
  }

 private:
  // Refusing the write is the only way to stop a transfer that is driving this, so the reason is
  // kept rather than thrown through the library doing the writing.
  auto KeepGoing() -> bool {
    if (!abort_check_) return true;
    try {
      abort_check_();
    } catch (...) {
      stopped_ = std::current_exception();
      return false;
    }
    return true;
  }

  QueuedByteSource::Push push_;
  std::function<void()> abort_check_;
  std::exception_ptr stopped_;
};

}  // namespace memgraph::utils
