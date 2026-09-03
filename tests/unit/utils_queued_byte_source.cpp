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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "utils/exceptions.hpp"
#include "utils/queued_byte_source.hpp"

using memgraph::utils::PushStreambuf;
using memgraph::utils::QueuedByteSource;

namespace {

constexpr std::size_t kBlocks = 4;

/// Reads the whole source with a buffer of `read_size`, which is deliberately not a divisor of the
/// block size in most tests so reads land inside blocks as well as on their edges.
auto DrainToString(QueuedByteSource &source, std::size_t read_size) -> std::string {
  std::string seen;
  std::vector<char> buffer(read_size);
  while (auto const read = source.Read(buffer.data(), buffer.size())) {
    seen.append(buffer.data(), read);
  }
  return seen;
}

auto Repeated(char c, std::size_t n) -> std::string { return std::string(n, c); }

}  // namespace

TEST(QueuedByteSourceTest, EveryByteAnUnevenTransferPushesIsReadBack) {
  auto const body = Repeated('a', 1000) + Repeated('b', 3) + Repeated('c', 50'000);

  QueuedByteSource source{kBlocks, [&body](auto const &push) {
                            // Pushed in uneven pieces, as a transfer hands over whatever arrived.
                            std::size_t offset = 0;
                            for (auto piece : {1U, 7U, 999U, 4096U, 65'536U}) {
                              if (offset >= body.size()) break;
                              auto const take = std::min<std::size_t>(piece, body.size() - offset);
                              if (!push(body.data() + offset, take)) return;
                              offset += take;
                            }
                            if (offset < body.size()) push(body.data() + offset, body.size() - offset);
                          }};

  EXPECT_EQ(DrainToString(source, 333), body);
}

TEST(QueuedByteSourceTest, ABodyLargerThanOneBlockCrossesTheBlockBoundaryIntact) {
  auto const body = Repeated('x', QueuedByteSource::kBlockBytes * 2 + 17);

  QueuedByteSource source{kBlocks, [&body](auto const &push) { push(body.data(), body.size()); }};

  auto const seen = DrainToString(source, 4096);
  ASSERT_EQ(seen.size(), body.size());
  EXPECT_EQ(seen, body);
}

TEST(QueuedByteSourceTest, ATransferThatPushesNothingIsExhaustedStraightAway) {
  QueuedByteSource source{kBlocks, [](auto const & /*push*/) {}};

  char byte = 0;
  EXPECT_EQ(source.Read(&byte, 1), 0U);
}

TEST(QueuedByteSourceTest, WhatTheTransferThrowsReachesTheReader) {
  QueuedByteSource source{kBlocks, [](auto const &push) {
                            std::string const head(QueuedByteSource::kBlockBytes, 'h');
                            push(head.data(), head.size());
                            throw std::runtime_error{"the transfer gave up"};
                          }};

  EXPECT_THROW(DrainToString(source, 1024), std::runtime_error);
}

TEST(QueuedByteSourceTest, TheTransferIsToldToStopWhenTheReaderGivesUp) {
  std::atomic<bool> told_to_stop{false};

  {
    QueuedByteSource source{1, [&told_to_stop](auto const &push) {
                              std::string const block(QueuedByteSource::kBlockBytes, 'z');
                              for (auto i = 0; i < 10'000; ++i) {
                                if (!push(block.data(), block.size())) {
                                  told_to_stop.store(true, std::memory_order_release);
                                  return;
                                }
                              }
                            }};

    char byte = 0;
    ASSERT_EQ(source.Read(&byte, 1), 1U);
    // Destroyed having read one byte of a body far larger than the queue can hold.
  }

  EXPECT_TRUE(told_to_stop.load(std::memory_order_acquire))
      << "the transfer should learn to stop through the value push returns";
}

TEST(QueuedByteSourceTest, ReadHandsBackTheBlocksThatArrivedRatherThanWaitingToFillTheBuffer) {
  std::atomic<bool> handed_over{false};
  std::atomic<bool> release{false};

  QueuedByteSource source{kBlocks, [&handed_over, &release](auto const &push) {
                            std::string const two_blocks(QueuedByteSource::kBlockBytes * 2, 'f');
                            push(two_blocks.data(), two_blocks.size());
                            // Both blocks are on the queue once push has returned.
                            handed_over.store(true, std::memory_order_release);
                            while (!release.load(std::memory_order_acquire)) {
                              std::this_thread::sleep_for(std::chrono::milliseconds{1});
                            }
                            std::string const tail(QueuedByteSource::kBlockBytes, 's');
                            push(tail.data(), tail.size());
                          }};

  while (!handed_over.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }

  // Asking for more than has arrived must not wait for the rest. Only the first block is worth
  // waiting for; after that a stalled transfer is one a cancellation would otherwise sit behind.
  std::vector<char> buffer(QueuedByteSource::kBlockBytes * 4);
  auto const read = source.Read(buffer.data(), buffer.size());
  EXPECT_EQ(read, QueuedByteSource::kBlockBytes * 2)
      << "the reader waited for the buffer to fill instead of taking the blocks that had arrived";
  EXPECT_EQ(std::count(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(read), 'f'), read);

  release.store(true, std::memory_order_release);
  auto const rest = DrainToString(source, QueuedByteSource::kBlockBytes);
  EXPECT_EQ(rest.size(), QueuedByteSource::kBlockBytes);
  EXPECT_EQ(std::count(rest.begin(), rest.end(), 's'), rest.size());
}

// A block is handed over once it is full, so a transfer that stops part way through one leaves those
// bytes waiting. Ending the transfer is what releases them, which is why a reader is never left
// holding a source that has more to give but will not give it.
TEST(QueuedByteSourceTest, BytesShortOfAWholeBlockAreHandedOverWhenTheTransferEnds) {
  QueuedByteSource source{kBlocks, [](auto const &push) {
                            std::string const partial(10, 'p');
                            push(partial.data(), partial.size());
                          }};

  EXPECT_EQ(DrainToString(source, 1024), Repeated('p', 10));
}

TEST(PushStreambufTest, WhatIsWrittenToTheStreamReachesThePush) {
  std::string received;
  PushStreambuf sink{[&received](char const *data, std::size_t size) {
                       received.append(data, size);
                       return true;
                     },
                     nullptr};

  std::ostream out{&sink};
  // A long write goes through xsputn; single characters go through overflow.
  out << Repeated('q', 5000);
  out.put('!');
  out.flush();

  EXPECT_EQ(received, Repeated('q', 5000) + "!");
}

TEST(PushStreambufTest, NothingIsReportedWhenTheTransferWasNotStopped) {
  PushStreambuf sink{[](char const *, std::size_t) { return true; }, nullptr};

  std::ostream out{&sink};
  out << "fine";
  out.flush();

  EXPECT_NO_THROW(sink.RethrowIfStopped());
}

TEST(PushStreambufTest, AWriteIsRefusedOnceTheCallersCheckStopsTheTransfer) {
  std::size_t written = 0;
  PushStreambuf sink{[&written](char const *, std::size_t size) {
                       written += size;
                       return true;
                     },
                     []() { throw std::runtime_error{"asked to stop"}; }};

  std::ostream out{&sink};
  out << Repeated('w', 100);
  out.flush();

  EXPECT_EQ(written, 0U) << "the write must be refused, which is how a transfer is told to stop";
  EXPECT_TRUE(out.fail()) << "refusing the write must be visible to whatever is writing";
}

// The reason the transfer ended lives here because it cannot be thrown through the library doing the
// writing. That library will report only that a write was refused, so this takes precedence.
TEST(PushStreambufTest, TheReasonTheCallersCheckGaveIsWhatIsReported) {
  PushStreambuf sink{[](char const *, std::size_t) { return true; },
                     []() { throw std::runtime_error{"asked to stop"}; }};

  std::ostream out{&sink};
  out << "anything";
  out.flush();

  try {
    sink.RethrowIfStopped();
    FAIL() << "the reason the transfer ended was lost";
  } catch (std::runtime_error const &e) {
    EXPECT_STREQ(e.what(), "asked to stop");
  }
}

TEST(PushStreambufTest, TheReasonIsReportedOnceRatherThanOnEveryAsk) {
  PushStreambuf sink{[](char const *, std::size_t) { return true; },
                     []() { throw std::runtime_error{"asked to stop"}; }};

  std::ostream out{&sink};
  out << "anything";
  out.flush();

  EXPECT_THROW(sink.RethrowIfStopped(), std::runtime_error);
  EXPECT_NO_THROW(sink.RethrowIfStopped());
}

TEST(PushStreambufTest, ATransferThatIsRefusedByTheSinkStopsWriting) {
  std::size_t offered = 0;
  PushStreambuf sink{[&offered](char const *, std::size_t size) {
                       offered += size;
                       return false;
                     },
                     nullptr};

  std::ostream out{&sink};
  out << Repeated('r', 100);
  out.flush();

  EXPECT_EQ(offered, 100U) << "the sink should have been offered the write once";
  EXPECT_TRUE(out.fail()) << "a refused write must be visible to whatever is writing";
}

// A source that pushes into a stream and a reader that pulls bytes are the two halves of a
// download, so they are checked together as well as apart.
TEST(QueuedByteSourceTest, AStreamWrittenThroughTheSinkIsReadBackByTheSource) {
  auto const body = Repeated('m', QueuedByteSource::kBlockBytes + 1234);

  QueuedByteSource source{kBlocks, [&body](auto const &push) {
                            PushStreambuf sink{push, nullptr};
                            std::ostream out{&sink};
                            out << body;
                            out.flush();
                          }};

  EXPECT_EQ(DrainToString(source, 777), body);
}
