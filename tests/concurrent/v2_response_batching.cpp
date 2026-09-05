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

// Response-batching correctness tests: a pipelined burst and a mid-burst threshold flush (64 KiB).
// Parametrized over both schedulers (asio -> OnReadAsio, priority_queue -> DoWork).

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "communication/context.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priorities.hpp"

namespace {

// kReplies separate Write() calls per frame exercise the response-batching path.
inline constexpr int kReplies = 10;

struct EchoContext {
  // The priority-queue scheduler routes the burst loop through AddTask; run it inline so that path
  // is exercised too. Passing the queued priority as the thread priority keeps them equal, so
  // DoWork drains the burst without taking the reschedule branch.
  template <typename Task>
  void AddTask(const Task &task, memgraph::utils::Priority priority) {
    task(priority);
  }
};

class EchoSession {
 public:
  EchoSession(EchoContext /*unused*/, memgraph::communication::v2::InputStream *input_stream,
              memgraph::communication::v2::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  // Returns true if a complete frame was processed; caller loops to drain pipelined frames.
  bool Execute() {
    if (input_stream_->size() < 2) return false;
    const uint8_t *hdr = input_stream_->data();
    const size_t size = (static_cast<size_t>(hdr[0]) << 8) + hdr[1];
    input_stream_->Resize(size + 2);
    if (input_stream_->size() < size + 2) return false;
    const uint8_t *payload = input_stream_->data() + 2;  // re-read: Resize may have reallocated
    for (int i = 0; i < kReplies; ++i) {
      if (!output_stream_->Write(payload, size)) return false;
    }
    input_stream_->Shift(size + 2);
    return true;
  }

  void HandleError() {}

  memgraph::utils::Priority ApproximateQueryPriority() const { return memgraph::utils::Priority::LOW; }

 private:
  memgraph::communication::v2::InputStream *input_stream_;
  memgraph::communication::v2::OutputStream *output_stream_;
};

using ServerT = memgraph::communication::v2::Server<EchoSession, EchoContext>;

// Build one [len][payload] frame; payload byte j = (tag + j) so frames are distinguishable.
std::vector<uint8_t> MakeFrame(uint16_t len, uint8_t tag) {
  std::vector<uint8_t> f;
  f.reserve(len + 2);
  f.push_back(static_cast<uint8_t>(len >> 8));
  f.push_back(static_cast<uint8_t>(len & 0xFF));
  for (uint16_t j = 0; j < len; ++j) f.push_back(static_cast<uint8_t>(tag + j));
  return f;
}

class ResponseBatchingTest : public ::testing::TestWithParam<const char *> {
 protected:
  void SetUp() override { gflags::SetCommandLineOption("scheduler", GetParam()); }
};

// The v2 server does not report its bound port for an ephemeral (:0) bind, so pick a
// concrete free port first. reuse_address on the server makes re-binding it safe.
uint16_t GetFreePort() {
  boost::asio::io_context ioc;
  boost::asio::ip::tcp::acceptor acceptor(ioc, {boost::asio::ip::make_address("127.0.0.1"), 0});
  const auto port = acceptor.local_endpoint().port();
  acceptor.close();
  return port;
}

std::vector<uint8_t> ReadExactly(memgraph::io::network::Socket &socket, size_t want) {
  std::vector<uint8_t> buf(want);
  size_t have = 0;
  while (have < want) {
    auto r = socket.Read(buf.data() + have, want - have);
    if (r <= 0) break;
    have += static_cast<size_t>(r);
  }
  EXPECT_EQ(have, want);
  buf.resize(have);
  return buf;
}

void ExpectReplies(const std::vector<uint8_t> &echoes, size_t &off, uint16_t len, uint8_t tag) {
  for (int r = 0; r < kReplies; ++r) {
    for (uint16_t j = 0; j < len; ++j) {
      ASSERT_LT(off, echoes.size());
      ASSERT_EQ(echoes[off], static_cast<uint8_t>(tag + j)) << "reply " << r << " byte " << j;
      ++off;
    }
  }
}

}  // namespace

// Several pipelined frames in one write: the batch coalesces all replies and delivers them intact.
TEST_P(ResponseBatchingTest, PipelinedBurstIsByteTransparent) {
  EchoContext ctx;
  memgraph::communication::ServerContext ctx_srv;
  const uint16_t port = GetFreePort();
  memgraph::communication::v2::ServerEndpoint endpoint{boost::asio::ip::make_address("127.0.0.1"), port};
  ServerT server(endpoint, &ctx, &ctx_srv, "Test", 2);
  ASSERT_TRUE(server.Start());
  memgraph::utils::OnScopeExit cleanup([&] {
    server.Shutdown();
    server.AwaitShutdown();
  });

  const std::vector<std::pair<uint16_t, uint8_t>> frames = {{7, 0x10}, {40, 0x40}, {3, 0xA0}, {200, 0x01}};
  std::vector<uint8_t> pipelined;
  for (auto [len, tag] : frames) {
    auto f = MakeFrame(len, tag);
    pipelined.insert(pipelined.end(), f.begin(), f.end());
  }

  memgraph::io::network::Socket socket;
  ASSERT_TRUE(socket.Connect(memgraph::io::network::Endpoint("127.0.0.1", port)));
  socket.SetTimeout(5, 0);
  ASSERT_TRUE(socket.Write(pipelined.data(), pipelined.size()));

  size_t expected = 0;
  for (auto [len, tag] : frames) expected += static_cast<size_t>(len) * kReplies;
  auto echoes = ReadExactly(socket, expected);
  ASSERT_EQ(echoes.size(), expected);

  size_t off = 0;
  for (auto [len, tag] : frames) ExpectReplies(echoes, off, len, tag);
  EXPECT_EQ(off, expected);

  socket.Close();
}

// Exercise both threshold paths in one burst: a frame whose small replies accumulate past the
// flush threshold (mid-buffer flush), then a frame whose replies each exceed it and are pushed
// straight through. The pending buffered bytes must flush before the pass-through, in order.
TEST_P(ResponseBatchingTest, ThresholdCrossingPreservesBytes) {
  EchoContext ctx;
  memgraph::communication::ServerContext ctx_srv;
  const uint16_t port = GetFreePort();
  memgraph::communication::v2::ServerEndpoint endpoint{boost::asio::ip::make_address("127.0.0.1"), port};
  ServerT server(endpoint, &ctx, &ctx_srv, "Test", 2);
  ASSERT_TRUE(server.Start());
  memgraph::utils::OnScopeExit cleanup([&] {
    server.Shutdown();
    server.AwaitShutdown();
  });

  // frame1: 10 * 1000 B accumulate past the 8 KiB threshold; frame2: 12000 B/reply passes through.
  const std::vector<std::pair<uint16_t, uint8_t>> frames = {{1000, 0x11}, {12000, 0x55}};
  std::vector<uint8_t> pipelined;
  for (auto [len, tag] : frames) {
    auto f = MakeFrame(len, tag);
    pipelined.insert(pipelined.end(), f.begin(), f.end());
  }

  memgraph::io::network::Socket socket;
  ASSERT_TRUE(socket.Connect(memgraph::io::network::Endpoint("127.0.0.1", port)));
  socket.SetTimeout(5, 0);
  ASSERT_TRUE(socket.Write(pipelined.data(), pipelined.size()));

  size_t expected = 0;
  for (auto [len, tag] : frames) expected += static_cast<size_t>(len) * kReplies;
  auto echoes = ReadExactly(socket, expected);
  ASSERT_EQ(echoes.size(), expected);

  size_t off = 0;
  for (auto [len, tag] : frames) ExpectReplies(echoes, off, len, tag);
  EXPECT_EQ(off, expected);

  socket.Close();
}

INSTANTIATE_TEST_SUITE_P(Schedulers, ResponseBatchingTest, ::testing::Values("asio", "priority_queue"));

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
