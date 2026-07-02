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

// Invariants for receiving a file-bearing RPC. The wire is
// [request-args segment][file segment...][footer]:
//   INV-1 (correctness): delivering the whole wire yields exactly the message
//          that was sent -- the callback fires once with the same files, byte for byte.
//   INV-2 (fragmentation independence): the parse result is identical no matter
//          where the byte stream is split across reads.
//   INV-3 (no premature completion): a strict prefix of the wire is never taken as
//          a complete message; the callback fires only once the last byte arrives.
//   INV-4 (well-formed input never errors): a valid wire never throws, for any split.
//   INV-5 (inter-message independence): consecutive messages on one connection
//          parse independently (SequentialMessagesOnOneConnection).

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "communication/buffer.hpp"
#include "communication/session.hpp"
#include "flags/general.hpp"
#include "io/network/endpoint.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/protocol.hpp"
#include "rpc/server.hpp"
#include "rpc/version.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "storage/v2/replication/serialization.hpp"

#include "rpc_messages.hpp"

using memgraph::communication::Buffer;
using memgraph::communication::OutputStream;
using memgraph::communication::ServerContext;
using memgraph::rpc::current_protocol_version;
using memgraph::rpc::FileReplicationHandler;
using memgraph::rpc::RpcMessageDeliverer;
using memgraph::rpc::SaveMessageHeader;
using memgraph::rpc::Server;
using memgraph::slk::Builder;
using memgraph::storage::replication::Encoder;

namespace fs = std::filesystem;

// SLK frames everything in units of a SegmentSize-wide field: a segment's length
// prefix, the file-segment mask, and the terminating footer are each this wide.
constexpr size_t kFrameWidth = sizeof(memgraph::slk::SegmentSize);

// Wires at or below this size are split at every byte (cheap, and the only way to
// hit file-to-file boundaries, which don't fall on a segment flush). Larger wires
// are split only around the structural boundaries to stay fast.
constexpr size_t kExhaustiveSplitLimit = 4096;

// A transfer scenario, named so the parameterised cases read clearly.
struct Scenario {
  std::string name;
  std::vector<size_t> file_sizes;  // one file per entry
  size_t args_pad = 0;             // extra bytes in the request-args segment (to make args > file metadata)
};

// The built wire plus the structural offsets captured while building it (not
// computed from encoding internals, so they can't drift).
struct Wire {
  std::vector<uint8_t> bytes;
  std::vector<std::pair<std::string, std::string>> expected;  // (filename, content) sent, in order
  size_t header_end = 0;                                      // end of the request-args segment / start of first mask
  size_t footer_start = 0;                                    // start of the trailing footer
  std::vector<size_t> flushes;  // cumulative size after each builder flush == segment edges
};

// The observable outcome of delivering a wire: what the parser produced.
struct ParseResult {
  bool threw = false;                                      // INV-4: a well-formed wire must not throw
  std::string error;                                       // exception message if it did
  bool completed_early = false;                            // INV-3: callback fired before the whole wire arrived
  int callbacks = 0;                                       // INV-1: exactly one delivery
  std::vector<std::pair<std::string, std::string>> files;  // (filename, content), in order
};

namespace {
std::string ReadFileBytes(const fs::path &p) {
  std::ifstream f{p, std::ios::binary};
  return std::string{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

// Compares received files to what was sent, reporting names, sizes and the first
// differing byte offset rather than dumping (potentially large) contents.
void ExpectFilesMatch(const std::vector<std::pair<std::string, std::string>> &got,
                      const std::vector<std::pair<std::string, std::string>> &want) {
  ASSERT_EQ(got.size(), want.size()) << "received " << got.size() << " files, expected " << want.size();
  for (size_t i = 0; i < want.size(); ++i) {
    EXPECT_EQ(got[i].first, want[i].first) << "file " << i << " name";
    auto const &g = got[i].second;
    auto const &e = want[i].second;
    if (g == e) continue;
    size_t off = 0;
    while (off < g.size() && off < e.size() && g[off] == e[off]) ++off;
    ADD_FAILURE() << "file '" << want[i].first << "' content differs: expected " << e.size() << " bytes, got "
                  << g.size() << " bytes, first difference at byte " << off;
  }
}

// INV-2: a fragmented delivery must parse to the same thing as the whole stream.
void ExpectSameParseAs(const ParseResult &got, const ParseResult &reference) {
  if (got.threw) {  // INV-4
    ADD_FAILURE() << "Execute() threw: " << got.error;
    return;
  }
  EXPECT_EQ(got.callbacks, reference.callbacks) << "delivery count differs from the whole-stream parse";
  ExpectFilesMatch(got.files, reference.files);
}

// Split points to exercise: every byte for small wires; otherwise the header/mask,
// mask/metadata, each segment edge (+/-1), and the interior of the footer.
std::vector<size_t> SplitsToTest(const Wire &w) {
  std::vector<size_t> out;
  auto add = [&](size_t k) {
    if (k > 0 && k < w.bytes.size()) out.push_back(k);
  };
  if (w.bytes.size() <= kExhaustiveSplitLimit) {
    for (size_t k = 1; k < w.bytes.size(); ++k) out.push_back(k);
    return out;
  }
  add(w.header_end);                // header | first file's mask
  add(w.header_end + kFrameWidth);  // mask | metadata
  for (size_t f : w.flushes) {      // each captured segment edge
    add(f - 1);
    add(f);
    add(f + 1);
  }
  for (size_t j = 1; j < kFrameWidth; ++j) add(w.footer_start + j);  // mid-footer
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

const Scenario kScenarios[] = {
    {"empty_file", {0}},
    {"one_byte", {1}},
    {"sub_segment", {memgraph::slk::kSegmentMaxDataSize - 1}},
    {"exact_segment", {memgraph::slk::kSegmentMaxDataSize}},
    {"over_segment", {memgraph::slk::kSegmentMaxDataSize + 1}},
    {"multi_segment", {2 * memgraph::slk::kSegmentMaxDataSize + 1}},
    {"two_small_files", {30, 40}},
    {"three_files_with_empty", {0, 64, 7}},
    {"big_args_short_meta", {2}, /*args_pad*/ 256},  // request args >> file metadata: guards the header capture
};
}  // namespace

class RpcFileFraming : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::temp_directory_path() / "mg_rpc_file_framing";
    fs::remove_all(root_);
    fs::create_directories(root_ / "wal");
    // OpenFile() saves received files under FLAGS_data_directory/<gp>/tmp/<type>.
    FLAGS_data_directory = (root_ / "recv").string();
  }

  void TearDown() override { fs::remove_all(root_); }

  // Builds the wire for a scenario exactly as the client does (header+args flushed
  // as their own segment, then the files, then the footer) and captures the
  // structural offsets along the way.
  Wire BuildWire(const Scenario &s) {
    Wire w;
    Builder builder([&w](const uint8_t *d, size_t n, bool) {
      w.bytes.insert(w.bytes.end(), d, d + n);
      w.flushes.push_back(w.bytes.size());
    });
    SaveMessageHeader({current_protocol_version, SumReq::kType.id, SumReq::kVersion}, &builder);
    if (s.args_pad != 0) memgraph::slk::Save(std::string(s.args_pad, 'p'), &builder);
    builder.FlushSegment(/*final_segment*/ false, /*force_flush*/ true);
    w.header_end = w.bytes.size();

    Encoder encoder{&builder};
    for (size_t i = 0; i < s.file_sizes.size(); ++i) {
      auto const name = s.name + "_" + std::to_string(i);
      std::string content(s.file_sizes[i], '\0');
      for (size_t j = 0; j < content.size(); ++j) content[j] = static_cast<char>((i * 131 + j) & 0xFF);
      {
        std::ofstream f{root_ / "wal" / name, std::ios::binary};
        f.write(content.data(), static_cast<std::streamsize>(content.size()));
      }
      encoder.WriteFile(root_ / "wal" / name, fs::path{"wal"} / name);
      w.expected.emplace_back(name, std::move(content));
    }
    builder.Finalize();
    w.footer_start = w.bytes.size() - kFrameWidth;
    return w;
  }

  fs::path root_;
};

class RpcFileFramingScenarios : public RpcFileFraming, public ::testing::WithParamInterface<Scenario> {};

TEST_P(RpcFileFramingScenarios, ParseIsIndependentOfFragmentation) {
  Wire const w = BuildWire(GetParam());

  ServerContext ctx;
  Server server{{"127.0.0.1", 0}, &ctx};
  int callbacks = 0;
  std::vector<std::pair<std::string, std::string>> received;
  // Read the received files inside the callback; the handler deletes them on
  // destruction, so they are gone once Execute returns.
  server.Register<Sum>([&](std::optional<FileReplicationHandler> const &frh,
                           uint64_t,
                           memgraph::slk::Reader *,
                           memgraph::slk::Builder *) {
    ++callbacks;
    received.clear();
    if (frh) {
      for (auto const &p : frh->GetActiveFileNames()) received.emplace_back(p.filename().string(), ReadFileBytes(p));
    }
  });

  // Delivers the wire as two fragments split at `split` (split == size means a
  // single read) and reports what the parser produced.
  auto Deliver = [&](size_t split) -> ParseResult {
    callbacks = 0;
    received.clear();
    Buffer buffer;
    OutputStream out{[](const uint8_t *, size_t, bool) { return true; }};
    memgraph::io::network::Endpoint ep{"127.0.0.1", 0};
    RpcMessageDeliverer deliverer{&server, ep, buffer.read_end(), &out};
    auto feed = [&](const uint8_t *data, size_t n) {
      buffer.write_end()->Resize(buffer.read_end()->size() + n + 8);
      auto sb = buffer.write_end()->GetBuffer();
      std::memcpy(sb.data, data, n);
      buffer.write_end()->Written(n);
      deliverer.Execute();
    };

    ParseResult r;
    try {
      feed(w.bytes.data(), split);
      r.completed_early = split < w.bytes.size() && callbacks > 0;
      if (split < w.bytes.size()) feed(w.bytes.data() + split, w.bytes.size() - split);
    } catch (const std::exception &e) {
      r.threw = true;
      r.error = e.what();
      return r;
    }
    r.callbacks = callbacks;
    r.files = received;
    return r;
  };

  // INV-1: delivering the whole wire in one read yields exactly what was sent.
  ParseResult const reference = Deliver(w.bytes.size());
  ASSERT_FALSE(reference.threw) << "whole-stream delivery threw: " << reference.error;
  EXPECT_EQ(reference.callbacks, 1);
  ExpectFilesMatch(reference.files, w.expected);

  // INV-2 / INV-3 / INV-4: every split parses the same as the whole stream, never
  // completes before the last byte, and never throws.
  for (size_t k : SplitsToTest(w)) {
    SCOPED_TRACE("split at byte " + std::to_string(k) + " of " + std::to_string(w.bytes.size()));
    ParseResult const got = Deliver(k);
    EXPECT_FALSE(got.completed_early) << "message completed before the whole wire arrived";  // INV-3
    ExpectSameParseAs(got, reference);                                                       // INV-2 (+ INV-4)
  }
}

INSTANTIATE_TEST_SUITE_P(Cases, RpcFileFramingScenarios, ::testing::ValuesIn(kScenarios),
                         [](const ::testing::TestParamInfo<Scenario> &info) { return info.param.name; });

// Two file-bearing messages over one connection: per-message state (consumed
// prefix, captured header, file handler) must reset so the second is not
// contaminated by the first. The parameterised tests use a fresh deliverer per
// split, so this is the only test of reuse across messages.
TEST_F(RpcFileFraming, SequentialMessagesOnOneConnection) {
  Wire const m1 = BuildWire({"seq_a", {1000}});
  Wire const m2 = BuildWire({"seq_b", {1500, 1500}});

  std::vector<size_t> counts;
  ServerContext ctx;
  Server server{{"127.0.0.1", 0}, &ctx};
  server.Register<Sum>([&counts](std::optional<FileReplicationHandler> const &frh,
                                 uint64_t,
                                 memgraph::slk::Reader *,
                                 memgraph::slk::Builder *) {
    if (frh) counts.push_back(frh->GetActiveFileNames().size());
  });

  Buffer buffer;
  OutputStream out{[](const uint8_t *, size_t, bool) { return true; }};
  memgraph::io::network::Endpoint ep{"127.0.0.1", 0};
  RpcMessageDeliverer deliverer{&server, ep, buffer.read_end(), &out};

  auto deliver_whole = [&](const std::vector<uint8_t> &wire) {
    buffer.write_end()->Resize(buffer.read_end()->size() + wire.size() + 8);
    auto sb = buffer.write_end()->GetBuffer();
    std::memcpy(sb.data, wire.data(), wire.size());
    buffer.write_end()->Written(wire.size());
    deliverer.Execute();
  };
  deliver_whole(m1.bytes);
  deliver_whole(m2.bytes);

  ASSERT_EQ(counts.size(), 2U);
  EXPECT_EQ(counts[0], 1U) << "first message";
  EXPECT_EQ(counts[1], 2U) << "second message (state leaked from first?)";
}
