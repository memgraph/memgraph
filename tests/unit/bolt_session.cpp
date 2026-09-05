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

#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "bolt_common.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/exceptions.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/access_type.hpp"  // storage::EngineLockMode (BeginTransaction engine-mode arg)
#include "utils/logging.hpp"

using memgraph::communication::bolt::ChunkedEncoderBuffer;
using memgraph::communication::bolt::ClientError;
using memgraph::communication::bolt::Encoder;
using memgraph::communication::bolt::Session;
using memgraph::communication::bolt::SessionException;
using memgraph::communication::bolt::State;
using memgraph::communication::bolt::Value;
using bolt_map_t = memgraph::communication::bolt::map_t;

static const char *kInvalidQuery = "invalid query";
static const char *kQueryReturn42 = "RETURN 42";
static const char *kQueryReturnMultiple = "UNWIND [1,2,3] as n RETURN n";
static const char *kQueryShowTx = "SHOW TRANSACTIONS";
static const char *kQueryEmpty = "no results";

class TestSessionContext {};

class TestSession final : public Session<TestInputStream, TestOutputStream> {
 public:
  using TEncoder = Encoder<ChunkedEncoderBuffer<TestOutputStream>>;

  TestSession(TestSessionContext *data, TestInputStream *input_stream, TestOutputStream *output_stream)
      : Session<TestInputStream, TestOutputStream>(input_stream, output_stream) {}

  memgraph::metrics::DatabaseMetricHandles *GetMetricHandles() { return nullptr; }

  // No trace stream needed; nullptr opts out of the per-message guard.
  memgraph::logging::SessionLogContext *GetLogContext() noexcept { return nullptr; }

  void InterpretParse(const std::string &query, bolt_map_t params, const bolt_map_t &extra) {
    if (extra.contains("tx_metadata")) {
      auto const &metadata = extra.at("tx_metadata").ValueMap();
      if (!metadata.empty()) md_ = metadata;
    }
    if (query == kQueryReturn42 || query == kQueryEmpty || query == kQueryReturnMultiple) {
      query_ = query;
      return;
    }
    if (query == kQueryShowTx) {
      if (md_.at("str").ValueString() != "aha" || md_.at("num").ValueInt() != 123) {
        throw ClientError("Wrong metadata!");
      }
      query_ = query;
      return;
    }
    query_ = "";
    throw ClientError("client sent invalid query");
  }

  std::pair<std::vector<std::string>, std::optional<int>> InterpretPrepare(
      memgraph::storage::EngineLockMode try_mode = memgraph::storage::EngineLockMode::Blocking) {
    // Pool bounded-try (mirrors BeginTransaction): lose the engine-lock race for the first N TryBounded
    // attempts, then succeed. The first loss bails HandlePrepare into State::PendingPrepare; the rest make
    // FinishPendingPrepare_ return Reschedule. Blocking (the fairness-cap fallback) is never gated, so the
    // reschedule loop is guaranteed to terminate.
    if (try_mode == memgraph::storage::EngineLockMode::TryBounded && try_bounded_fail_count_ > 0) {
      --try_bounded_fail_count_;
      throw memgraph::query::WouldBlockInlineException{};
    }
    if (query_ == kQueryReturn42 || query_ == kQueryEmpty || query_ == kQueryReturnMultiple) {
      return {{"result_name"}, {}};
    }
    if (query_ == kQueryShowTx) {
      return {{"username", "transaction_id", "query", "status", "metadata"}, {}};
    }
    throw ClientError("client sent invalid query");
  }

  bolt_map_t Pull(std::optional<int> n, std::optional<int> qid) {
    if (should_abort_) {
      throw memgraph::query::HintedAbortError(memgraph::query::AbortReason::TERMINATED);
    }
    if (query_ == kQueryReturn42) {
      encoder_.MessageRecord(std::vector<Value>{Value(42)});
      return {};
    } else if (query_ == kQueryEmpty) {
      return {};
    } else if (query_ == kQueryReturnMultiple) {
      static const std::array elements{1, 2, 3};
      static size_t global_counter = 0;

      int local_counter = 0;
      for (; global_counter < elements.size() && (!n || local_counter < *n); ++global_counter) {
        encoder_.MessageRecord(std::vector<Value>{Value(elements[global_counter])});
        ++local_counter;
      }

      if (global_counter == elements.size()) {
        global_counter = 0;
        return {std::pair("has_more", false)};
      }

      return {std::pair("has_more", true)};
    } else if (query_ == kQueryShowTx) {
      encoder_.MessageRecord({"", 1'234'567'890, query_, md_});
      return {};
    } else {
      throw ClientError("client sent invalid query");
    }
  }

  bolt_map_t Discard(std::optional<int> /*unused*/, std::optional<int> /*unused*/) { return {}; }

  void BeginTransaction(const bolt_map_t &extra,
                        memgraph::storage::EngineLockMode mode = memgraph::storage::EngineLockMode::Blocking) {
    // Pool bounded-try: simulate losing the engine-lock race for the first N attempts, then succeed. The
    // first loss happens inside HandleBegin (the BEGIN goes State::PendingBegin); the rest happen inside
    // FinishPendingBegin_ (each returns Reschedule). Blocking (the fairness-cap fallback) never throws
    // WouldBlock, so it is intentionally NOT gated here -- that is what guarantees the loop terminates.
    if (mode == memgraph::storage::EngineLockMode::TryBounded && try_bounded_fail_count_ > 0) {
      --try_bounded_fail_count_;
      throw memgraph::query::WouldBlockInlineException{};
    }
    if (extra.contains("tx_metadata")) {
      auto const &metadata = extra.at("tx_metadata").ValueMap();
      if (!metadata.empty()) md_ = metadata;
    }
  }

  bolt_map_t CommitTransaction() {
    md_.clear();
    return {};
  }

  void RollbackTransaction() { md_.clear(); }

  void Abort() { md_.clear(); }

  std::expected<void, memgraph::communication::bolt::AuthFailure> Authenticate(const std::string & /*username*/,
                                                                               const std::string & /*password*/) {
    return {/* success */};
  }

  std::expected<void, memgraph::communication::bolt::AuthFailure> SSOAuthenticate(const std::string & /*username*/,
                                                                                  const std::string & /*password*/) {
    return {/* success */};
  }

#ifdef MG_ENTERPRISE
  // Rejection carries the client-facing message rather than a bolt AuthFailure: the coordinator SSO causes (bad token,
  // unknown role, ungranted role) are specific to that path.
  std::expected<void, std::string_view> CoordinatorSSOAuthenticate(const std::string & /*scheme*/,
                                                                   const std::string & /*identity_provider_response*/) {
    return {/* success */};
  }

  void CoordinatorPassthroughAuthenticate() {}

  std::optional<bool> CoordinatorHasWritableRole() const { return std::nullopt; }
#endif

  void LogOff() {}

#ifdef MG_ENTERPRISE
  auto Route(bolt_map_t const & /*routing*/, std::vector<memgraph::communication::bolt::Value> const & /*bookmarks*/,
             std::optional<std::string> const & /*db*/, bolt_map_t const & /*extra*/) -> bolt_map_t {
    return {};
  }
#endif

  std::optional<std::string> GetServerNameForInit() { return std::nullopt; }

  void Configure(const bolt_map_t &) {}

  std::string GetCurrentDB() const { return ""; }

  void TestHook_ShouldAbort() { should_abort_ = true; }

  // Make the next `n` TryBounded engine-lock acquires lose the race, then succeed. Drives the HandleBegin /
  // HandlePrepare bail + FinishPendingBegin_ / FinishPendingPrepare_ reschedule paths deterministically.
  void TestHook_FailBoundedTries(int n) { try_bounded_fail_count_ = n; }

  // Simulate an idle pool (no queued work): AdmissionEngineLockMode returns Blocking, bypassing the
  // TryBounded + reschedule path entirely.
  void TestHook_SetPoolIdle() { pool_has_pending_work_ = false; }

  bool PoolHasPendingWork() const noexcept { return pool_has_pending_work_; }

  memgraph::storage::EngineLockMode AdmissionEngineLockMode() const noexcept {
    return pool_has_pending_work_ ? memgraph::storage::EngineLockMode::TryBounded
                                  : memgraph::storage::EngineLockMode::Blocking;
  }

  void Execute() {
    while (Execute_(*this)) {
      // Execute now exists on result, so it can be schduled again.
      // No scheduler here, just loop until done
    }
  }

  // Run the dechunk loop exactly once (one Execute_ pass), mirroring communication/v2 DoWork's single
  // Execute() call so a test can observe the mid-batch bail into State::PendingBegin. The naive Execute()
  // above would keep re-entering while the BEGIN is parked, so the pending-BEGIN path must be stepped.
  bool ExecuteStep() { return Execute_(*this); }

  memgraph::communication::bolt::PendingBeginOutcome FinishPendingBegin() { return FinishPendingBegin_(*this); }

  memgraph::communication::bolt::PendingPrepareOutcome FinishPendingPrepare() { return FinishPendingPrepare_(*this); }

 private:
  std::string query_;
  bolt_map_t md_;
  bool should_abort_ = false;
  int try_bounded_fail_count_{0};
  // Default true: existing reschedule tests exercise the TryBounded path unchanged.
  bool pool_has_pending_work_{true};
};

// TODO: This could be done in fixture.
// Shortcuts for writing variable initializations in tests
#define INIT_VARS                                                       \
  TestInputStream input_stream;                                         \
  TestOutputStream output_stream;                                       \
  TestSessionContext session_context;                                   \
  TestSession session(&session_context, &input_stream, &output_stream); \
  std::vector<uint8_t> &output = output_stream.output;

// Sample testdata that has correct inputs and outputs.
inline constexpr uint8_t handshake_req[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
                                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
inline constexpr uint8_t handshake_resp[] = {0x00, 0x00, 0x00, 0x01};
inline constexpr uint8_t init_req[] = {0xb2, 0x01, 0xd0, 0x15, 0x6c, 0x69, 0x62, 0x6e, 0x65, 0x6f, 0x34, 0x6a, 0x2d,
                                       0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x31, 0xa3,
                                       0x86, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x65, 0x85, 0x62, 0x61, 0x73, 0x69, 0x63,
                                       0x89, 0x70, 0x72, 0x69, 0x6e, 0x63, 0x69, 0x70, 0x61, 0x6c, 0x80, 0x8b, 0x63,
                                       0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x80};
inline constexpr uint8_t init_resp[] = {0x00, 0x18, 0xb1, 0x70, 0xa1, 0x8d, 0x63, 0x6f, 0x6e, 0x6e,
                                        0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x86,
                                        0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x31, 0x00, 0x00};
inline constexpr uint8_t run_req_header[] = {0xb2, 0x10, 0xd1};
inline constexpr uint8_t pullall_req[] = {0xb0, 0x3f};
inline constexpr uint8_t discardall_req[] = {0xb0, 0x2f};
inline constexpr uint8_t reset_req[] = {0xb0, 0x0f};
inline constexpr uint8_t ackfailure_req[] = {0xb0, 0x0e};
inline constexpr uint8_t success_resp[] = {0x00, 0x03, 0xb1, 0x70, 0xa0, 0x00, 0x00};
inline constexpr uint8_t ignored_resp[] = {0x00, 0x02, 0xb0, 0x7e, 0x00, 0x00};

namespace v4 {
inline constexpr uint8_t handshake_req[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
                                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
inline constexpr uint8_t handshake_resp[] = {0x00, 0x00, 0x00, 0x04};
inline constexpr uint8_t init_req[] = {
    0xb1, 0x01, 0xa5, 0x8a, 0x75, 0x73, 0x65, 0x72, 0x5f, 0x61, 0x67, 0x65, 0x6e, 0x74, 0xd0, 0x2f, 0x6e, 0x65, 0x6f,
    0x34, 0x6a, 0x2d, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x2f, 0x34, 0x2e, 0x31, 0x2e, 0x31, 0x20, 0x50, 0x79, 0x74,
    0x68, 0x6f, 0x6e, 0x2f, 0x33, 0x2e, 0x37, 0x2e, 0x33, 0x2d, 0x66, 0x69, 0x6e, 0x61, 0x6c, 0x2d, 0x30, 0x20, 0x28,
    0x6c, 0x69, 0x6e, 0x75, 0x78, 0x29, 0x86, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x65, 0x85, 0x62, 0x61, 0x73, 0x69, 0x63,
    0x89, 0x70, 0x72, 0x69, 0x6e, 0x63, 0x69, 0x70, 0x61, 0x6c, 0x80, 0x8b, 0x63, 0x72, 0x65, 0x64, 0x65, 0x6e, 0x74,
    0x69, 0x61, 0x6c, 0x73, 0x80, 0x87, 0x72, 0x6f, 0x75, 0x74, 0x69, 0x6e, 0x67, 0xa1, 0x87, 0x61, 0x64, 0x64, 0x72,
    0x65, 0x73, 0x73, 0x8e, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x3a, 0x37, 0x36, 0x38, 0x37};

inline constexpr uint8_t init_resp[] = {0x00, 0x18, 0xb1, 0x70, 0xa1, 0x8d, 0x63, 0x6f, 0x6e, 0x6e,
                                        0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x86,
                                        0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x31, 0x00, 0x00};
inline constexpr uint8_t run_req_header[] = {0xb3, 0x10, 0xd1};
inline constexpr uint8_t pullall_req[] = {0xb1, 0x3f, 0xa0};
inline constexpr uint8_t pull_one_req[] = {0xb1, 0x3f, 0xa1, 0x81, 0x6e, 0x01};
inline constexpr uint8_t reset_req[] = {0xb0, 0x0f};
inline constexpr uint8_t goodbye[] = {0xb0, 0x02};
inline constexpr uint8_t rollback[] = {0xb0, 0x13};
}  // namespace v4

namespace v4_1 {
inline constexpr uint8_t handshake_req[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x01, 0x04, 0x00, 0x00,
                                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
inline constexpr uint8_t handshake_resp[] = {0x00, 0x00, 0x01, 0x04};
inline constexpr uint8_t noop[] = {0x00, 0x00};
}  // namespace v4_1

namespace v4_3 {
inline constexpr uint8_t handshake_req[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x03, 0x04, 0x00, 0x00,
                                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
inline constexpr uint8_t handshake_resp[] = {0x00, 0x00, 0x03, 0x04};
inline constexpr uint8_t route[] = {
    0xb3,  // struct with 3 fields
    0x66,  // ROUTE signature
    0xa0,  // empty map {}
    0x90,  // empty list []
    0x85,
    0x6e,
    0x65,
    0x6f,
    0x34,
    0x6a  // "neo4j"
};
constexpr std::string_view extra_w_metadata =
    "\xa2"                                              // Map size 2
    "\x8b\x74\x78\x5f\x6d\x65\x74\x61\x64\x61\x74\x61"  // "tx_metadata"
    "\xa2"                                              // Map size 2
    "\x83\x73\x74\x72"                                  // "str"
    "\x83\x61\x68\x61"                                  // "aha"
    "\x83\x6e\x75\x6d"                                  // "num"
    "\x7b"                                              // 123
    "\x8a\x74\x78\x5f\x74\x69\x6d\x65\x6f\x75\x74"      // "tx_timeout"
    "\xc9\x07\xd0";                                     // INT_16 2000

constexpr std::string_view extra_w_127ms_timeout =
    "\xa1"                                          // Map size 1
    "\x8a\x74\x78\x5F\x74\x69\x6D\x65\x6F\x75\x74"  // String size 10 "tx_timeout"
    "\x7f";                                         // Integer 127 (representing 127ms)

inline constexpr uint8_t commit[] = {0xb0, 0x12};
}  // namespace v4_3

// Write bolt chunk header (length)
void WriteChunkHeader(TestInputStream &input_stream, uint16_t len) {
  len = memgraph::utils::HostToBigEndian(len);
  input_stream.Write(reinterpret_cast<uint8_t *>(&len), sizeof(len));
}

// Write bolt chunk tail (two zeros)
void WriteChunkTail(TestInputStream &input_stream) { WriteChunkHeader(input_stream, 0); }

// Check that the server responded with a failure message.
void CheckFailureMessage(std::vector<uint8_t> &output) {
  ASSERT_GE(output.size(), 6);
  // skip the first two bytes because they are the chunk header
  ASSERT_EQ(output[2], 0xB1);  // tiny struct 1
  ASSERT_EQ(output[3], 0x7F);  // signature failure
  output.clear();
}

// Check that the server responded with a success message.
void CheckSuccessMessage(std::vector<uint8_t> &output, bool clear = true) {
  ASSERT_GE(output.size(), 6);
  // skip the first two bytes because they are the chunk header
  ASSERT_EQ(output[2], 0xB1);  // tiny struct 1
  ASSERT_EQ(output[3], 0x70);  // signature success
  if (clear) {
    output.clear();
  }
}

// Check that the server responded with a ignore message.
void CheckIgnoreMessage(std::vector<uint8_t> &output) {
  ASSERT_GE(output.size(), 6);
  // skip the first two bytes because they are the chunk header
  ASSERT_EQ(output[2], 0xB0);
  ASSERT_EQ(output[3], 0x7E);  // signature ignore
  output.clear();
}

// Execute and check a correct handshake
void ExecuteHandshake(TestInputStream &input_stream, TestSession &session, std::vector<uint8_t> &output,
                      const uint8_t *request = handshake_req, const uint8_t *expected_resp = handshake_resp) {
  input_stream.Write(request, 20);
  session.Execute();
  ASSERT_EQ(session.state_, State::Init);
  PrintOutput(output);
  auto to_validate = std::span<uint8_t const>{output};
  CheckOutput(to_validate, expected_resp, 4);
  output.clear();
}

// Write bolt chunk and execute command
void ExecuteCommand(TestInputStream &input_stream, TestSession &session, const uint8_t *data, size_t len,
                    bool chunk = true) {
  if (chunk) WriteChunkHeader(input_stream, len);
  input_stream.Write(data, len);
  if (chunk) WriteChunkTail(input_stream);
  session.Execute();
}

// Execute and check a correct init
void ExecuteInit(TestInputStream &input_stream, TestSession &session, std::vector<uint8_t> &output,
                 const bool is_v4 = false) {
  const auto *request = is_v4 ? v4::init_req : init_req;
  const auto request_size = is_v4 ? sizeof(v4::init_req) : sizeof(init_req);
  ExecuteCommand(input_stream, session, request, request_size);
  ASSERT_EQ(session.state_, State::Idle);
  PrintOutput(output);
  const auto *response = is_v4 ? v4::init_resp : init_resp;
  auto to_validate = std::span<uint8_t const>{output};
  CheckOutput(to_validate, response, 28);
  output.clear();
}

// Write bolt encoded run request
void WriteRunRequest(TestInputStream &input_stream, const char *str, const bool is_v4 = false,
                     std::string_view extra = "\xA0") {
  // write chunk header
  auto len = strlen(str);
  WriteChunkHeader(input_stream, (3 + is_v4 * extra.size()) + 2 + len + 1);

  const auto *run_header = is_v4 ? v4::run_req_header : run_req_header;
  const auto run_header_size = is_v4 ? sizeof(v4::run_req_header) : sizeof(run_req_header);
  // write string header
  input_stream.Write(run_header, run_header_size);

  // write string length
  WriteChunkHeader(input_stream, len);

  // write string
  input_stream.Write(str, len);

  // write empty map for parameters
  input_stream.Write("\xA0", 1);  // TinyMap0

  if (is_v4) {
    // write empty map for extra field
    input_stream.Write(extra.data(), extra.size());  // TinyMap
  }

  // write chunk tail
  WriteChunkTail(input_stream);
}

TEST(BoltSession, HandshakeWrongPreamble) {
  INIT_VARS;

  // write 0x00000001 five times
  for (int i = 0; i < 5; ++i) input_stream.Write(handshake_req + 4, 4);
  ASSERT_THROW(session.Execute(), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  PrintOutput(output);
  CheckFailureMessage(output);
}

TEST(BoltSession, HandshakeInTwoPackets) {
  INIT_VARS;

  input_stream.Write(handshake_req, 10);
  session.Execute();

  ASSERT_EQ(session.state_, State::Handshake);

  input_stream.Write(handshake_req + 10, 10);
  session.Execute();

  ASSERT_EQ(session.state_, State::Init);
  PrintOutput(output);
  auto to_validate = std::span<uint8_t const>{output};
  CheckOutput(to_validate, handshake_resp, 4);
  output.clear();
}

TEST(BoltSession, HandshakeWriteFail) {
  INIT_VARS;
  output_stream.SetWriteSuccess(false);
  ASSERT_THROW(ExecuteCommand(input_stream, session, handshake_req, sizeof(handshake_req), false), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  ASSERT_EQ(output.size(), 0);
}

TEST(BoltSession, HandshakeOK) {
  INIT_VARS;
  ExecuteHandshake(input_stream, session, output);
}

TEST(BoltSession, HandshakeMultiVersionRequest) {
  // Should pick the first version, 4.0, even though a higher version is present
  // but with a lower priority
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
                                        0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t priority_response[] = {0x00, 0x00, 0x00, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 0);
    ASSERT_EQ(session.version_.major, 4);
  }

  // Should pick the second version, 4.1, because first, 3.0, is not supported
  {
    INIT_VARS;
    const uint8_t unsupported_first_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                                                 0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t unsupported_first_response[] = {0x00, 0x00, 0x01, 0x04};
    ExecuteHandshake(input_stream, session, output, unsupported_first_request, unsupported_first_response);
    ASSERT_EQ(session.version_.minor, 1);
    ASSERT_EQ(session.version_.major, 4);
  }

  // No supported version present in the request
  {
    INIT_VARS;
    const uint8_t no_supported_versions_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                                                     0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    ASSERT_THROW(ExecuteHandshake(input_stream, session, output, no_supported_versions_request), SessionException);
  }
}

TEST(BoltSession, HandshakeWithVersionOffset) {
  // It pick the versions depending on the offset given by the second byte
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x03, 0x03, 0x04, 0x00, 0x00,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t priority_response[] = {0x00, 0x00, 0x03, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 3);
    ASSERT_EQ(session.version_.major, 4);
  }
  // This should pick 4.4 version since 4.5 is not existant
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x03, 0x05, 0x04, 0x00, 0x00,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t priority_response[] = {0x00, 0x00, 0x04, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 4);
    ASSERT_EQ(session.version_.major, 4);
  }
  // With multiple offsets (added v5.2)
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x03, 0x03, 0x07, 0x00, 0x03,
                                        0x03, 0x06, 0x00, 0x03, 0x03, 0x05, 0x00, 0x03, 0x03, 0x04};
    const uint8_t priority_response[] = {0x00, 0x00, 0x02, 0x05};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 2);
    ASSERT_EQ(session.version_.major, 5);
  }
  // Offset overflows
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x07, 0x06, 0x04, 0x00, 0x00,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t priority_response[] = {0x00, 0x00, 0x04, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 4);
    ASSERT_EQ(session.version_.major, 4);
  }
  // Using offset but no version supported
  {
    INIT_VARS;
    const uint8_t no_supported_versions_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x03, 0x10, 0x04, 0x00, 0x00,
                                                     0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    ASSERT_THROW(ExecuteHandshake(input_stream, session, output, no_supported_versions_request), SessionException);
  }
}

TEST(BoltSession, InitWrongSignature) {
  INIT_VARS;
  ExecuteHandshake(input_stream, session, output);
  ASSERT_THROW(ExecuteCommand(input_stream, session, run_req_header, sizeof(run_req_header)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, InitWrongMarker) {
  INIT_VARS;
  ExecuteHandshake(input_stream, session, output);

  // wrong marker, good signature
  uint8_t data[2] = {0x00, init_req[1]};
  ASSERT_THROW(ExecuteCommand(input_stream, session, data, 2), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, InitMissingData) {
  // test lengths, they test the following situations:
  // missing header data, missing client name, missing metadata
  int len[] = {1, 2, 25};

  for (int i = 0; i < 3; ++i) {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, init_req, len[i]), SessionException);

    ASSERT_EQ(session.state_, State::Close);
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, InitWriteFail) {
  INIT_VARS;
  ExecuteHandshake(input_stream, session, output);
  output_stream.SetWriteSuccess(false);
  ASSERT_THROW(ExecuteCommand(input_stream, session, init_req, sizeof(init_req)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  ASSERT_EQ(output.size(), 0);
}

TEST(BoltSession, InitOK) {
  {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
  }
  {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
  }
}

TEST(BoltSession, ExecuteRunWrongMarker) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  // wrong marker, good signature
  uint8_t data[2] = {0x00, run_req_header[1]};
  ASSERT_THROW(ExecuteCommand(input_stream, session, data, sizeof(data)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, ExecuteRunMissingData) {
  std::array<uint8_t, 6> run_req_without_parameters{
      run_req_header[0], run_req_header[1], run_req_header[2], 0x00, 0x00, 0x00};
  // test lengths, they test the following situations:
  // missing header data, missing query data, missing parameters
  int len[] = {1, 2, run_req_without_parameters.size()};
  for (int i = 0; i < 3; ++i) {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, run_req_without_parameters.data(), len[i]), SessionException);

    ASSERT_EQ(session.state_, State::Close);
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, ExecuteRunBasicException) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);

    output_stream.SetWriteSuccess(i == 0);
    WriteRunRequest(input_stream, kInvalidQuery);
    if (i == 0) {
      session.Execute();
    } else {
      ASSERT_THROW(session.Execute(), SessionException);
    }

    if (i == 0) {
      ASSERT_EQ(session.state_, State::Error);
      CheckFailureMessage(output);
    } else {
      ASSERT_EQ(session.state_, State::Close);
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ExecuteRunWithoutPullAll) {
  // v1
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);

    WriteRunRequest(input_stream, kQueryReturn42);
    session.Execute();

    ASSERT_EQ(session.state_, State::Result);
  }

  // v4+
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
    ExecuteInit(input_stream, session, output, true);

    WriteRunRequest(input_stream, kQueryReturn42, true);
    session.Execute();

    ASSERT_EQ(session.state_, State::Result);
  }
}

TEST(BoltSession, ExecutePullAllDiscardAllResetWrongMarker) {
  // This test first tests PULL_ALL then DISCARD_ALL and then RESET
  // It tests for missing data in the message header
  const uint8_t *dataset[3] = {pullall_req, discardall_req, reset_req};

  for (int i = 0; i < 3; ++i) {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);

    // wrong marker, good signature
    uint8_t data[2] = {0x00, dataset[i][1]};
    ASSERT_THROW(ExecuteCommand(input_stream, session, data, sizeof(data)), SessionException);

    ASSERT_EQ(session.state_, State::Close);
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, ExecutePullAllBufferEmpty) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);

    output_stream.SetWriteSuccess(i == 0);
    ASSERT_THROW(ExecuteCommand(input_stream, session, pullall_req, sizeof(pullall_req)), SessionException);

    ASSERT_EQ(session.state_, State::Close);
    if (i == 0) {
      CheckFailureMessage(output);
    } else {
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ExecutePullAllDiscardAllReset) {
  // This test first tests PULL_ALL then DISCARD_ALL and then RESET
  // It tests a good message
  {
    const uint8_t *dataset[3] = {pullall_req, discardall_req, reset_req};

    for (int i = 0; i < 3; ++i) {
      // first test with socket write success, then with socket write fail
      for (int j = 0; j < 2; ++j) {
        INIT_VARS;

        ExecuteHandshake(input_stream, session, output);
        ExecuteInit(input_stream, session, output);
        WriteRunRequest(input_stream, kQueryReturn42);
        session.Execute();

        if (j == 1) output.clear();

        output_stream.SetWriteSuccess(j == 0);
        if (j == 0) {
          ExecuteCommand(input_stream, session, dataset[i], 2);
        } else {
          ASSERT_THROW(ExecuteCommand(input_stream, session, dataset[i], 2), SessionException);
        }

        if (j == 0) {
          ASSERT_EQ(session.state_, State::Idle);
          ASSERT_FALSE(session.encoder_buffer_.HasData());
          PrintOutput(output);
        } else {
          ASSERT_EQ(session.state_, State::Close);
          ASSERT_EQ(output.size(), 0);
        }
      }
    }
  }
}

TEST(BoltSession, ExecuteInvalidMessage) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);
  ASSERT_THROW(ExecuteCommand(input_stream, session, init_req, sizeof(init_req)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorIgnoreMessage) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);

    WriteRunRequest(input_stream, kInvalidQuery);
    session.Execute();

    output.clear();

    output_stream.SetWriteSuccess(i == 0);
    if (i == 0) {
      ExecuteCommand(input_stream, session, init_req, sizeof(init_req));
    } else {
      ASSERT_THROW(ExecuteCommand(input_stream, session, init_req, sizeof(init_req)), SessionException);
    }

    // assert that all data from the init message was cleaned up
    ASSERT_EQ(session.decoder_buffer_.Size(), 0);

    if (i == 0) {
      ASSERT_EQ(session.state_, State::Error);
      auto to_validate = std::span<uint8_t const>{output};
      CheckOutput(to_validate, ignored_resp, sizeof(ignored_resp));
      output.clear();
    } else {
      ASSERT_EQ(session.state_, State::Close);
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ErrorRunAfterRun) {
  // first test with socket write success, then with socket write fail
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteRunRequest(input_stream, kQueryReturn42);
  session.Execute();

  output.clear();

  output_stream.SetWriteSuccess(true);

  // Session holds results of last run.
  ASSERT_EQ(session.state_, State::Result);

  // New run request.
  WriteRunRequest(input_stream, kQueryReturn42);
  ASSERT_THROW(session.Execute(), SessionException);

  ASSERT_EQ(session.state_, State::Close);
}

TEST(BoltSession, ErrorCantCleanup) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteRunRequest(input_stream, kInvalidQuery);
  session.Execute();

  output.clear();

  // there is data missing in the request, cleanup should fail
  ASSERT_THROW(ExecuteCommand(input_stream, session, init_req, sizeof(init_req) - 10), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorWrongMarker) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteRunRequest(input_stream, kInvalidQuery);
  session.Execute();

  output.clear();

  // wrong marker, good signature
  uint8_t data[2] = {0x00, init_req[1]};
  ASSERT_THROW(ExecuteCommand(input_stream, session, data, sizeof(data)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorOK) {
  {
    SCOPED_TRACE("v1");
    // test ACK_FAILURE and RESET
    const uint8_t *dataset[] = {ackfailure_req, reset_req};

    for (int i = 0; i < 2; ++i) {
      SCOPED_TRACE("i: " + std::to_string(i));
      // first test with socket write success, then with socket write fail
      for (int j = 0; j < 2; ++j) {
        SCOPED_TRACE("j: " + std::to_string(j));
        const auto write_success = j == 0;
        INIT_VARS;

        ExecuteHandshake(input_stream, session, output);
        ASSERT_EQ(session.version_.major, 1U);

        ExecuteInit(input_stream, session, output);
        WriteRunRequest(input_stream, kInvalidQuery);
        session.Execute();

        output.clear();

        output_stream.SetWriteSuccess(write_success);
        if (write_success) {
          ExecuteCommand(input_stream, session, dataset[i], 2);
        } else {
          ASSERT_THROW(ExecuteCommand(input_stream, session, dataset[i], 2), SessionException);
        }

        // assert that all data from the init message was cleaned up
        EXPECT_EQ(session.decoder_buffer_.Size(), 0);

        if (write_success) {
          EXPECT_EQ(session.state_, State::Idle);
          auto to_validate = std::span<uint8_t const>{output};
          CheckOutput(to_validate, success_resp, sizeof(success_resp));
          output.clear();
        } else {
          EXPECT_EQ(session.state_, State::Close);
          EXPECT_EQ(output.size(), 0);
        }
      }
    }
  }

  {
    SCOPED_TRACE("v4");
    const uint8_t *dataset[] = {ackfailure_req, v4::reset_req};
    for (int i = 0; i < 2; ++i) {
      SCOPED_TRACE("i: " + std::to_string(i));
      // first test with socket write success, then with socket write fail
      for (int j = 0; j < 2; ++j) {
        SCOPED_TRACE("j: " + std::to_string(j));
        const auto write_success = j == 0;
        const auto is_reset = i == 1;
        INIT_VARS;

        ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
        ASSERT_EQ(session.version_.major, 4U);
        ExecuteInit(input_stream, session, output, true);

        WriteRunRequest(input_stream, kInvalidQuery, true);
        session.Execute();

        output.clear();
        output_stream.SetWriteSuccess(write_success);

        // ACK_FAILURE does not exist in v3+, ingored message is sent
        if (write_success) {
          ExecuteCommand(input_stream, session, dataset[i], 2);
        } else {
          ASSERT_THROW(ExecuteCommand(input_stream, session, dataset[i], 2), SessionException);
        }

        if (write_success) {
          if (is_reset) {
            EXPECT_EQ(session.state_, State::Idle);
            auto to_validate = std::span<uint8_t const>{output};
            CheckOutput(to_validate, success_resp, sizeof(success_resp));
            output.clear();
          } else {
            ASSERT_EQ(session.state_, State::Error);
            auto to_validate = std::span<uint8_t const>{output};
            CheckOutput(to_validate, ignored_resp, sizeof(ignored_resp));
            output.clear();
          }
        } else {
          EXPECT_EQ(session.state_, State::Close);
        }
      }
    }
  }
}

TEST(BoltSession, ErrorMissingData) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteRunRequest(input_stream, kInvalidQuery);
  session.Execute();

  output.clear();

  // some marker, missing signature
  uint8_t data[1] = {0x00};
  ASSERT_THROW(ExecuteCommand(input_stream, session, data, sizeof(data)), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  CheckFailureMessage(output);
}

TEST(BoltSession, MultipleChunksInOneExecute) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteRunRequest(input_stream, kQueryReturn42);
  ExecuteCommand(input_stream, session, pullall_req, sizeof(pullall_req));

  ASSERT_EQ(session.state_, State::Idle);
  PrintOutput(output);

  // Count chunks in output
  int len, num = 0;
  while (output.size() > 0) {
    len = (output[0] << 8) + output[1];
    output.erase(output.begin(), output.begin() + len + 4);
    ++num;
  }

  // there should be 3 chunks in the output
  // the first is a success with the query headers
  // the second is a record message
  // and the last is a success message with query run metadata
  ASSERT_EQ(num, 3);
}

TEST(BoltSession, PartialPull) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);

  WriteRunRequest(input_stream, kQueryReturnMultiple, true);
  ExecuteCommand(input_stream, session, v4::pull_one_req, sizeof(v4::pull_one_req));

  // Not all results were pulled
  ASSERT_EQ(session.state_, State::Result);
  PrintOutput(output);

  int len{0}, num{0};
  while (output.size() > 0) {
    len = (output[0] << 8) + output[1];
    output.erase(output.begin(), output.begin() + len + 4);
    ++num;
  }

  // the first is a success with the query headers
  // the second is a record message
  // and the last is a success message with query run metadata
  ASSERT_EQ(num, 3);

  ExecuteCommand(input_stream, session, v4::pullall_req, sizeof(v4::pullall_req));
  ASSERT_EQ(session.state_, State::Idle);
  PrintOutput(output);

  len = 0;
  num = 0;
  while (output.size() > 0) {
    len = (output[0] << 8) + output[1];
    output.erase(output.begin(), output.begin() + len + 4);
    ++num;
  }

  // First two are the record messages
  // and the last is a success message with query run metadata
  ASSERT_EQ(num, 3);
}

TEST(BoltSession, PartialChunk) {
  INIT_VARS;
  ExecuteHandshake(input_stream, session, output);
  ExecuteInit(input_stream, session, output);

  WriteChunkHeader(input_stream, sizeof(discardall_req));
  input_stream.Write(discardall_req, sizeof(discardall_req));

  // missing chunk tail
  session.Execute();

  ASSERT_EQ(session.state_, State::Idle);
  ASSERT_EQ(output.size(), 0);

  WriteChunkTail(input_stream);

  ASSERT_THROW(session.Execute(), SessionException);

  ASSERT_EQ(session.state_, State::Close);
  ASSERT_GT(output.size(), 0);
  PrintOutput(output);
}

TEST(BoltSession, Goodbye) {
  // v4 supports goodbye message
  {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4::goodbye, sizeof(v4::goodbye)),
                 memgraph::communication::SessionClosedException);
  }

  // v1 does not support goodbye message
  {
    INIT_VARS;
    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4::goodbye, sizeof(v4::goodbye)), SessionException);
  }
}

TEST(BoltSession, Noop) {
  // v4.1 supports NOOP chunk
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_1::handshake_req, v4_1::handshake_resp);
    ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop));
    ExecuteInit(input_stream, session, output, true);
    ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop));
    WriteRunRequest(input_stream, kQueryReturn42, true);
    ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop));
    ExecuteCommand(input_stream, session, v4::pullall_req, sizeof(v4::pullall_req));
    ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop));
  }

  // v1 does not support NOOP chunk
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, handshake_req, handshake_resp);

    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop)), SessionException);
    CheckFailureMessage(output);

    session.state_ = State::Init;
    ExecuteInit(input_stream, session, output);

    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop)), SessionException);
    CheckFailureMessage(output);

    session.state_ = State::Idle;
    WriteRunRequest(input_stream, kQueryEmpty);
    session.Execute();
    CheckSuccessMessage(output);

    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop)), SessionException);
    CheckFailureMessage(output);

    session.state_ = State::Result;
    ExecuteCommand(input_stream, session, pullall_req, sizeof(pullall_req));
    CheckSuccessMessage(output);

    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_1::noop, sizeof(v4_1::noop)), SessionException);
  }
}

TEST(BoltSession, Route) {
  {
    SCOPED_TRACE("v1");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_3::route, sizeof(v4_3::route)), SessionException);
    EXPECT_EQ(session.state_, State::Close);
  }
#ifdef MG_ENTERPRISE
  {
    SCOPED_TRACE("v4");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_3::handshake_req, v4_3::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, v4_3::route, sizeof(v4_3::route)));
    EXPECT_EQ(session.state_, State::Idle);
    CheckSuccessMessage(output);
  }
#else
  {
    SCOPED_TRACE("v4");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_3::handshake_req, v4_3::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, v4_3::route, sizeof(v4_3::route)));
    static constexpr uint8_t expected_resp[] = {
        0x00 /*two bytes of chunk header, chunk contains 64 bytes of data*/,
        0x40,
        0xb1 /*TinyStruct1*/,
        0x7f /*Failure*/,
        0xa2 /*TinyMap with 2 items*/,
        0x84 /*TinyString with 4 chars*/,
        'c',
        'o',
        'd',
        'e',
        0x82 /*TinyString with 2 chars*/,
        '6',
        '6',
        0x87 /*TinyString with 7 chars*/,
        'm',
        'e',
        's',
        's',
        'a',
        'g',
        'e',
        0xd0 /*String*/,
        0x2b /*With 43 chars*/,
        'R',
        'o',
        'u',
        't',
        'e',
        ' ',
        'm',
        'e',
        's',
        's',
        'a',
        'g',
        'e',
        ' ',
        'i',
        's',
        ' ',
        'n',
        'o',
        't',
        ' ',
        's',
        'u',
        'p',
        'p',
        'o',
        'r',
        't',
        'e',
        'd',
        ' ',
        'i',
        'n',
        ' ',
        'M',
        'e',
        'm',
        'g',
        'r',
        'a',
        'p',
        'h',
        '!',
        0x00 /*Terminating zeros*/,
        0x00,
    };
    EXPECT_EQ(input_stream.size(), 0U);
    auto to_validate = std::span<uint8_t const>{output};
    CheckOutput(to_validate, expected_resp, sizeof(expected_resp));
    output.clear();

    EXPECT_EQ(session.state_, State::Error);

    SCOPED_TRACE("Try to reset connection after ROUTE failed");
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, v4::reset_req, sizeof(v4::reset_req)));
    EXPECT_EQ(input_stream.size(), 0U);
    to_validate = std::span<uint8_t const>{output};
    CheckOutput(to_validate, success_resp, sizeof(success_resp));
    output.clear();
    EXPECT_EQ(session.state_, State::Idle);
  }
#endif
}

TEST(BoltSession, Rollback) {
  // v1 does not support ROLLBACK message
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4::rollback, sizeof(v4::rollback)), SessionException);
  }
  // v4 supports ROLLBACK message
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
    ExecuteCommand(input_stream, session, v4::rollback, sizeof(v4::rollback));

    ASSERT_EQ(session.state_, State::Idle);
    CheckSuccessMessage(output);
  }
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4::rollback, sizeof(v4::rollback)), SessionException);
  }
}

TEST(BoltSession, ResetInIdle) {
  {
    SCOPED_TRACE("v1");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, reset_req, sizeof(reset_req)));
    EXPECT_EQ(session.state_, State::Idle);
  }
  {
    SCOPED_TRACE("v4");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_3::handshake_req, v4_3::handshake_resp);
    ExecuteInit(input_stream, session, output, true);
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, v4::reset_req, sizeof(v4::reset_req)));
    EXPECT_EQ(session.state_, State::Idle);
  }
}

TEST(BoltSession, PassMetadata) {
  // v4+
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_3::handshake_req, v4_3::handshake_resp);
    ExecuteInit(input_stream, session, output, true);

    WriteRunRequest(input_stream, kQueryShowTx, true, v4_3::extra_w_metadata);
    session.Execute();
    ASSERT_EQ(session.state_, State::Result);

    ExecuteCommand(input_stream, session, v4::pullall_req, sizeof(v4::pullall_req));
    ASSERT_EQ(session.state_, State::Idle);
    PrintOutput(output);
    constexpr std::array<uint8_t, 5> md_num_123{0x83, 0x6E, 0x75, 0x6D, 0x7B};
    constexpr std::array<uint8_t, 8> md_str_aha{0x83, 0x73, 0x74, 0x72, 0x83, 0x61, 0x68, 0x61};
    auto find_num = std::search(begin(output), end(output), begin(md_num_123), end(md_num_123));
    EXPECT_NE(find_num, end(output));
    auto find_str = std::search(begin(output), end(output), begin(md_str_aha), end(md_str_aha));
    EXPECT_NE(find_str, end(output));
  }
}

TEST(BoltSession, PartialStream) {
  // v4+
  {
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output, v4_3::handshake_req, v4_3::handshake_resp);
    ExecuteInit(input_stream, session, output, true);

    WriteRunRequest(input_stream, kQueryReturnMultiple, true, v4_3::extra_w_127ms_timeout);
    session.Execute();
    ASSERT_EQ(session.state_, State::Result);

    ExecuteCommand(input_stream, session, v4::pull_one_req, sizeof(v4::pull_one_req));
    ASSERT_EQ(session.state_, State::Result);
    constexpr std::array<uint8_t, 10> md_has_more_true{0x88, 0x68, 0x61, 0x73, 0x5F, 0x6D, 0x6F, 0x72, 0x65, 0xC3};
    auto find_has_more = std::search(cbegin(output), cend(output), cbegin(md_has_more_true), cend(md_has_more_true));
    EXPECT_NE(find_has_more, cend(output));

    session.TestHook_ShouldAbort();  // pretend the 127ms timeout was hit
    ExecuteCommand(input_stream, session, v4::pull_one_req, sizeof(v4::pull_one_req));

    PrintOutput(output);

    auto const error_msg = std::u8string_view{u8"Transaction was asked to abort by another user."};
    auto const find_msg = std::search(cbegin(output), cend(output), cbegin(error_msg), cend(error_msg));
    EXPECT_NE(find_msg, cend(output));
  }
}

// ---------------------------------------------------------------------------
// Pending-BEGIN reschedule (pool-native, no strand inline). A BEGIN whose bounded-try engine-lock
// acquire loses the race bails out of HandleBegin into State::PendingBegin (stashing its decoded
// extras); the completion is retried out-of-band via FinishPendingBegin(), which reschedules on each
// further loss and, past the fairness cap, does one blocking acquire so the BEGIN cannot starve.
// ---------------------------------------------------------------------------

// A minimal single-chunk BEGIN with an empty extras map, pre-framed (chunk header + end marker) so it
// can be written straight into the input stream:
//   0x00 0x03           chunk size 3
//   0xB1                TinyStruct1
//   0x11                Begin signature
//   0xA0                empty TinyMap (extras)
//   0x00 0x00           end-of-message terminator
static constexpr std::array<uint8_t, 7> begin_bytes{0x00, 0x03, 0xB1, 0x11, 0xA0, 0x00, 0x00};

using memgraph::communication::bolt::PendingBeginOutcome;

TEST(BoltSession, PendingBeginReschedulesThenCompletesWithOneSuccess) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // 4 bounded-try losses: the 1st is consumed by HandleBegin (BEGIN -> PendingBegin), leaving 3 losses
  // for FinishPendingBegin -> exactly 3 Reschedule outcomes before the 4th attempt wins.
  constexpr int kExpectedReschedules = 3;
  session.TestHook_FailBoundedTries(kExpectedReschedules + 1);
  input_stream.Write(begin_bytes.data(), begin_bytes.size());
  output.clear();

  // One dechunk pass: HandleBegin bails with WouldBlock, stashes the extras, parks in PendingBegin.
  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingBegin);
  EXPECT_TRUE(session.HasPendingBegin());
  EXPECT_TRUE(output.empty());  // no SUCCESS emitted on the bail path

  // Each lost bounded-try returns Reschedule and MUST keep the extras stashed (HasPendingBegin stays
  // true); a premature reset would trip FinishPendingBegin_'s MG_ASSERT on the next attempt.
  int reschedules = 0;
  constexpr int kSafetyBound = 100;  // fail loudly instead of looping forever if the retry logic regresses
  for (int i = 0; i < kSafetyBound; ++i) {
    const auto outcome = session.FinishPendingBegin();
    if (outcome == PendingBeginOutcome::Done) break;
    ASSERT_EQ(outcome, PendingBeginOutcome::Reschedule);
    EXPECT_TRUE(session.HasPendingBegin());  // stash survives across reschedules
    EXPECT_TRUE(output.empty());             // no reply while rescheduling
    EXPECT_EQ(session.state_, State::PendingBegin);
    ++reschedules;
  }

  // Reverting the reschedule (e.g. HandleBegin/FinishPendingBegin blocking instead of bailing) breaks
  // this exact count -> the test is mutation-checkable.
  EXPECT_EQ(reschedules, kExpectedReschedules);
  EXPECT_FALSE(session.HasPendingBegin());  // stash consumed on the terminal Done
  EXPECT_EQ(session.state_, State::Idle);
  // Exactly ONE SUCCESS, emitted only by FinishPendingBegin on Done (empty extras -> canonical frame).
  ASSERT_EQ(output.size(), sizeof(success_resp));
  CheckSuccessMessage(output, /*clear=*/false);
}

TEST(BoltSession, PendingBeginFairnessCapForcesBlockingTerminal) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // EVERY bounded-try loses the race: only the fairness cap can end the loop.
  session.TestHook_FailBoundedTries(1000);
  input_stream.Write(begin_bytes.data(), begin_bytes.size());
  output.clear();

  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingBegin);
  EXPECT_TRUE(session.HasPendingBegin());
  EXPECT_TRUE(output.empty());

  // Under unbounded contention the pool-side completion must still terminate: after kBeginRescheduleCap
  // reschedules FinishPendingBegin_ does one Blocking acquire (which is never gated by the fail hook and
  // never throws WouldBlock), completing the BEGIN. Count the reschedules to prove it is the cap -- not a
  // lucky bounded-try -- that breaks the loop.
  int reschedules = 0;
  constexpr int kSafetyBound = 100;
  for (int i = 0; i < kSafetyBound; ++i) {
    const auto outcome = session.FinishPendingBegin();
    if (outcome == PendingBeginOutcome::Done) break;
    ASSERT_EQ(outcome, PendingBeginOutcome::Reschedule);
    EXPECT_TRUE(session.HasPendingBegin());
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(session.state_, State::PendingBegin);
    ++reschedules;
  }
  // Loop count tied to the cap: the terminal Blocking acquire fires on the next call.
  EXPECT_EQ(reschedules, TestSession::kBeginRescheduleCap);
  EXPECT_FALSE(session.HasPendingBegin());
  EXPECT_EQ(session.state_, State::Idle);
  ASSERT_EQ(output.size(), sizeof(success_resp));  // exactly one SUCCESS, from the Blocking fallback
  CheckSuccessMessage(output, /*clear=*/false);
}

TEST(BoltSession, PendingBeginHoldsBackPipelinedMessageUntilItCompletes) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // One loss (consumed by HandleBegin) is enough to force the BEGIN onto the pending path; the first
  // FinishPendingBegin then wins. Keeps this test focused on ordering, not on the reschedule count.
  session.TestHook_FailBoundedTries(1);
  // A BEGIN immediately followed by a RUN, in one buffer, with the engine lock "contended".
  input_stream.Write(begin_bytes.data(), begin_bytes.size());
  WriteRunRequest(input_stream, kQueryReturn42, /*is_v4=*/true);
  output.clear();

  // One dechunk pass stops at the BEGIN: Execute_ returns at State::PendingBegin WITHOUT reading the RUN
  // chunk, so the RUN is neither decoded nor answered while the BEGIN is outstanding.
  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingBegin);
  EXPECT_TRUE(session.HasPendingBegin());
  EXPECT_TRUE(output.empty());             // no SUCCESS for the BEGIN yet, and the RUN was not processed
  EXPECT_TRUE(session.HasBufferedData());  // the RUN is still queued behind the parked BEGIN

  // Complete the BEGIN: exactly one SUCCESS (empty extras -> canonical frame). The RUN is STILL unread.
  ASSERT_EQ(session.FinishPendingBegin(), PendingBeginOutcome::Done);
  EXPECT_EQ(session.state_, State::Idle);
  EXPECT_FALSE(session.HasPendingBegin());
  ASSERT_EQ(output.size(), sizeof(success_resp));  // only the BEGIN's SUCCESS -- the RUN produced nothing
  CheckSuccessMessage(output, /*clear=*/true);

  // Only now, after the BEGIN finished, does the resume process the pipelined RUN -- proving ordering.
  session.Execute();
  EXPECT_EQ(session.state_, State::Result);  // RUN parsed+prepared, awaiting PULL
  CheckSuccessMessage(output, /*clear=*/false);
}

TEST(BoltSession, PendingBeginGatedToBlockWhenPoolIdle) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // Pool is quiet: AdmissionEngineLockMode returns Blocking. Even though the bounded-try fail hook is armed,
  // BeginTransaction is called with Blocking (not TryBounded) and never throws WouldBlock.
  session.TestHook_SetPoolIdle();
  session.TestHook_FailBoundedTries(10);  // would produce 10 reschedules on a busy pool, but mode is Blocking
  input_stream.Write(begin_bytes.data(), begin_bytes.size());
  output.clear();

  // One dechunk pass: HandleBegin acquires Blocking and completes the BEGIN inline, so the loop drains and
  // Execute_ returns false. A PendingBegin bail (the busy-pool reschedule path) would instead return true --
  // asserting false is what proves the gate took the Blocking path, not the reschedule path.
  ASSERT_FALSE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::Idle);
  EXPECT_FALSE(session.HasPendingBegin());
  // Exactly one SUCCESS, emitted inline by HandleBegin (not the pending path).
  ASSERT_EQ(output.size(), sizeof(success_resp));
  CheckSuccessMessage(output, /*clear=*/false);
}

// ---------------------------------------------------------------------------
// Pending-PREPARE reschedule (pool-native, no strand inline). A RUN is first parsed (State::Parsed); the
// follow-up PREPARE, whose bounded-try engine-lock acquire loses the race, bails out of HandlePrepare into
// State::PendingPrepare (parse retained in SessionHL::parsed_res_). The completion is retried out-of-band via
// FinishPendingPrepare(), which reschedules on each further loss and, past the fairness cap, does one blocking
// acquire so the PREPARE cannot starve. Its single header SUCCESS is emitted only on the terminal Done.
// ---------------------------------------------------------------------------

using memgraph::communication::bolt::PendingPrepareOutcome;

// Count whole bolt frames in `output`, consuming it. A frame is a 2-byte length header, `len` payload bytes,
// then the 2-byte end marker; used to prove "exactly one header" without pinning exact payload bytes.
static int DrainFrameCount(std::vector<uint8_t> &output) {
  int frames = 0;
  while (output.size() > 0) {
    const int len = (output[0] << 8) + output[1];
    output.erase(output.begin(), output.begin() + len + 4);
    ++frames;
  }
  return frames;
}

TEST(BoltSession, PendingPrepareReschedulesThenCompletesWithOneHeader) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // 4 bounded-try losses: the 1st is consumed by HandlePrepare (PREPARE -> PendingPrepare), leaving 3 for
  // FinishPendingPrepare -> exactly 3 Reschedule outcomes before the 4th attempt wins.
  constexpr int kExpectedReschedules = 3;
  session.TestHook_FailBoundedTries(kExpectedReschedules + 1);
  WriteRunRequest(input_stream, kQueryReturn42, /*is_v4=*/true);
  output.clear();

  // First dechunk pass parses the RUN (State::Parsed); the second drives HandlePrepare, whose bounded-try
  // acquire loses and parks the PREPARE in State::PendingPrepare (parse retained in parsed_res_).
  ASSERT_TRUE(session.ExecuteStep());
  ASSERT_EQ(session.state_, State::Parsed);
  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingPrepare);
  EXPECT_TRUE(session.HasPendingPrepare());
  EXPECT_TRUE(output.empty());  // no header emitted on the bail path

  // Each lost bounded-try returns Reschedule and MUST keep the PREPARE pending (parse retained); a premature
  // reset would trip FinishPendingPrepare_'s MG_ASSERT on the next attempt.
  int reschedules = 0;
  constexpr int kSafetyBound = 100;  // fail loudly instead of looping forever if the retry logic regresses
  for (int i = 0; i < kSafetyBound; ++i) {
    const auto outcome = session.FinishPendingPrepare();
    if (outcome == PendingPrepareOutcome::Done) break;
    ASSERT_EQ(outcome, PendingPrepareOutcome::Reschedule);
    EXPECT_TRUE(session.HasPendingPrepare());  // pending state survives across reschedules
    EXPECT_TRUE(output.empty());               // no header while rescheduling
    EXPECT_EQ(session.state_, State::PendingPrepare);
    ++reschedules;
  }

  // Making HandlePrepare/FinishPendingPrepare block instead of bail (never reaching PendingPrepare) breaks this
  // exact count -> the test is mutation-checkable.
  EXPECT_EQ(reschedules, kExpectedReschedules);
  EXPECT_FALSE(session.HasPendingPrepare());  // pending flag cleared on the terminal Done
  EXPECT_EQ(session.state_, State::Result);
  // Exactly ONE header SUCCESS, emitted only by FinishPendingPrepare on Done.
  CheckSuccessMessage(output, /*clear=*/false);
  EXPECT_EQ(DrainFrameCount(output), 1);
}

TEST(BoltSession, PendingPrepareFairnessCapForcesBlockingTerminal) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // EVERY bounded-try loses the race: only the fairness cap can end the loop.
  session.TestHook_FailBoundedTries(1000);
  WriteRunRequest(input_stream, kQueryReturn42, /*is_v4=*/true);
  output.clear();

  ASSERT_TRUE(session.ExecuteStep());
  ASSERT_EQ(session.state_, State::Parsed);
  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingPrepare);
  EXPECT_TRUE(session.HasPendingPrepare());
  EXPECT_TRUE(output.empty());

  // Under unbounded contention the pool-side completion must still terminate: after kPrepareRescheduleCap
  // reschedules FinishPendingPrepare_ does one Blocking acquire (never gated by the fail hook, never throws
  // WouldBlock), completing the PREPARE. Count the reschedules to prove it is the cap -- not a lucky
  // bounded-try -- that breaks the loop.
  int reschedules = 0;
  constexpr int kSafetyBound = 100;
  for (int i = 0; i < kSafetyBound; ++i) {
    const auto outcome = session.FinishPendingPrepare();
    if (outcome == PendingPrepareOutcome::Done) break;
    ASSERT_EQ(outcome, PendingPrepareOutcome::Reschedule);
    EXPECT_TRUE(session.HasPendingPrepare());
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(session.state_, State::PendingPrepare);
    ++reschedules;
  }
  // Loop count tied to the cap: the terminal Blocking acquire fires on the next call.
  EXPECT_EQ(reschedules, TestSession::kPrepareRescheduleCap);
  EXPECT_FALSE(session.HasPendingPrepare());
  EXPECT_EQ(session.state_, State::Result);
  CheckSuccessMessage(output, /*clear=*/false);  // exactly one header, from the Blocking fallback
  EXPECT_EQ(DrainFrameCount(output), 1);
}

TEST(BoltSession, PendingPrepareHoldsBackPipelinedMessageUntilItCompletes) {
  INIT_VARS;

  ExecuteHandshake(input_stream, session, output, v4::handshake_req, v4::handshake_resp);
  ExecuteInit(input_stream, session, output, true);
  ASSERT_EQ(session.state_, State::Idle);

  // One loss (consumed by HandlePrepare) forces the PREPARE onto the pending path; the first
  // FinishPendingPrepare then wins. Keeps this test focused on ordering, not on the reschedule count.
  session.TestHook_FailBoundedTries(1);
  // A RUN immediately followed by its PULL, in one buffer, with the engine lock "contended".
  WriteRunRequest(input_stream, kQueryReturn42, /*is_v4=*/true);
  WriteChunkHeader(input_stream, sizeof(v4::pullall_req));
  input_stream.Write(v4::pullall_req, sizeof(v4::pullall_req));
  WriteChunkTail(input_stream);
  output.clear();

  // First pass parses the RUN; the second bails the PREPARE into PendingPrepare. Neither reads the pipelined
  // PULL: Execute_ stops at PendingPrepare BEFORE the dechunk loop, so the PULL stays queued and unanswered.
  ASSERT_TRUE(session.ExecuteStep());
  ASSERT_EQ(session.state_, State::Parsed);
  ASSERT_TRUE(session.ExecuteStep());
  EXPECT_EQ(session.state_, State::PendingPrepare);
  EXPECT_TRUE(session.HasPendingPrepare());
  EXPECT_TRUE(output.empty());             // no header for the PREPARE yet, and the PULL was not processed
  EXPECT_TRUE(session.HasBufferedData());  // the PULL is still queued behind the parked PREPARE

  // Complete the PREPARE: exactly one header SUCCESS. The PULL is STILL unread.
  ASSERT_EQ(session.FinishPendingPrepare(), PendingPrepareOutcome::Done);
  EXPECT_EQ(session.state_, State::Result);
  EXPECT_FALSE(session.HasPendingPrepare());
  CheckSuccessMessage(output, /*clear=*/false);  // only the PREPARE's header -- the PULL produced nothing
  EXPECT_EQ(DrainFrameCount(output), 1);

  // Only now, after the PREPARE finished, does the resume process the pipelined PULL -- proving ordering.
  session.Execute();
  EXPECT_EQ(session.state_, State::Idle);  // PULL drained the RUN's result
  EXPECT_EQ(DrainFrameCount(output), 2);   // RECORD(42) + SUCCESS from the pipelined PULL
}
