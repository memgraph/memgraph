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

#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "bolt_common.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/exceptions.hpp"
#include "utils/logging.hpp"

using memgraph::communication::bolt::ClientError;
using memgraph::communication::bolt::Session;
using memgraph::communication::bolt::SessionException;
using memgraph::communication::bolt::State;
using memgraph::communication::bolt::Value;

static const char *kInvalidQuery = "invalid query";
static const char *kQueryReturn42 = "RETURN 42";
static const char *kQueryReturnMultiple = "UNWIND [1,2,3] as n RETURN n";
static const char *kQueryShowTx = "SHOW TRANSACTIONS";
static const char *kQueryEmpty = "no results";

class TestSessionData {};

class TestSession : public Session<TestInputStream, TestOutputStream> {
 public:
  using Session<TestInputStream, TestOutputStream>::TEncoder;

  TestSession(TestSessionData *data, TestInputStream *input_stream, TestOutputStream *output_stream)
      : Session<TestInputStream, TestOutputStream>(input_stream, output_stream) {}

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, Value> &params,
      const std::map<std::string, Value> &metadata) override {
    if (!metadata.empty()) md_ = metadata;
    if (query == kQueryReturn42 || query == kQueryEmpty || query == kQueryReturnMultiple) {
      query_ = query;
      return {{"result_name"}, {}};
    } else if (query == kQueryShowTx) {
      if (md_.at("str").ValueString() != "aha" || md_.at("num").ValueInt() != 123) {
        throw ClientError("Wrong metadata!");
      }
      query_ = query;
      return {{"username", "transaction_id", "query", "metadata"}, {}};
    } else {
      query_ = "";
      throw ClientError("client sent invalid query");
    }
  }

  std::map<std::string, Value> Pull(TEncoder *encoder, std::optional<int> n, std::optional<int> qid) override {
    if (query_ == kQueryReturn42) {
      encoder->MessageRecord(std::vector<Value>{Value(42)});
      return {};
    } else if (query_ == kQueryEmpty) {
      return {};
    } else if (query_ == kQueryReturnMultiple) {
      static const std::array elements{1, 2, 3};
      static size_t global_counter = 0;

      int local_counter = 0;
      for (; global_counter < elements.size() && (!n || local_counter < *n); ++global_counter) {
        encoder->MessageRecord(std::vector<Value>{Value(elements[global_counter])});
        ++local_counter;
      }

      if (global_counter == elements.size()) {
        global_counter = 0;
        return {std::pair("has_more", false)};
      }

      return {std::pair("has_more", true)};
    } else if (query_ == kQueryShowTx) {
      encoder->MessageRecord({"", 1234567890, query_, md_});
      return {};
    } else {
      throw ClientError("client sent invalid query");
    }
  }

  std::map<std::string, Value> Discard(std::optional<int>, std::optional<int>) override { return {}; }

  void BeginTransaction(const std::map<std::string, Value> &metadata) override { md_ = metadata; }
  void CommitTransaction() override { md_.clear(); }
  void RollbackTransaction() override { md_.clear(); }

  void Abort() override { md_.clear(); }

  bool Authenticate(const std::string &username, const std::string &password) override { return true; }

  std::optional<std::string> GetServerNameForInit() override { return std::nullopt; }

 private:
  std::string query_;
  std::map<std::string, Value> md_;
};

// TODO: This could be done in fixture.
// Shortcuts for writing variable initializations in tests
#define INIT_VARS                                                    \
  TestInputStream input_stream;                                      \
  TestOutputStream output_stream;                                    \
  TestSessionData session_data;                                      \
  TestSession session(&session_data, &input_stream, &output_stream); \
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
inline constexpr uint8_t route[]{0xb3, 0x66, 0xa0, 0x90, 0xc0};
const std::string extra_w_metadata =
    "\xa2\x8b\x74\x78\x5f\x6d\x65\x74\x61\x64\x61\x74\x61\xa2\x83\x73\x74\x72\x83\x61\x68\x61\x83\x6e\x75\x6d\x7b\x8a"
    "\x74\x78\x5f\x74\x69\x6d\x65\x6f\x75\x74\xc9\x07\xd0";
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
  CheckOutput(output, expected_resp, 4);
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
  CheckOutput(output, response, 28);
}

// Write bolt encoded run request
void WriteRunRequest(TestInputStream &input_stream, const char *str, const bool is_v4 = false,
                     const std::string &extra = "\xA0") {
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
  CheckOutput(output, handshake_resp, 4);
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
  // This should pick 4.3 version since 4.4 and 4.5 are not existant
  {
    INIT_VARS;
    const uint8_t priority_request[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x03, 0x05, 0x04, 0x00, 0x00,
                                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const uint8_t priority_response[] = {0x00, 0x00, 0x03, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 3);
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
    const uint8_t priority_response[] = {0x00, 0x00, 0x03, 0x04};
    ExecuteHandshake(input_stream, session, output, priority_request, priority_response);
    ASSERT_EQ(session.version_.minor, 3);
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
      CheckOutput(output, ignored_resp, sizeof(ignored_resp));
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
          CheckOutput(output, success_resp, sizeof(success_resp));
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

        // ACK_FAILURE does not exist in v3+, ignored message is sent
        if (write_success) {
          ExecuteCommand(input_stream, session, dataset[i], 2);
        } else {
          ASSERT_THROW(ExecuteCommand(input_stream, session, dataset[i], 2), SessionException);
        }

        if (write_success) {
          if (is_reset) {
            EXPECT_EQ(session.state_, State::Idle);
            CheckOutput(output, success_resp, sizeof(success_resp));
          } else {
            ASSERT_EQ(session.state_, State::Error);
            CheckOutput(output, ignored_resp, sizeof(ignored_resp));
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
  // Memgraph does not support route message, but it handles it
  {
    SCOPED_TRACE("v1");
    INIT_VARS;

    ExecuteHandshake(input_stream, session, output);
    ExecuteInit(input_stream, session, output);
    ASSERT_THROW(ExecuteCommand(input_stream, session, v4_3::route, sizeof(v4_3::route)), SessionException);
    EXPECT_EQ(session.state_, State::Close);
  }
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
    CheckOutput(output, expected_resp, sizeof(expected_resp));
    EXPECT_EQ(session.state_, State::Error);

    SCOPED_TRACE("Try to reset connection after ROUTE failed");
    ASSERT_NO_THROW(ExecuteCommand(input_stream, session, v4::reset_req, sizeof(v4::reset_req)));
    EXPECT_EQ(input_stream.size(), 0U);
    CheckOutput(output, success_resp, sizeof(success_resp));
    EXPECT_EQ(session.state_, State::Idle);
  }
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
