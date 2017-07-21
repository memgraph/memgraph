#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bolt_common.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/session.hpp"
#include "query/engine.hpp"

DECLARE_bool(interpret);

// Shortcuts for writing variable initializations in tests
#define INIT_VARS                                          \
  Dbms dbms;                                               \
  TestSocket socket(10);                                   \
  QueryEngine<ResultStreamT> query_engine;                 \
  SessionT session(std::move(socket), dbms, query_engine); \
  std::vector<uint8_t> &output = session.socket_.output;

using ResultStreamT =
    communication::bolt::ResultStream<communication::bolt::Encoder<
        communication::bolt::ChunkedEncoderBuffer<TestSocket>>>;
using SessionT = communication::bolt::Session<TestSocket>;
using StateT = communication::bolt::State;

// Sample testdata that has correct inputs and outputs.
const uint8_t handshake_req[] = {0x60, 0x60, 0xb0, 0x17, 0x00, 0x00, 0x00,
                                 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
const uint8_t handshake_resp[] = {0x00, 0x00, 0x00, 0x01};
const uint8_t init_req[] = {
    0xb2, 0x01, 0xd0, 0x15, 0x6c, 0x69, 0x62, 0x6e, 0x65, 0x6f, 0x34,
    0x6a, 0x2d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2f, 0x31, 0x2e,
    0x32, 0x2e, 0x31, 0xa3, 0x86, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x65,
    0x85, 0x62, 0x61, 0x73, 0x69, 0x63, 0x89, 0x70, 0x72, 0x69, 0x6e,
    0x63, 0x69, 0x70, 0x61, 0x6c, 0x80, 0x8b, 0x63, 0x72, 0x65, 0x64,
    0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x80};
const uint8_t init_resp[] = {0x00, 0x03, 0xb1, 0x70, 0xa0, 0x00, 0x00};
const uint8_t run_req_header[] = {0xb2, 0x10, 0xd1};
const uint8_t pullall_req[] = {0xb0, 0x3f};
const uint8_t discardall_req[] = {0xb0, 0x2f};
const uint8_t reset_req[] = {0xb0, 0x0f};
const uint8_t ackfailure_req[] = {0xb0, 0x0e};
const uint8_t success_resp[] = {0x00, 0x03, 0xb1, 0x70, 0xa0, 0x00, 0x00};
const uint8_t ignored_resp[] = {0x00, 0x02, 0xb0, 0x7e, 0x00, 0x00};

// Write bolt chunk header (length)
void WriteChunkHeader(SessionT &session, uint16_t len) {
  len = bswap(len);
  auto buff = session.Allocate();
  memcpy(buff.data, reinterpret_cast<uint8_t *>(&len), sizeof(len));
  session.Written(sizeof(len));
}

// Write bolt chunk tail (two zeros)
void WriteChunkTail(SessionT &session) { WriteChunkHeader(session, 0); }

// Check that the server responded with a failure message
void CheckFailureMessage(std::vector<uint8_t> &output) {
  ASSERT_GE(output.size(), 6);
  // skip the first two bytes because they are the chunk header
  ASSERT_EQ(output[2], 0xB1);  // tiny struct 1
  ASSERT_EQ(output[3], 0x7F);  // signature failure
}

// Execute and check a correct handshake
void ExecuteHandshake(SessionT &session, std::vector<uint8_t> &output) {
  auto buff = session.Allocate();
  memcpy(buff.data, handshake_req, 20);
  session.Written(20);
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Init);
  ASSERT_TRUE(session.socket_.IsOpen());
  PrintOutput(output);
  CheckOutput(output, handshake_resp, 4);
}

// Write bolt chunk and execute command
void ExecuteCommand(SessionT &session, const uint8_t *data, size_t len,
                    bool chunk = true) {
  if (chunk) WriteChunkHeader(session, len);
  auto buff = session.Allocate();
  memcpy(buff.data, data, len);
  session.Written(len);
  if (chunk) WriteChunkTail(session);
  session.Execute();
}

// Execute and check a correct init
void ExecuteInit(SessionT &session, std::vector<uint8_t> &output) {
  ExecuteCommand(session, init_req, sizeof(init_req));
  ASSERT_EQ(session.state_, StateT::Idle);
  ASSERT_TRUE(session.socket_.IsOpen());
  PrintOutput(output);
  CheckOutput(output, init_resp, 7);
}

// Write bolt encoded run request
void WriteRunRequest(SessionT &session, const char *str) {
  // write chunk header
  auto len = strlen(str);
  WriteChunkHeader(session, 3 + 2 + len + 1);

  // write string header
  auto buff = session.Allocate();
  memcpy(buff.data, run_req_header, 3);
  session.Written(3);

  // write string length
  WriteChunkHeader(session, len);

  // write string
  buff = session.Allocate();
  memcpy(buff.data, str, len);
  session.Written(len);

  // write empty map for parameters
  buff = session.Allocate();
  buff.data[0] = 0xA0;  // TinyMap0
  session.Written(1);

  // write chunk tail
  WriteChunkTail(session);
}

TEST(BoltSession, HandshakeWrongPreamble) {
  INIT_VARS;

  auto buff = session.Allocate();
  // copy 0x00000001 four times
  for (int i = 0; i < 4; ++i) memcpy(buff.data + i * 4, handshake_req + 4, 4);
  session.Written(20);
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  PrintOutput(output);
  CheckFailureMessage(output);
}

TEST(BoltSession, HandshakeInTwoPackets) {
  INIT_VARS;

  auto buff = session.Allocate();
  memcpy(buff.data, handshake_req, 10);
  session.Written(10);
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Handshake);
  ASSERT_TRUE(session.socket_.IsOpen());

  memcpy(buff.data + 10, handshake_req + 10, 10);
  session.Written(10);
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Init);
  ASSERT_TRUE(session.socket_.IsOpen());
  PrintOutput(output);
  CheckOutput(output, handshake_resp, 4);
}

TEST(BoltSession, HandshakeWriteFail) {
  INIT_VARS;
  session.socket_.SetWriteSuccess(false);
  ExecuteCommand(session, handshake_req, sizeof(handshake_req), false);

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  ASSERT_EQ(output.size(), 0);
}

TEST(BoltSession, HandshakeOK) {
  INIT_VARS;
  ExecuteHandshake(session, output);
}

TEST(BoltSession, InitWrongSignature) {
  INIT_VARS;
  ExecuteHandshake(session, output);
  ExecuteCommand(session, run_req_header, sizeof(run_req_header));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, InitWrongMarker) {
  INIT_VARS;
  ExecuteHandshake(session, output);

  // wrong marker, good signature
  uint8_t data[2] = {0x00, init_req[1]};
  ExecuteCommand(session, data, 2);

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, InitMissingData) {
  // test lengths, they test the following situations:
  // missing header data, missing client name, missing metadata
  int len[] = {1, 2, 25};

  for (int i = 0; i < 3; ++i) {
    INIT_VARS;
    ExecuteHandshake(session, output);
    ExecuteCommand(session, init_req, len[i]);

    ASSERT_EQ(session.state_, StateT::Close);
    ASSERT_FALSE(session.socket_.IsOpen());
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, InitWriteFail) {
  INIT_VARS;
  ExecuteHandshake(session, output);
  session.socket_.SetWriteSuccess(false);
  ExecuteCommand(session, init_req, sizeof(init_req));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  ASSERT_EQ(output.size(), 0);
}

TEST(BoltSession, InitOK) {
  INIT_VARS;
  ExecuteHandshake(session, output);
  ExecuteInit(session, output);
}

TEST(BoltSession, ExecuteRunWrongMarker) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  // wrong marker, good signature
  uint8_t data[2] = {0x00, run_req_header[1]};
  ExecuteCommand(session, data, sizeof(data));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, ExecuteRunMissingData) {
  // test lengths, they test the following situations:
  // missing header data, missing query data, missing parameters
  int len[] = {1, 2, 37};

  for (int i = 0; i < 3; ++i) {
    INIT_VARS;
    ExecuteHandshake(session, output);
    ExecuteInit(session, output);
    ExecuteCommand(session, run_req_header, len[i]);

    ASSERT_EQ(session.state_, StateT::Close);
    ASSERT_FALSE(session.socket_.IsOpen());
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, ExecuteRunBasicException) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(session, output);
    ExecuteInit(session, output);

    session.socket_.SetWriteSuccess(i == 0);
    WriteRunRequest(session, "MATCH (omnom");
    session.Execute();

    if (i == 0) {
      ASSERT_EQ(session.state_, StateT::ErrorIdle);
      ASSERT_TRUE(session.socket_.IsOpen());
      CheckFailureMessage(output);
    } else {
      ASSERT_EQ(session.state_, StateT::Close);
      ASSERT_FALSE(session.socket_.IsOpen());
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ExecuteRunWithoutPullAll) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "RETURN 2");
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Result);
}

TEST(BoltSession, ExecutePullAllDiscardAllResetWrongMarker) {
  // This test first tests PULL_ALL then DISCARD_ALL and then RESET
  // It tests for missing data in the message header
  const uint8_t *dataset[3] = {pullall_req, discardall_req, reset_req};

  for (int i = 0; i < 3; ++i) {
    INIT_VARS;

    ExecuteHandshake(session, output);
    ExecuteInit(session, output);

    // wrong marker, good signature
    uint8_t data[2] = {0x00, dataset[i][1]};
    ExecuteCommand(session, data, sizeof(data));

    ASSERT_EQ(session.state_, StateT::Close);
    ASSERT_FALSE(session.socket_.IsOpen());
    CheckFailureMessage(output);
  }
}

TEST(BoltSession, ExecutePullAllBufferEmpty) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(session, output);
    ExecuteInit(session, output);

    session.socket_.SetWriteSuccess(i == 0);
    ExecuteCommand(session, pullall_req, sizeof(pullall_req));

    if (i == 0) {
      ASSERT_EQ(session.state_, StateT::ErrorIdle);
      ASSERT_TRUE(session.socket_.IsOpen());
      CheckFailureMessage(output);
    } else {
      ASSERT_EQ(session.state_, StateT::Close);
      ASSERT_FALSE(session.socket_.IsOpen());
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ExecutePullAllDiscardAllReset) {
  // This test first tests PULL_ALL then DISCARD_ALL and then RESET
  // It tests a good message
  const uint8_t *dataset[3] = {pullall_req, discardall_req, reset_req};

  for (int i = 0; i < 3; ++i) {
    // first test with socket write success, then with socket write fail
    for (int j = 0; j < 2; ++j) {
      INIT_VARS;

      ExecuteHandshake(session, output);
      ExecuteInit(session, output);
      WriteRunRequest(session, "CREATE (n) RETURN n");
      session.Execute();

      if (j == 1) output.clear();

      session.socket_.SetWriteSuccess(j == 0);
      ExecuteCommand(session, dataset[i], 2);

      if (j == 0) {
        ASSERT_EQ(session.state_, StateT::Idle);
        ASSERT_TRUE(session.socket_.IsOpen());
        ASSERT_FALSE(session.encoder_buffer_.HasData());
        PrintOutput(output);
      } else {
        ASSERT_EQ(session.state_, StateT::Close);
        ASSERT_FALSE(session.socket_.IsOpen());
        ASSERT_EQ(output.size(), 0);
      }
    }
  }
}

TEST(BoltSession, ExecuteInvalidMessage) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);
  ExecuteCommand(session, init_req, sizeof(init_req));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorIgnoreMessage) {
  // first test with socket write success, then with socket write fail
  for (int i = 0; i < 2; ++i) {
    INIT_VARS;

    ExecuteHandshake(session, output);
    ExecuteInit(session, output);

    WriteRunRequest(session, "MATCH (omnom");
    session.Execute();

    output.clear();

    session.socket_.SetWriteSuccess(i == 0);
    ExecuteCommand(session, init_req, sizeof(init_req));

    // assert that all data from the init message was cleaned up
    ASSERT_EQ(session.decoder_buffer_.Size(), 0);

    if (i == 0) {
      ASSERT_EQ(session.state_, StateT::ErrorIdle);
      ASSERT_TRUE(session.socket_.IsOpen());
      CheckOutput(output, ignored_resp, sizeof(ignored_resp));
    } else {
      ASSERT_EQ(session.state_, StateT::Close);
      ASSERT_FALSE(session.socket_.IsOpen());
      ASSERT_EQ(output.size(), 0);
    }
  }
}

TEST(BoltSession, ErrorRunAfterRun) {
  // first test with socket write success, then with socket write fail
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "MATCH (n) RETURN n");
  session.Execute();

  output.clear();

  session.socket_.SetWriteSuccess(true);

  // Session holds results of last run.
  ASSERT_EQ(session.state_, StateT::Result);

  // New run request.
  WriteRunRequest(session, "MATCH (n) RETURN n");
  session.Execute();

  // Run after run fails, but we still keep results.
  // TODO: actually we don't, but we should. Change state to ErrorResult once
  // that is fixed.
  ASSERT_EQ(session.state_, StateT::ErrorIdle);
  ASSERT_TRUE(session.socket_.IsOpen());
}

TEST(BoltSession, ErrorCantCleanup) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "MATCH (omnom");
  session.Execute();

  output.clear();

  // there is data missing in the request, cleanup should fail
  ExecuteCommand(session, init_req, sizeof(init_req) - 10);

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorWrongMarker) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "MATCH (omnom");
  session.Execute();

  output.clear();

  // wrong marker, good signature
  uint8_t data[2] = {0x00, init_req[1]};
  ExecuteCommand(session, data, sizeof(data));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, ErrorOK) {
  // test ACK_FAILURE and RESET
  const uint8_t *dataset[] = {ackfailure_req, reset_req};

  for (int i = 0; i < 2; ++i) {
    // first test with socket write success, then with socket write fail
    for (int j = 0; j < 2; ++j) {
      INIT_VARS;

      ExecuteHandshake(session, output);
      ExecuteInit(session, output);

      WriteRunRequest(session, "MATCH (omnom");
      session.Execute();

      output.clear();

      session.socket_.SetWriteSuccess(j == 0);
      ExecuteCommand(session, dataset[i], 2);

      // assert that all data from the init message was cleaned up
      ASSERT_EQ(session.decoder_buffer_.Size(), 0);

      if (j == 0) {
        ASSERT_EQ(session.state_, StateT::Idle);
        ASSERT_TRUE(session.socket_.IsOpen());
        CheckOutput(output, success_resp, sizeof(success_resp));
      } else {
        ASSERT_EQ(session.state_, StateT::Close);
        ASSERT_FALSE(session.socket_.IsOpen());
        ASSERT_EQ(output.size(), 0);
      }
    }
  }
}

TEST(BoltSession, ErrorMissingData) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "MATCH (omnom");
  session.Execute();

  output.clear();

  // some marker, missing signature
  uint8_t data[1] = {0x00};
  ExecuteCommand(session, data, sizeof(data));

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

TEST(BoltSession, MultipleChunksInOneExecute) {
  INIT_VARS;

  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteRunRequest(session, "CREATE (n) RETURN n");
  ExecuteCommand(session, pullall_req, sizeof(pullall_req));

  ASSERT_EQ(session.state_, StateT::Idle);
  ASSERT_TRUE(session.socket_.IsOpen());
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

TEST(BoltSession, PartialChunk) {
  INIT_VARS;
  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  WriteChunkHeader(session, sizeof(discardall_req));
  auto buff = session.Allocate();
  memcpy(buff.data, discardall_req, sizeof(discardall_req));
  session.Written(2);

  // missing chunk tail
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Idle);
  ASSERT_TRUE(session.socket_.IsOpen());
  ASSERT_EQ(output.size(), 0);

  WriteChunkTail(session);

  session.Execute();

  ASSERT_EQ(session.state_, StateT::ErrorIdle);
  ASSERT_TRUE(session.socket_.IsOpen());
  ASSERT_GT(output.size(), 0);
  PrintOutput(output);
}

TEST(BoltSession, InvalidChunk) {
  INIT_VARS;
  ExecuteHandshake(session, output);
  ExecuteInit(session, output);

  // this will write 0x00 0x02 0x00 0x02 0x00 0x02
  // that is a chunk of good size, but it's invalid because the last
  // two bytes are 0x00 0x02 and they should be 0x00 0x00
  for (int i = 0; i < 3; ++i) WriteChunkHeader(session, 2);
  session.Execute();

  ASSERT_EQ(session.state_, StateT::Close);
  ASSERT_FALSE(session.socket_.IsOpen());
  CheckFailureMessage(output);
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  // Set the interpret to true to avoid calling the compiler which only
  // supports a limited set of queries.
  FLAGS_interpret = true;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
