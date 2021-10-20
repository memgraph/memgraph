// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <filesystem>
#include <optional>
#include <utility>

#include <gtest/gtest.h>
#include "integrations/kafka/exceptions.hpp"
#include "kafka_mock.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/storage.hpp"

using Streams = query::Streams;
using StreamInfo = query::KafkaStream::StreamInfo;
using StreamStatus = query::StreamStatus<query::KafkaStream>;
namespace {
const static std::string kTopicName{"TrialTopic"};

struct StreamCheckData {
  std::string name;
  StreamInfo info;
  bool is_running;
  std::optional<std::string> owner;
};

std::string GetDefaultStreamName() {
  return std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()};
}

StreamInfo CreateDefaultStreamInfo() {
  return StreamInfo{.common_info{
                        .batch_interval = std::nullopt,
                        .batch_size = std::nullopt,
                        .transformation_name = "not used in the tests",
                    },
                    .topics = {kTopicName},
                    .consumer_group = "ConsumerGroup " + GetDefaultStreamName(),
                    .bootstrap_servers = ""};
}

StreamCheckData CreateDefaultStreamCheckData() {
  return {GetDefaultStreamName(), CreateDefaultStreamInfo(), false, std::nullopt};
}

std::filesystem::path GetCleanDataDirectory() {
  const auto path = std::filesystem::temp_directory_path() / "query-streams";
  std::filesystem::remove_all(path);
  return path;
}
}  // namespace

class StreamsTest : public ::testing::Test {
 public:
  StreamsTest() { ResetStreamsObject(); }

 protected:
  storage::Storage db_;
  std::filesystem::path data_directory_{GetCleanDataDirectory()};
  KafkaClusterMock mock_cluster_{std::vector<std::string>{kTopicName}};
  // Though there is a Streams object in interpreter context, it makes more sense to use a separate object to test,
  // because that provides a way to recreate the streams object and also give better control over the arguments of the
  // Streams constructor.
  // InterpreterContext::auth_checker_ is used in the Streams object, but only in the message processing part. Because
  // these tests don't send any messages, the auth_checker_ pointer can be left as nullptr.
  query::InterpreterContext interpreter_context_{&db_, query::InterpreterConfig{}, data_directory_,
                                                 "dont care bootstrap servers"};
  std::filesystem::path streams_data_directory_{data_directory_ / "separate-dir-for-test"};
  std::optional<Streams> streams_;

  void ResetStreamsObject() {
    streams_.emplace(&interpreter_context_, mock_cluster_.Bootstraps(), streams_data_directory_);
  }

  void CheckStreamStatus(const StreamCheckData &check_data) {
    SCOPED_TRACE(fmt::format("Checking status of '{}'", check_data.name));
    const auto &stream_statuses = streams_->GetStreamInfo();
    auto it = std::find_if(stream_statuses.begin(), stream_statuses.end(),
                           [&check_data](const auto &stream_status) { return stream_status.name == check_data.name; });
    ASSERT_NE(it, stream_statuses.end());
    const auto &status = *it;
    EXPECT_EQ(check_data.info.common_info.batch_interval, status.info.batch_interval);
    EXPECT_EQ(check_data.info.common_info.batch_size, status.info.batch_size);
    EXPECT_EQ(check_data.info.common_info.transformation_name, status.info.transformation_name);
    EXPECT_EQ(check_data.is_running, status.is_running);
  }

  void StartStream(StreamCheckData &check_data) {
    streams_->Start(check_data.name);
    check_data.is_running = true;
  }

  void StopStream(StreamCheckData &check_data) {
    streams_->Stop(check_data.name);
    check_data.is_running = false;
  }

  void Clear() {
    if (!std::filesystem::exists(data_directory_)) return;
    std::filesystem::remove_all(data_directory_);
  }
};

TEST_F(StreamsTest, SimpleStreamManagement) {
  auto check_data = CreateDefaultStreamCheckData();
  streams_->Create<query::KafkaStream>(check_data.name, check_data.info, check_data.owner);
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  EXPECT_NO_THROW(streams_->Start(check_data.name));
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  EXPECT_NO_THROW(streams_->StopAll());
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  EXPECT_NO_THROW(streams_->StartAll());
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  EXPECT_NO_THROW(streams_->Stop(check_data.name));
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  EXPECT_NO_THROW(streams_->Drop(check_data.name));
  EXPECT_TRUE(streams_->GetStreamInfo().empty());
}

TEST_F(StreamsTest, CreateAlreadyExisting) {
  auto stream_info = CreateDefaultStreamInfo();
  auto stream_name = GetDefaultStreamName();
  streams_->Create<query::KafkaStream>(stream_name, stream_info, std::nullopt);

  try {
    streams_->Create<query::KafkaStream>(stream_name, stream_info, std::nullopt);
    FAIL() << "Creating already existing stream should throw\n";
  } catch (query::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Stream already exists with name '{}'", stream_name));
  }
}

TEST_F(StreamsTest, DropNotExistingStream) {
  const auto stream_info = CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  const std::string not_existing_stream_name{"ThisDoesn'tExists"};
  streams_->Create<query::KafkaStream>(stream_name, stream_info, std::nullopt);

  try {
    streams_->Drop(not_existing_stream_name);
    FAIL() << "Dropping not existing stream should throw\n";
  } catch (query::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Couldn't find stream '{}'", not_existing_stream_name));
  }
}

TEST_F(StreamsTest, RestoreStreams) {
  std::array stream_check_datas{
      CreateDefaultStreamCheckData(),
      CreateDefaultStreamCheckData(),
      CreateDefaultStreamCheckData(),
      CreateDefaultStreamCheckData(),
  };

  // make the stream infos unique
  for (auto i = 0; i < stream_check_datas.size(); ++i) {
    auto &stream_check_data = stream_check_datas[i];
    auto &stream_info = stream_check_data.info;
    auto iteration_postfix = std::to_string(i);

    stream_check_data.name += iteration_postfix;
    stream_info.topics[0] += iteration_postfix;
    stream_info.consumer_group += iteration_postfix;
    stream_info.common_info.transformation_name += iteration_postfix;
    if (i > 0) {
      stream_info.common_info.batch_interval = std::chrono::milliseconds((i + 1) * 10);
      stream_info.common_info.batch_size = 1000 + i;
      stream_check_data.owner = std::string{"owner"} + iteration_postfix;
    }

    mock_cluster_.CreateTopic(stream_info.topics[0]);
  }
  stream_check_datas[1].info.common_info.batch_interval = {};
  stream_check_datas[2].info.common_info.batch_size = {};
  stream_check_datas[3].owner = {};

  const auto check_restore_logic = [&stream_check_datas, this]() {
    // Reset the Streams object to trigger reloading
    ResetStreamsObject();
    EXPECT_TRUE(streams_->GetStreamInfo().empty());
    streams_->RestoreStreams();
    EXPECT_EQ(stream_check_datas.size(), streams_->GetStreamInfo().size());
    for (const auto &check_data : stream_check_datas) {
      ASSERT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));
    }
  };

  streams_->RestoreStreams();
  EXPECT_TRUE(streams_->GetStreamInfo().empty());

  for (auto &check_data : stream_check_datas) {
    streams_->Create<query::KafkaStream>(check_data.name, check_data.info, check_data.owner);
  }
  {
    SCOPED_TRACE("After streams are created");
    check_restore_logic();
  }

  for (auto &check_data : stream_check_datas) {
    StartStream(check_data);
  }
  {
    SCOPED_TRACE("After starting streams");
    check_restore_logic();
  }

  // Stop two of the streams
  StopStream(stream_check_datas[1]);
  StopStream(stream_check_datas[3]);
  {
    SCOPED_TRACE("After stopping two streams");
    check_restore_logic();
  }

  // Stop the rest of the streams
  StopStream(stream_check_datas[0]);
  StopStream(stream_check_datas[2]);
  check_restore_logic();
  {
    SCOPED_TRACE("After stopping all streams");
    check_restore_logic();
  }
}

TEST_F(StreamsTest, CheckWithTimeout) {
  const auto stream_info = CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  streams_->Create<query::KafkaStream>(stream_name, stream_info, std::nullopt);

  std::chrono::milliseconds timeout{3000};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(streams_->Check(stream_name, timeout, std::nullopt), integrations::kafka::ConsumerCheckFailedException);
  const auto end = std::chrono::steady_clock::now();

  const auto elapsed = (end - start);
  EXPECT_LE(timeout, elapsed);
  EXPECT_LE(elapsed, timeout * 1.2);
}
