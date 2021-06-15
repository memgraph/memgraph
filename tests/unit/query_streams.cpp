#include <algorithm>
#include <filesystem>
#include <optional>
#include <utility>

#include <gtest/gtest.h>
#include "kafka_mock.hpp"
#include "query/interpreter.hpp"
#include "query/streams.hpp"
#include "storage/v2/storage.hpp"

using Streams = query::Streams;
using StreamInfo = query::StreamInfo;
using StreamStatus = query::StreamStatus;
namespace {
const static std::string kTopicName{"TrialTopic"};

std::string GetDefaultStreamName() {
  return std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()};
}

StreamInfo CreateDefaultStreamInfo() {
  return StreamInfo{
      .topics = {kTopicName},
      .consumer_group = "ConsumerGroup " + GetDefaultStreamName(),
      .batch_interval = std::nullopt,
      .batch_size = std::nullopt,
      // TODO(antaljanosbenjamin) Add proper reference once Streams supports that
      .transformation_name = "not yet used",
  };
}

void CheckStreamStatus(const StreamInfo &expected_info, const bool expected_is_running, const StreamStatus &status) {
  // the order don't have to be strictly the same, but based on the implementation it shouldn't change
  EXPECT_TRUE(std::equal(expected_info.topics.begin(), expected_info.topics.end(), status.info.topics.begin(),
                         status.info.topics.end()));
  EXPECT_EQ(expected_info.consumer_group, status.info.consumer_group);
  EXPECT_EQ(expected_info.batch_interval, status.info.batch_interval);
  EXPECT_EQ(expected_info.batch_size, status.info.batch_size);
  // TODO(antaljanosbenjamin) Add proper reference once Streams supports that
  EXPECT_EQ(expected_info.transformation_name, status.info.transformation_name);
  EXPECT_EQ(expected_is_running, status.is_running);
}

}  // namespace

class StreamsTest : public ::testing::Test {
 protected:
  storage::Storage db_;
  std::filesystem::path data_directory_{std::filesystem::temp_directory_path() / "query-streams"};
  KafkaClusterMock mock_cluster_{std::vector<std::string>{kTopicName}};
  // Though there is a Streams object in interpreter context, it makes more sense to use a separate object to test,
  // because that provides a way to recreate the streams object and also give better control over the arguments of the
  // Streams constructor.
  query::InterpreterContext interpreter_context_{&db_, data_directory_, "dont care bootstrap servers"};
  std::filesystem::path streams_data_directory_{data_directory_ / "separate-dir-for-test"};
  std::optional<Streams> streams_{std::in_place, &interpreter_context_, mock_cluster_.Bootstraps(),
                                  streams_data_directory_};
};

TEST_F(StreamsTest, SimpleStreamManagement) {
  auto stream_info = CreateDefaultStreamInfo();
  auto stream_name = GetDefaultStreamName();
  streams_->Create(stream_name, stream_info);
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(stream_info, false, streams_->Show(stream_name)));
  streams_->Start(stream_name);
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(stream_info, true, streams_->Show(stream_name)));
  streams_->StopAll();
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(stream_info, false, streams_->Show(stream_name)));
  streams_->StartAll();
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(stream_info, true, streams_->Show(stream_name)));
  streams_->Stop(stream_name);
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(stream_info, false, streams_->Show(stream_name)));
  streams_->Drop(stream_name);
}

TEST_F(StreamsTest, CreateAlreadyExisting) {
  auto stream_info = CreateDefaultStreamInfo();
  auto stream_name = GetDefaultStreamName();
  streams_->Create(stream_name, stream_info);

  try {
    streams_->Create(stream_name, stream_info);
    FAIL() << "Creating already existing stream should throw\n";
  } catch (query::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Stream already exists with name '{}'", stream_name));
  }
}

TEST_F(StreamsTest, DropNotExistingStream) {
  const auto stream_info = CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  const std::string not_existing_stream_name{"ThisDoesn'tExists"};
  streams_->Create(stream_name, stream_info);

  try {
    streams_->Drop(not_existing_stream_name);
    FAIL() << "Dropping not existing stream should throw\n";
  } catch (query::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Couldn't find stream '{}'", not_existing_stream_name));
  }
}

TEST_F(StreamsTest, ShowNotExisting) {
  const auto stream_info = CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  const std::string not_existing_stream_name{"ThisDoesn'tExists"};
  streams_->Create(stream_name, stream_info);

  try {
    streams_->Show(not_existing_stream_name);
    FAIL() << "Getting the status of non existing stream should throw\n";
  } catch (query::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Couldn't find stream '{}'", not_existing_stream_name));
  }
}
