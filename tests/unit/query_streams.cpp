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

struct StreamCheckData {
  std::string name;
  StreamInfo info;
  bool is_running;
};

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

StreamCheckData CreateDefaultStreamCheckData() { return {GetDefaultStreamName(), CreateDefaultStreamInfo(), false}; }

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

  void CheckStreamStatus(const StreamCheckData &check_data) {
    const auto &stream_statuses = streams_->Show();
    auto it = std::find_if(stream_statuses.begin(), stream_statuses.end(), [&check_data](const auto &name_and_status) {
      return name_and_status.first == check_data.name;
    });
    ASSERT_NE(it, stream_statuses.end());
    const auto &status = it->second;
    // the order don't have to be strictly the same, but based on the implementation it shouldn't change
    EXPECT_TRUE(std::equal(check_data.info.topics.begin(), check_data.info.topics.end(), status.info.topics.begin(),
                           status.info.topics.end()));
    EXPECT_EQ(check_data.info.consumer_group, status.info.consumer_group);
    EXPECT_EQ(check_data.info.batch_interval, status.info.batch_interval);
    EXPECT_EQ(check_data.info.batch_size, status.info.batch_size);
    // TODO(antaljanosbenjamin) Add proper reference once Streams supports that
    EXPECT_EQ(check_data.info.transformation_name, status.info.transformation_name);
    EXPECT_EQ(check_data.is_running, status.is_running);
  }
};

TEST_F(StreamsTest, SimpleStreamManagement) {
  auto check_data = CreateDefaultStreamCheckData();
  streams_->Create(check_data.name, check_data.info);
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  streams_->Start(check_data.name);
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  streams_->StopAll();
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  streams_->StartAll();
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  streams_->Stop(check_data.name);
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(CheckStreamStatus(check_data));

  streams_->Drop(check_data.name);
  EXPECT_TRUE(streams_->Show().empty());
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
