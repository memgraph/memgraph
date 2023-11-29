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

#include <algorithm>
#include <filesystem>
#include <optional>
#include <utility>

#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "integrations/constants.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "kafka_mock.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "test_utils.hpp"

using Streams = memgraph::query::stream::Streams;
using StreamInfo = memgraph::query::stream::KafkaStream::StreamInfo;
using StreamStatus = memgraph::query::stream::StreamStatus<memgraph::query::stream::KafkaStream>;
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

std::filesystem::path GetCleanDataDirectory() {
  const auto path = std::filesystem::temp_directory_path() / "query-streams";
  std::filesystem::remove_all(path);
  return path;
}
}  // namespace

// We need this proxy class because we can't make template class a friend in different file.
class StreamsTest {
 public:
  std::optional<Streams> streams_;
  using KafkaStream = memgraph::query::stream::Streams::StreamData<memgraph::query::stream::KafkaStream>;

  auto ReadLock() { return streams_->streams_.ReadLock(); }
};

template <typename StorageType>
class StreamsTestFixture : public ::testing::Test {
 public:
  StreamsTestFixture() { ResetStreamsObject(); }

 protected:
  const std::string testSuite = "query_streams";
  std::filesystem::path data_directory_{GetCleanDataDirectory()};
  KafkaClusterMock mock_cluster_{std::vector<std::string>{kTopicName}};
  // Though there is a Streams object in interpreter context, it makes more sense to use a separate object to test,
  // because that provides a way to recreate the streams object and also give better control over the arguments of the
  // Streams constructor.
  // InterpreterContext::auth_checker_ is used in the Streams object, but only in the message processing part. Because
  // these tests don't send any messages, the auth_checker_ pointer can be left as nullptr.

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory_;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db_{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };
  memgraph::query::InterpreterContext interpreter_context_{memgraph::query::InterpreterConfig{}, nullptr, &repl_state};
  std::filesystem::path streams_data_directory_{data_directory_ / "separate-dir-for-test"};
  std::optional<StreamsTest> proxyStreams_;

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory_);
  }

  void ResetStreamsObject() {
    proxyStreams_.emplace();
    proxyStreams_->streams_.emplace(streams_data_directory_);
  }

  void CheckStreamStatus(const StreamCheckData &check_data) {
    SCOPED_TRACE(fmt::format("Checking status of '{}'", check_data.name));
    const auto &stream_statuses = proxyStreams_->streams_->GetStreamInfo();
    auto it = std::find_if(stream_statuses.begin(), stream_statuses.end(),
                           [&check_data](const auto &stream_status) { return stream_status.name == check_data.name; });
    ASSERT_NE(it, stream_statuses.end());
    const auto &status = *it;
    EXPECT_EQ(check_data.info.common_info.batch_interval, status.info.batch_interval);
    EXPECT_EQ(check_data.info.common_info.batch_size, status.info.batch_size);
    EXPECT_EQ(check_data.info.common_info.transformation_name, status.info.transformation_name);
    EXPECT_EQ(check_data.is_running, status.is_running);
  }

  void CheckConfigAndCredentials(const StreamCheckData &check_data) {
    const auto locked_streams = proxyStreams_->ReadLock();
    const auto &stream = locked_streams->at(check_data.name);
    const auto *stream_data = std::get_if<StreamsTest::KafkaStream>(&stream);
    ASSERT_NE(stream_data, nullptr);
    const auto stream_info =
        stream_data->stream_source->ReadLock()->Info(check_data.info.common_info.transformation_name);
    EXPECT_TRUE(
        std::equal(check_data.info.configs.begin(), check_data.info.configs.end(), stream_info.configs.begin()));
  }

  void StartStream(StreamCheckData &check_data) {
    proxyStreams_->streams_->Start(check_data.name);
    check_data.is_running = true;
  }

  void StopStream(StreamCheckData &check_data) {
    proxyStreams_->streams_->Stop(check_data.name);
    check_data.is_running = false;
  }

  StreamInfo CreateDefaultStreamInfo() {
    return StreamInfo{.common_info{
                          .batch_interval = memgraph::query::stream::kDefaultBatchInterval,
                          .batch_size = memgraph::query::stream::kDefaultBatchSize,
                          .transformation_name = "not used in the tests",
                      },
                      .topics = {kTopicName},
                      .consumer_group = "ConsumerGroup " + GetDefaultStreamName(),
                      .bootstrap_servers = mock_cluster_.Bootstraps()};
  }

  StreamCheckData CreateDefaultStreamCheckData() {
    return {GetDefaultStreamName(), CreateDefaultStreamInfo(), false, std::nullopt};
  }

  void Clear() {
    if (!std::filesystem::exists(data_directory_)) return;
    std::filesystem::remove_all(data_directory_);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(StreamsTestFixture, StorageTypes);

TYPED_TEST(StreamsTestFixture, SimpleStreamManagement) {
  auto check_data = this->CreateDefaultStreamCheckData();
  this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
      check_data.name, check_data.info, check_data.owner, this->db_, &this->interpreter_context_);
  EXPECT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));

  EXPECT_NO_THROW(this->proxyStreams_->streams_->Start(check_data.name));
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));

  EXPECT_NO_THROW(this->proxyStreams_->streams_->StopAll());
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));

  EXPECT_NO_THROW(this->proxyStreams_->streams_->StartAll());
  check_data.is_running = true;
  EXPECT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));

  EXPECT_NO_THROW(this->proxyStreams_->streams_->Stop(check_data.name));
  check_data.is_running = false;
  EXPECT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));

  EXPECT_NO_THROW(this->proxyStreams_->streams_->Drop(check_data.name));
  EXPECT_TRUE(this->proxyStreams_->streams_->GetStreamInfo().empty());
}

TYPED_TEST(StreamsTestFixture, CreateAlreadyExisting) {
  auto stream_info = this->CreateDefaultStreamInfo();
  auto stream_name = GetDefaultStreamName();
  this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
      stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_);

  try {
    this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
        stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_);
    FAIL() << "Creating already existing stream should throw\n";
  } catch (memgraph::query::stream::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Stream already exists with name '{}'", stream_name));
  }
}

TYPED_TEST(StreamsTestFixture, DropNotExistingStream) {
  const auto stream_info = this->CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  const std::string not_existing_stream_name{"ThisDoesn'tExists"};
  this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
      stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_);

  try {
    this->proxyStreams_->streams_->Drop(not_existing_stream_name);
    FAIL() << "Dropping not existing stream should throw\n";
  } catch (memgraph::query::stream::StreamsException &exception) {
    EXPECT_EQ(exception.what(), fmt::format("Couldn't find stream '{}'", not_existing_stream_name));
  }
}

TYPED_TEST(StreamsTestFixture, RestoreStreams) {
  std::array stream_check_datas{
      this->CreateDefaultStreamCheckData(),
      this->CreateDefaultStreamCheckData(),
      this->CreateDefaultStreamCheckData(),
      this->CreateDefaultStreamCheckData(),
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

      // These are just random numbers to make the CONFIGS and CREDENTIALS map vary between consumers:
      // - 0 means no config, no credential
      // - 1 means only config
      // - 2 means only credential
      // - 3 means both configuration and credential is set
      if (i == 1 || i == 3) {
        stream_info.configs.emplace(std::string{"sasl.username"}, std::string{"username"} + iteration_postfix);
      }
      if (i == 2 || i == 3) {
        stream_info.credentials.emplace(std::string{"sasl.password"}, std::string{"password"} + iteration_postfix);
      }
    }

    this->mock_cluster_.CreateTopic(stream_info.topics[0]);
  }

  stream_check_datas[3].owner = {};

  const auto check_restore_logic = [&stream_check_datas, this]() {
    // Reset the Streams object to trigger reloading
    this->ResetStreamsObject();
    EXPECT_TRUE(this->proxyStreams_->streams_->GetStreamInfo().empty());
    this->proxyStreams_->streams_->RestoreStreams(this->db_, &this->interpreter_context_);
    EXPECT_EQ(stream_check_datas.size(), this->proxyStreams_->streams_->GetStreamInfo().size());
    for (const auto &check_data : stream_check_datas) {
      ASSERT_NO_FATAL_FAILURE(this->CheckStreamStatus(check_data));
      ASSERT_NO_FATAL_FAILURE(this->CheckConfigAndCredentials(check_data));
    }
  };

  this->proxyStreams_->streams_->RestoreStreams(this->db_, &this->interpreter_context_);
  EXPECT_TRUE(this->proxyStreams_->streams_->GetStreamInfo().empty());

  for (auto &check_data : stream_check_datas) {
    this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
        check_data.name, check_data.info, check_data.owner, this->db_, &this->interpreter_context_);
  }
  {
    SCOPED_TRACE("After streams are created");
    check_restore_logic();
  }

  for (auto &check_data : stream_check_datas) {
    this->StartStream(check_data);
  }
  {
    SCOPED_TRACE("After starting streams");
    check_restore_logic();
  }

  // Stop two of the streams
  this->StopStream(stream_check_datas[1]);
  this->StopStream(stream_check_datas[3]);
  {
    SCOPED_TRACE("After stopping two streams");
    check_restore_logic();
  }

  // Stop the rest of the streams
  this->StopStream(stream_check_datas[0]);
  this->StopStream(stream_check_datas[2]);
  check_restore_logic();
  {
    SCOPED_TRACE("After stopping all streams");
    check_restore_logic();
  }
}

TYPED_TEST(StreamsTestFixture, CheckWithTimeout) {
  const auto stream_info = this->CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
      stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_);

  std::chrono::milliseconds timeout{3000};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(this->proxyStreams_->streams_->Check(stream_name, this->db_, timeout, std::nullopt),
               memgraph::integrations::kafka::ConsumerCheckFailedException);
  const auto end = std::chrono::steady_clock::now();

  const auto elapsed = (end - start);
  EXPECT_LE(timeout, elapsed);
  EXPECT_LE(elapsed, timeout * 1.2);
}

TYPED_TEST(StreamsTestFixture, CheckInvalidConfig) {
  auto stream_info = this->CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  static constexpr auto kInvalidConfigName = "doesnt.exist";
  static constexpr auto kConfigValue = "myprecious";
  stream_info.configs.emplace(kInvalidConfigName, kConfigValue);
  const auto checker = [](const std::string_view message) {
    EXPECT_TRUE(message.find(kInvalidConfigName) != std::string::npos) << message;
    EXPECT_TRUE(message.find(kConfigValue) != std::string::npos) << message;
  };
  EXPECT_THROW_WITH_MSG(this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
                            stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_),
                        memgraph::integrations::kafka::SettingCustomConfigFailed, checker);
}

TYPED_TEST(StreamsTestFixture, CheckInvalidCredentials) {
  auto stream_info = this->CreateDefaultStreamInfo();
  const auto stream_name = GetDefaultStreamName();
  static constexpr auto kInvalidCredentialName = "doesnt.exist";
  static constexpr auto kCredentialValue = "myprecious";
  stream_info.credentials.emplace(kInvalidCredentialName, kCredentialValue);
  const auto checker = [](const std::string_view message) {
    EXPECT_TRUE(message.find(kInvalidCredentialName) != std::string::npos) << message;
    EXPECT_TRUE(message.find(memgraph::integrations::kReducted) != std::string::npos) << message;
    EXPECT_TRUE(message.find(kCredentialValue) == std::string::npos) << message;
  };
  EXPECT_THROW_WITH_MSG(this->proxyStreams_->streams_->template Create<memgraph::query::stream::KafkaStream>(
                            stream_name, stream_info, std::nullopt, this->db_, &this->interpreter_context_),
                        memgraph::integrations::kafka::SettingCustomConfigFailed, checker);
}
