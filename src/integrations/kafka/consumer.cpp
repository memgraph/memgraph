#include "integrations/kafka/consumer.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <set>

#include "integrations/kafka/exceptions.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/thread.hpp"

namespace integrations::kafka {

constexpr std::chrono::milliseconds kDefaultBatchInterval{100};
constexpr int64_t kDefaultBatchSize = 1000;
constexpr int64_t kDefaultTestBatchLimit = 1;

Message::Message(std::unique_ptr<RdKafka::Message> &&message) : message_{std::move(message)} {
  // Because of these asserts, the message can be safely accessed in the member function functions, because it cannot
  // be null and always points to a valid message (not to a wrapped error)
  MG_ASSERT(message_.get() != nullptr, "Kafka message cannot be null!");
  MG_ASSERT(message_->err() == 0 && message_->c_ptr() != nullptr, "Invalid kafka message!");
};

std::string_view Message::Key() const {
  const auto *c_message = message_->c_ptr();
  std::string_view view = {static_cast<const char *>(c_message->key), c_message->key_len};
  return {static_cast<const char *>(c_message->key), c_message->key_len};
}

std::string_view Message::TopicName() const {
  rd_kafka_message_s *c_message = message_->c_ptr();
  if (c_message->rkt == nullptr) {
    return {};
  }
  return rd_kafka_topic_name(c_message->rkt);
}

std::span<const char> Message::Payload() const {
  const auto *c_message = message_->c_ptr();
  return {static_cast<char *>(c_message->payload), c_message->len};
}

int64_t Message::Timestamp() const {
  const auto *c_message = message_->c_ptr();
  rd_kafka_timestamp_type_t timestamp_type{};
  return rd_kafka_message_timestamp(c_message, &timestamp_type);
}

Consumer::Consumer(ConsumerInfo &&info) : info_{std::move(info)} {
  MG_ASSERT(info_.consumer_function, "Empty consumer function for Kafka consumer");
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (conf == nullptr) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Couldn't create Kafka configuration!");
  }

  std::string error;

  if (conf->set("event_cb", this, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.partition.eof", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.auto.offset.store", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("bootstrap.servers", info_.bootstrap_servers, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("group.id", info_.consumer_group, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  consumer_ = std::unique_ptr<RdKafka::KafkaConsumer, std::function<void(RdKafka::KafkaConsumer *)>>(
      RdKafka::KafkaConsumer::create(conf.get(), error), [this](auto *consumer) {
        this->StopConsuming();
        consumer->close();
        delete consumer;
      });

  if (consumer_ == nullptr) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  RdKafka::ErrorCode err{};
  RdKafka::Metadata *raw_metadata = nullptr;
  err = consumer_->metadata(true, nullptr, &raw_metadata, 1000);
  std::unique_ptr<RdKafka::Metadata> metadata(raw_metadata);
  if (err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }

  std::set<std::string> topic_names_form_metadata{};
  for (const auto &topic_metadata : *metadata->topics()) {
    topic_names_form_metadata.insert(topic_metadata->topic());
  }

  for (const auto &topic_name : info_.topics) {
    if (!topic_names_form_metadata.contains(topic_name)) {
      throw TopicNotFoundException(info_.consumer_name, topic_name);
    }
  }

  err = consumer_->subscribe(info_.topics);
  if (err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }
}

void Consumer::Start(std::optional<int64_t> limit_batches) {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  StartConsuming(limit_batches);
}

void Consumer::StartIfStopped() {
  if (!is_running_) {
    StartConsuming(std::nullopt);
  }
}

void Consumer::Stop() {
  if (!is_running_) {
    throw ConsumerStoppedException(info_.consumer_name);
  }

  StopConsuming();
}

void Consumer::StopIfRunning() {
  if (is_running_) {
    StopConsuming();
  }
}

void Consumer::Test(std::optional<int64_t> limit_batches, const ConsumerFunction &test_consumer_function) {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  int64_t num_of_batches = limit_batches.value_or(kDefaultTestBatchLimit);

  is_running_.store(true);

  std::vector<std::unique_ptr<RdKafka::TopicPartition>> partitions;
  auto save_offsets = [&partitions, this]() {
    std::vector<RdKafka::TopicPartition *> tmp_partitions;
    auto err = consumer_->assignment(tmp_partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
    err = consumer_->position(tmp_partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
    partitions.reserve(tmp_partitions.size());
    std::transform(
        tmp_partitions.begin(), tmp_partitions.end(), std::back_inserter(partitions),
        [](RdKafka::TopicPartition *const partition) { return std::unique_ptr<RdKafka::TopicPartition>{partition}; });
  };

  save_offsets();
  // The order of these scope guards is important: first the offsets have to be restored, and only after is_running_
  // should be set to false.
  utils::OnScopeExit cleanup([this]() { is_running_.store(false); });
  utils::OnScopeExit restore_offsets{[&partitions, this]() {
    std::vector<RdKafka::TopicPartition *> tmp_partitions;
    tmp_partitions.reserve(partitions.size());
    std::transform(partitions.begin(), partitions.end(), std::back_inserter(tmp_partitions),
                   [](const auto &partition) { return partition.get(); });
    auto err = consumer_->assign(tmp_partitions);
    if (err != RdKafka::ERR_NO_ERROR) {
      spdlog::error("Couldn't restore previous offsets after testing Kafka consumer {}!", info_.consumer_name);
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
  }};

  for (int64_t i = 0; i < num_of_batches;) {
    auto [batch, error] = GetBatch();

    if (error.has_value()) {
      throw ConsumerTestFailedException(info_.consumer_name, *error);
    }

    if (batch.empty()) {
      continue;
    }
    ++i;

    try {
      test_consumer_function(batch);
    } catch (const std::exception &e) {
      spdlog::warn("Kafka consumer {} test failed with error {}", info_.consumer_name, e.what());
      throw ConsumerTestFailedException(info_.consumer_name, e.what());
    }
  }
}

bool Consumer::IsRunning() const { return is_running_; }

void Consumer::event_cb(RdKafka::Event &event) {
  switch (event.type()) {
    case RdKafka::Event::Type::EVENT_ERROR:
      spdlog::warn("Kafka consumer {} received an error: {}", info_.consumer_name, RdKafka::err2str(event.err()));
      break;
    case RdKafka::Event::Type::EVENT_STATS:
    case RdKafka::Event::Type::EVENT_LOG:
    case RdKafka::Event::Type::EVENT_THROTTLE:
      break;
  }
}
void Consumer::StartConsuming(std::optional<int64_t> limit_batches) {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");

  if (thread_.joinable()) {
    // This can happen if the thread just finished its last batch, already set is_running_ to false and currently
    // shutting down.
    thread_.join();
  };

  is_running_.store(true);

  thread_ = std::thread([this, limit_batches = limit_batches]() {
    constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, std::min(full_thread_name.size(), kMaxThreadNameSize)));

    int64_t batch_count = 0;

    while (is_running_) {
      auto [batch, error] = this->GetBatch();
      if (error.has_value()) {
        spdlog::warn("Error happened in consumer {} while fetching messages: {}!", info_.consumer_name, *error);
        is_running_.store(false);
      }

      if (batch.empty()) continue;

      spdlog::info("Kafka consumer {} is processing a batch", info_.consumer_name);

      // TODO (mferencevic): Figure out what to do with all other exceptions.
      try {
        info_.consumer_function(batch);
        consumer_->commitSync();
      } catch (const utils::BasicException &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }

      if (limit_batches != std::nullopt) {
        if (limit_batches <= ++batch_count) {
          is_running_.store(false);
          break;
        }
      }
    }
  });
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) thread_.join();
}

std::pair<std::vector<Message>, std::optional<std::string>> Consumer::GetBatch() {
  std::vector<Message> batch{};
  std::optional<std::string> error{};

  int64_t batch_size = info_.batch_size.value_or(kDefaultBatchSize);
  batch.reserve(batch_size);

  auto remaining_timeout_in_ms = info_.batch_interval.value_or(kDefaultBatchInterval).count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < batch_size; ++i) {
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(remaining_timeout_in_ms));
    switch (msg->err()) {
      case RdKafka::ERR__TIMED_OUT:
        run_batch = false;
        break;

      case RdKafka::ERR_NO_ERROR:
        batch.emplace_back(std::move(msg));
        break;

      default:
        spdlog::warn("Unexpected error while consuming message in consumer {}, error: {}!", info_.consumer_name,
                     msg->errstr());
        run_batch = false;
        error = msg->errstr();
        break;
    }

    if (!run_batch) {
      break;
    }

    auto now = std::chrono::steady_clock::now();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
    remaining_timeout_in_ms = remaining_timeout_in_ms - took.count();
    start = now;
  }

  return {std::move(batch), std::move(error)};
}

}  // namespace integrations::kafka
