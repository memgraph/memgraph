#include "integrations/kafka/consumer.hpp"

#include <algorithm>
#include <iterator>
#include <memory>
#include <unordered_set>

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

std::span<const char> Message::Key() const {
  const auto *c_message = message_->c_ptr();
  return {static_cast<const char *>(c_message->key), c_message->key_len};
}

std::string_view Message::TopicName() const {
  const auto *c_message = message_->c_ptr();
  return c_message->rkt == nullptr ? std::string_view{} : rd_kafka_topic_name(c_message->rkt);
}

std::span<const char> Message::Payload() const {
  const auto *c_message = message_->c_ptr();
  return {static_cast<const char *>(c_message->payload), c_message->len};
}

int64_t Message::Timestamp() const {
  const auto *c_message = message_->c_ptr();
  return rd_kafka_message_timestamp(c_message, nullptr);
}

Consumer::Consumer(const std::string &bootstrap_servers, ConsumerInfo info, ConsumerFunction consumer_function)
    : info_{std::move(info)}, consumer_function_(std::move(consumer_function)) {
  MG_ASSERT(consumer_function_, "Empty consumer function for Kafka consumer");
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

  if (conf->set("bootstrap.servers", bootstrap_servers, error) != RdKafka::Conf::CONF_OK) {
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

  RdKafka::Metadata *raw_metadata = nullptr;
  if (const auto err = consumer_->metadata(true, nullptr, &raw_metadata, 1000); err != RdKafka::ERR_NO_ERROR) {
    delete raw_metadata;
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }
  std::unique_ptr<RdKafka::Metadata> metadata(raw_metadata);

  std::unordered_set<std::string> topic_names_from_metadata{};
  std::transform(metadata->topics()->begin(), metadata->topics()->end(),
                 std::inserter(topic_names_from_metadata, topic_names_from_metadata.begin()),
                 [](const auto topic_metadata) { return topic_metadata->topic(); });

  for (const auto &topic_name : info_.topics) {
    if (!topic_names_from_metadata.contains(topic_name)) {
      throw TopicNotFoundException(info_.consumer_name, topic_name);
    }
  }

  if (const auto err = consumer_->subscribe(info_.topics); err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }
}

Consumer::~Consumer() { StopIfRunning(); }

void Consumer::Start() {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  StartConsuming();
}

void Consumer::StartIfStopped() {
  if (!is_running_) {
    StartConsuming();
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
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Consumer::Test(std::optional<int64_t> limit_batches, const ConsumerFunction &test_consumer_function) {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  int64_t num_of_batches = limit_batches.value_or(kDefaultTestBatchLimit);

  is_running_.store(true);

  std::vector<std::unique_ptr<RdKafka::TopicPartition>> partitions;
  {
    // Save the current offsets in order to restore them in cleanup
    std::vector<RdKafka::TopicPartition *> tmp_partitions;
    if (const auto err = consumer_->assignment(tmp_partitions); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
    if (const auto err = consumer_->position(tmp_partitions); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
    partitions.reserve(tmp_partitions.size());
    std::transform(
        tmp_partitions.begin(), tmp_partitions.end(), std::back_inserter(partitions),
        [](RdKafka::TopicPartition *const partition) { return std::unique_ptr<RdKafka::TopicPartition>{partition}; });
  }

  utils::OnScopeExit cleanup([this, &partitions]() {
    is_running_.store(false);

    std::vector<RdKafka::TopicPartition *> tmp_partitions;
    tmp_partitions.reserve(partitions.size());
    std::transform(partitions.begin(), partitions.end(), std::back_inserter(tmp_partitions),
                   [](const auto &partition) { return partition.get(); });

    if (const auto err = consumer_->assign(tmp_partitions); err != RdKafka::ERR_NO_ERROR) {
      spdlog::error("Couldn't restore previous offsets after testing Kafka consumer {}!", info_.consumer_name);
      throw ConsumerTestFailedException(info_.consumer_name, RdKafka::err2str(err));
    }
  });

  for (int64_t i = 0; i < num_of_batches;) {
    auto maybe_batch = GetBatch();

    if (maybe_batch.HasError()) {
      throw ConsumerTestFailedException(info_.consumer_name, maybe_batch.GetError());
    }

    const auto &batch = maybe_batch.GetValue();

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

ConsumerInfo Consumer::Info() const { return info_; }

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
void Consumer::StartConsuming() {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");

  if (thread_.joinable()) {
    // This can happen if the thread just finished its last batch, already set is_running_ to false and currently
    // shutting down.
    thread_.join();
  };

  is_running_.store(true);

  thread_ = std::thread([this]() {
    constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = this->GetBatch();
      if (maybe_batch.HasError()) {
        spdlog::warn("Error happened in consumer {} while fetching messages: {}!", info_.consumer_name,
                     maybe_batch.GetError());
        break;
      }
      const auto &batch = maybe_batch.GetValue();

      if (batch.empty()) continue;

      spdlog::info("Kafka consumer {} is processing a batch", info_.consumer_name);

      // TODO (mferencevic): Figure out what to do with all other exceptions.
      try {
        consumer_function_(batch);
        consumer_->commitSync();
      } catch (const utils::BasicException &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }
    }
    is_running_.store(false);
  });
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) thread_.join();
}

utils::BasicResult<std::string, std::vector<Message>> Consumer::GetBatch() {
  std::vector<Message> batch{};

  int64_t batch_size = info_.batch_size.value_or(kDefaultBatchSize);
  batch.reserve(batch_size);

  auto remaining_timeout_in_ms = info_.batch_interval.value_or(kDefaultBatchInterval).count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < batch_size && is_running_.load(); ++i) {
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(remaining_timeout_in_ms));
    switch (msg->err()) {
      case RdKafka::ERR__TIMED_OUT:
        run_batch = false;
        break;

      case RdKafka::ERR_NO_ERROR:
        batch.emplace_back(std::move(msg));
        break;

      default:
        auto error = msg->errstr();
        spdlog::warn("Unexpected error while consuming message in consumer {}, error: {}!", info_.consumer_name,
                     msg->errstr());
        return {std::move(error)};
    }

    if (!run_batch) {
      break;
    }

    auto now = std::chrono::steady_clock::now();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
    remaining_timeout_in_ms = remaining_timeout_in_ms - took.count();
    start = now;
  }

  return {std::move(batch)};
}

}  // namespace integrations::kafka
