#include "integrations/kafka/consumer.hpp"

#include <chrono>

#include "glog/logging.h"

#include "integrations/kafka/exceptions.hpp"

namespace integrations {
namespace kafka {

using namespace std::chrono_literals;

constexpr int64_t kDefaultBatchIntervalMillis = 100;
constexpr int64_t kDefaultBatchSize = 10;

void Consumer::event_cb(RdKafka::Event &event) {
  switch (event.type()) {
    case RdKafka::Event::Type::EVENT_ERROR:
      LOG(WARNING) << "[Kafka] stream " << info_.stream_name << " ERROR ("
                   << RdKafka::err2str(event.err()) << "): " << event.str();
      break;
    default:
      break;
  }
}

Consumer::Consumer(const StreamInfo &info) : info_(info) {
  std::unique_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  std::string error;

  if (conf->set("event_cb", this, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.stream_name, error);
  }

  if (conf->set("enable.partition.eof", "false", error) !=
      RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.stream_name, error);
  }

  if (conf->set("bootstrap.servers", info_.stream_uri, error) !=
      RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.stream_name, error);
  }

  if (conf->set("group.id", "mg", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.stream_name, error);
  }

  consumer_ = std::unique_ptr<RdKafka::KafkaConsumer,
                              std::function<void(RdKafka::KafkaConsumer *)>>(
      RdKafka::KafkaConsumer::create(conf.get(), error),
      [this](auto *consumer) {
        this->StopConsuming();
        consumer->close();
        delete consumer;
      });

  if (!consumer_) {
    throw ConsumerFailedToInitializeException(info_.stream_name, error);
  }

  // Try fetching metadata first and check if topic exists.
  RdKafka::ErrorCode err;
  RdKafka::Metadata *raw_metadata = nullptr;
  err = consumer_->metadata(true, nullptr, &raw_metadata, 1000);
  std::unique_ptr<RdKafka::Metadata> metadata(raw_metadata);
  if (err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.stream_name,
                                              RdKafka::err2str(err));
  }

  bool topic_found = false;
  for (const auto &topic_metadata : *metadata->topics()) {
    if (topic_metadata->topic() == info_.stream_topic) {
      topic_found = true;
      break;
    }
  }

  if (!topic_found) {
    throw TopicNotFoundException(info_.stream_name);
  }

  err = consumer_->subscribe({info_.stream_topic});
  if (err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.stream_name,
                                              RdKafka::err2str(err));
  }
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) thread_.join();
}

void Consumer::StartConsuming(
    std::experimental::optional<int64_t> batch_limit) {
  thread_ = std::thread([this, batch_limit]() {
    int64_t batch_count = 0;
    is_running_.store(true);

    while (is_running_) {
      int64_t remaining_timeout_in_ms =
          info_.batch_interval_in_ms.value_or(kDefaultBatchIntervalMillis);
      int64_t remaining_size = info_.batch_size.value_or(kDefaultBatchSize);

      auto start = std::chrono::system_clock::now();

      bool run_batch = true;
      while (is_running_ && run_batch && remaining_size-- > 0) {
        std::unique_ptr<RdKafka::Message> msg(
            consumer_->consume(remaining_timeout_in_ms));
        switch (msg->err()) {
          case RdKafka::ERR__TIMED_OUT:
            run_batch = false;
            break;

          case RdKafka::ERR_NO_ERROR:
            // TODO (msantl): store message to current batch and pass the batch
            // to transform
            break;

          default:
            LOG(ERROR) << "Consumer error: " << msg->errstr();
            is_running_.store(false);
            break;
        }

        auto now = std::chrono::system_clock::now();
        auto took =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
        if (took.count() >= remaining_timeout_in_ms) {
          break;
        }

        remaining_timeout_in_ms = remaining_timeout_in_ms - took.count();
        start = now;
      }

      if (batch_limit != std::experimental::nullopt) {
        batch_count++;
        if (batch_limit <= batch_count) {
          is_running_.store(false);
        }
      }
    }
  });
}

void Consumer::Start(std::experimental::optional<int64_t> batch_limit) {
  if (!consumer_) {
    throw ConsumerNotAvailableException(info_.stream_name);
  }

  if (is_running_) {
    throw ConsumerRunningException(info_.stream_name);
  }

  StartConsuming(batch_limit);
}

void Consumer::Stop() {
  if (!consumer_) {
    throw ConsumerNotAvailableException(info_.stream_name);
  }

  if (!is_running_) {
    throw ConsumerStoppedException(info_.stream_name);
  }

  StopConsuming();
}

void Consumer::StartIfNotStopped() {
  if (!consumer_) {
    throw ConsumerNotAvailableException(info_.stream_name);
  }

  if (!is_running_) {
    StartConsuming(std::experimental::nullopt);
  }
}

void Consumer::StopIfNotRunning() {
  if (!consumer_) {
    throw ConsumerNotAvailableException(info_.stream_name);
  }

  if (is_running_) {
    StopConsuming();
  }
}

StreamInfo Consumer::info() {
  info_.is_running = is_running_;
  return info_;
}

}  // namespace kafka
}  // namespace integrations
