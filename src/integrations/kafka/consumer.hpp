#pragma once

#include <atomic>
#include <experimental/optional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rdkafkacpp.h"

namespace integrations {
namespace kafka {

struct StreamInfo {
  std::string stream_name;
  std::string stream_uri;
  std::string stream_topic;
  std::string transform_uri;
  std::experimental::optional<int64_t> batch_interval_in_ms;
  std::experimental::optional<int64_t> batch_size;

  bool is_running = false;
};

class Consumer final : public RdKafka::EventCb {
 public:
  Consumer() = delete;

  explicit Consumer(const StreamInfo &info);

  Consumer(const Consumer &other) = delete;
  Consumer(Consumer &&other) = delete;

  Consumer &operator=(const Consumer &other) = delete;
  Consumer &operator=(Consumer &&other) = delete;

  void Start(std::experimental::optional<int64_t> batch_limit);

  void Stop();

  void StartIfNotStopped();

  void StopIfNotRunning();

  StreamInfo info();

 private:
  void event_cb(RdKafka::Event &event) override;

  StreamInfo info_;

  std::atomic<bool> is_running_{false};
  std::thread thread_;

  std::unique_ptr<RdKafka::KafkaConsumer,
                  std::function<void(RdKafka::KafkaConsumer *)>>
      consumer_;

  void StopConsuming();

  void StartConsuming(std::experimental::optional<int64_t> batch_limit);
};

}  // namespace kafka
}  // namespace integrations
