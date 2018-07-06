#pragma once

#include <atomic>
#include <experimental/optional>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rdkafkacpp.h"

#include "integrations/kafka/transform.hpp"

namespace integrations {
namespace kafka {

struct StreamInfo {
  std::string stream_name;
  std::string stream_uri;
  std::string stream_topic;
  std::string transform_uri;
  std::experimental::optional<int64_t> batch_interval_in_ms;
  std::experimental::optional<int64_t> batch_size;

  std::experimental::optional<int64_t> limit_batches;

  bool is_running = false;
};

class Consumer final : public RdKafka::EventCb {
 public:
  Consumer() = delete;

  Consumer(const StreamInfo &info, const std::string &transform_script_path,
           std::function<void(const std::vector<std::string> &)> stream_writer);

  Consumer(const Consumer &other) = delete;
  Consumer(Consumer &&other) = delete;

  Consumer &operator=(const Consumer &other) = delete;
  Consumer &operator=(Consumer &&other) = delete;

  void Start(std::experimental::optional<int64_t> limit_batches);

  void Stop();

  void StartIfStopped();

  void StopIfRunning();

  std::vector<std::string> Test(
      std::experimental::optional<int64_t> limit_batches);

  StreamInfo info();

 private:
  StreamInfo info_;
  std::string transform_script_path_;
  std::function<void(const std::vector<std::string> &)> stream_writer_;

  Transform transform_;

  std::atomic<bool> is_running_{false};
  std::thread thread_;

  std::unique_ptr<RdKafka::KafkaConsumer,
                  std::function<void(RdKafka::KafkaConsumer *)>>
      consumer_;

  void event_cb(RdKafka::Event &event) override;

  void StopConsuming();

  void StartConsuming(std::experimental::optional<int64_t> limit_batches);

  std::vector<std::unique_ptr<RdKafka::Message>> GetBatch();
};

}  // namespace kafka
}  // namespace integrations
