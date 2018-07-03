#pragma once

#include "integrations/kafka/consumer.hpp"

#include <experimental/optional>
#include <mutex>
#include <unordered_map>

namespace integrations {
namespace kafka {

class Streams final {
 public:
  void CreateStream(const StreamInfo &info);

  void DropStream(const std::string &stream_name);

  void StartStream(const std::string &stream_name,
                   std::experimental::optional<int64_t> batch_limit =
                       std::experimental::nullopt);

  void StopStream(const std::string &stream_name);

  void StartAllStreams();

  void StopAllStreams();

  std::vector<StreamInfo> ShowStreams();

  std::vector<std::string> TestStream(
      const std::string &stream_name,
      std::experimental::optional<int64_t> batch_limit =
          std::experimental::nullopt);

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, Consumer> consumers_;

  // TODO (msantl): persist stream storage
};

}  // namespace kafka
}  // namespace integrations
