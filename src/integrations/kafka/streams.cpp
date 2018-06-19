#include "integrations/kafka/streams.hpp"
#include "integrations/kafka/exceptions.hpp"

namespace integrations {
namespace kafka {

void Streams::CreateStream(const StreamInfo &info) {
  std::lock_guard<std::mutex> g(mutex_);
  if (consumers_.find(info.stream_name) != consumers_.end())
    throw StreamExistsException(info.stream_name);

  consumers_.emplace(info.stream_name, info);
}

void Streams::DropStream(const std::string &stream_name) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  consumers_.erase(find_it);
}

void Streams::StartStream(const std::string &stream_name,
                          std::experimental::optional<int64_t> batch_limit) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  find_it->second.Start(batch_limit);
}

void Streams::StopStream(const std::string &stream_name) {
  std::lock_guard<std::mutex> g(mutex_);
  auto find_it = consumers_.find(stream_name);
  if (find_it == consumers_.end())
    throw StreamDoesntExistException(stream_name);

  find_it->second.Stop();
}

void Streams::StartAllStreams() {
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    consumer_kv.second.StartIfNotStopped();
  }
}

void Streams::StopAllStreams() {
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    consumer_kv.second.StopIfNotRunning();
  }
}

std::vector<StreamInfo> Streams::ShowStreams() {
  std::vector<StreamInfo> streams;
  std::lock_guard<std::mutex> g(mutex_);
  for (auto &consumer_kv : consumers_) {
    streams.emplace_back(consumer_kv.second.info());
  }

  return streams;
}

}  // namespace kafka
}  // namespace integrations
