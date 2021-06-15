#include "query/streams.hpp"

#include <string_view>
#include <utility>

#include <json/json.hpp>
#include "query/interpreter.hpp"

namespace query {

using Consumer = integrations::kafka::Consumer;
using ConsumerInfo = integrations::kafka::ConsumerInfo;
using Message = integrations::kafka::Message;

// nlohmann::json doesn't support string_view access yet
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kIsRunningKey{"is_running"};

void to_json(nlohmann::json &data, StreamStatus &&status) {
  auto &info = status.info;
  data[kTopicsKey] = std::move(info.topics);
  data[kConsumerGroupKey] = info.consumer_group;

  if (info.batch_interval) {
    data[kBatchIntervalKey] = info.batch_interval->count();
  } else {
    data[kBatchIntervalKey] = nullptr;
  }

  if (info.batch_size) {
    data[kBatchSizeKey] = *info.batch_size;
  } else {
    data[kBatchSizeKey] = nullptr;
  }

  data[kIsRunningKey] = status.is_running;
}

void from_json(const nlohmann::json &data, StreamStatus &status) {
  auto &info = status.info;
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kConsumerGroupKey).get_to(info.consumer_group);

  const auto batch_interval = data.at(kBatchIntervalKey);
  if (!batch_interval.is_null()) {
    using BatchInterval = decltype(info.batch_interval)::value_type;
    info.batch_interval = BatchInterval{batch_interval.get<BatchInterval::rep>()};
  } else {
    info.batch_interval = {};
  }

  const auto batch_size = data.at(kBatchSizeKey);
  if (info.batch_size) {
    info.batch_size = batch_size.get<decltype(info.batch_size)::value_type>();
  } else {
    info.batch_size = {};
  }

  data.at(kIsRunningKey).get_to(status.is_running);
}

Streams::Streams(InterpreterContext *interpreter_context, std::string bootstrap_servers,
                 std::filesystem::path directory)
    : interpreter_context_(interpreter_context),
      bootstrap_servers_(std::move(bootstrap_servers)),
      storage_(std::move(directory)) {}

void Streams::RestoreStreams() {
  spdlog::info("Loading streams...");
  MG_ASSERT(streams_.empty(), "Cannot restore streams when some streams already exist!");

  std::lock_guard lock(mutex_);

  for (const auto &[stream_name, stream_data] : storage_) {
    const auto get_failed_message = [](const std::string_view stream_name, const std::string_view message,
                                       const std::string_view nested_message) {
      return fmt::format("Failed to load stream '{}', because: {} caused by {}", stream_name, message, nested_message);
    };

    StreamStatus status;
    try {
      nlohmann::json::parse(stream_data).get_to(status);
    } catch (const nlohmann::json::type_error &exception) {
      spdlog::warn(get_failed_message(stream_name, "invalid type conversion", exception.what()));
      continue;
    } catch (const nlohmann::json::out_of_range &exception) {
      spdlog::warn(get_failed_message(stream_name, "non existing field", exception.what()));
      continue;
    }

    try {
      auto it = CreateConsumer(lock, stream_name, std::move(status.info));
      if (status.is_running) {
        it->second.consumer->Start();
      }
    } catch (const utils::BasicException &exception) {
      spdlog::warn(get_failed_message(stream_name, "unexpected error", exception.what()));
    }
  }
}

void Streams::Create(const std::string &stream_name, StreamInfo info) {
  std::lock_guard lock{mutex_};
  auto it = CreateConsumer(lock, stream_name, std::move(info));
  if (!storage_.Put(it->first, nlohmann::json(CreateStatusFromData(it->second)).dump())) {
    streams_.erase(it);
    throw StreamsException{"Couldn't persist stream '{}'", it->first};
  };
}

void Streams::Drop(const std::string &stream_name) {
  std::lock_guard lock(mutex_);
  auto it = GetStream(lock, stream_name);

  streams_.erase(it);
  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name) {
  std::lock_guard lock(mutex_);
  auto it = GetStream(lock, stream_name);

  it->second.consumer->Start();

  Persist(it->first, it->second);
}

void Streams::Stop(const std::string &stream_name) {
  std::lock_guard lock(mutex_);
  auto it = GetStream(lock, stream_name);

  it->second.consumer->Stop();

  Persist(it->first, it->second);
}

void Streams::StartAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto &[stream_name, stream_data] : streams_) {
    if (!stream_data.consumer->IsRunning()) {
      stream_data.consumer->Start();
      Persist(stream_name, stream_data);
    }
  }
}

void Streams::StopAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto &[stream_name, stream_data] : streams_) {
    if (stream_data.consumer->IsRunning()) {
      stream_data.consumer->Stop();
      Persist(stream_name, stream_data);
    }
  }
}

std::vector<std::pair<std::string, StreamStatus>> Streams::Show() const {
  std::vector<std::pair<std::string, StreamStatus>> result;
  {
    std::lock_guard lock(mutex_);
    for (const auto &[stream_name, stream_data] : streams_) {
      // Create string
      result.emplace_back(stream_name, CreateStatusFromData(stream_data));
    }
  }
  return result;
}

TransformationResult Streams::Test(const std::string &stream_name, std::optional<int64_t> batch_limit) {
  std::lock_guard lock(mutex_);
  auto it = GetStream(lock, stream_name);
  TransformationResult result;
  auto consumer_function = [&result](const std::vector<Message> &messages) {
    for (const auto &message : messages) {
      // TODO(antaljanosbenjamin) Update the logic with using the transform from modules
      const auto payload = message.Payload();
      const std::string_view payload_as_string_view{payload.data(), payload.size()};
      result[fmt::format("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view)] = "replace with params";
    }
  };

  it->second.consumer->Test(batch_limit, consumer_function);

  return result;
}

StreamStatus Streams::CreateStatusFromData(const Streams::StreamData &data) {
  auto info = data.consumer->Info();
  return StreamStatus{StreamInfo{
                          std::move(info.topics),
                          std::move(info.consumer_group),
                          info.batch_interval,
                          info.batch_size,
                          data.transformation_name,
                      },
                      data.consumer->IsRunning()};
}

Streams::StreamsMap::iterator Streams::CreateConsumer(const std::lock_guard<std::mutex> &lock,
                                                      const std::string &stream_name, StreamInfo info) {
  if (streams_.contains(stream_name)) {
    throw StreamsException{"Stream already exists with name '{}'", stream_name};
  }

  auto consumer_function = [interpreter_context =
                                interpreter_context_](const std::vector<integrations::kafka::Message> &messages) {
    Interpreter interpreter = Interpreter{interpreter_context};
    TransformationResult result;

    for (const auto &message : messages) {
      // TODO(antaljanosbenjamin) Update the logic with using the transform from modules
      const auto payload = message.Payload();
      const std::string_view payload_as_string_view{payload.data(), payload.size()};
      result[fmt::format("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view)] = "replace with params";
    }

    for (const auto &[query, params] : result) {
      // auto prepared_query = interpreter.Prepare(query, {});
      spdlog::info("Executing query '{}'", query);
      // TODO(antaljanosbenjamin) run the query in real life, try not to copy paste the whole execution code, but
      // extract it to a function that can be called from multiple places (e.g: triggers)
    }
  };

  ConsumerInfo consumer_info{
      .consumer_name = stream_name,
      .topics = std::move(info.topics),
      .consumer_group = std::move(info.consumer_group),
      .batch_interval = info.batch_interval,
      .batch_size = info.batch_size,
  };

  auto consumer =
      std::make_unique<Consumer>(bootstrap_servers_, std::move(consumer_info), std::move(consumer_function));

  auto emplace_result =
      streams_.emplace(stream_name, StreamData{std::move(info.transformation_name), std::move(consumer)});
  MG_ASSERT(emplace_result.second, "Unexpected error during storing consumer '{}'", stream_name);
  return emplace_result.first;
}

Streams::StreamsMap::iterator Streams::GetStream(const std::lock_guard<std::mutex> &lock,
                                                 const std::string &stream_name) {
  auto it = streams_.find(stream_name);
  if (it == streams_.end()) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  }
  return it;
}

void Streams::Persist(const std::string &stream_name, const StreamData &data) {
  if (!storage_.Put(stream_name, nlohmann::json(CreateStatusFromData(data)).dump())) {
    throw StreamsException{"Couldn't persist steam data for stream '{}'", stream_name};
  }
}

}  // namespace query
