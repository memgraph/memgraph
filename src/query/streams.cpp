#include "query/streams.hpp"

#include <string_view>
#include <utility>

#include <json/json.hpp>
#include "query/interpreter.hpp"

namespace query {

using Consumer = integrations::kafka::Consumer;
using ConsumerInfo = integrations::kafka::ConsumerInfo;

// nlohmann::json doesn't support string_view access yet
const std::string kStreamNameKey{"stream_name"};
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kIsRunningKey{"is_running"};

void to_json(nlohmann::json &data, const StreamStatus &status) {
  data[kStreamNameKey] = status.name;
  data[kTopicsKey] = status.topics;
  data[kConsumerGroupKey] = status.consumer_group;

  if (status.batch_interval) {
    data[kBatchIntervalKey] = status.batch_interval->count();
  } else {
    data[kBatchIntervalKey] = nullptr;
  }

  if (status.batch_size) {
    data[kBatchSizeKey] = *status.batch_size;
  } else {
    data[kBatchSizeKey] = nullptr;
  }

  data[kIsRunningKey] = status.is_running;
}

void from_json(const nlohmann::json &data, StreamStatus &status) {
  data.at(kStreamNameKey).get_to(status.name);
  data.at(kTopicsKey).get_to(status.topics);
  data.at(kConsumerGroupKey).get_to(status.consumer_group);

  const auto batch_interval = data.at(kBatchIntervalKey);
  if (!batch_interval.is_null()) {
    using BatchInterval = decltype(status.batch_interval)::value_type;
    status.batch_interval = BatchInterval{batch_interval.get<BatchInterval::rep>()};
  } else {
    status.batch_interval = {};
  }

  const auto batch_size = data.at(kBatchSizeKey);
  if (status.batch_size) {
    status.batch_size = batch_size.get<decltype(status.batch_size)::value_type>();
  } else {
    status.batch_size = {};
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

  std::lock_guard lock(mutex_);

  for (const auto &[stream_name, stream_data] : storage_) {
    const auto get_failed_message = [](const std::string_view stream_name, const std::string_view message,
                                       const std::string_view nested_message) {
      return fmt::format("Failed to load stream '{}', because: {} caused by {}", stream_name, message, nested_message);
    };

    StreamStatus status;
    status.name = "unknown stream";
    try {
      nlohmann::json::parse(stream_data).get_to(status);
    } catch (const nlohmann::json::type_error &exception) {
      spdlog::warn(get_failed_message(status.name, "invalid type conversion", exception.what()));
      continue;
    } catch (const nlohmann::json::out_of_range &exception) {
      spdlog::warn(get_failed_message(status.name, "non existing field", exception.what()));
      continue;
    }

    try {
      CreateConsumer(lock, std::move(status));
    } catch (const utils::BasicException &exception) {
      spdlog::warn("Couldn't create stream: {}", exception.what());
    }
  }
}

void Streams::Create(StreamStatus status) {
  std::lock_guard lock{mutex_};
  try {
    auto it = CreateConsumer(lock, std::move(status));
    if (!storage_.Put(it->first, nlohmann::json(it->second.status).dump())) {
      spdlog::error("Couldn't persist stream '{}'", it->first);
      it->second.consumer->StopIfRunning();
      streams_.erase(it);
      throw StreamsException{"Couldn't persist steam data"};
    };
  } catch (const utils::BasicException &exception) {
    throw StreamsException{fmt::format("Couldn't create consumer: {}", exception.what())};
  }
}

void Streams::Drop(const std::string &stream_name) {
  std::lock_guard lock(mutex_);
  auto find_it = streams_.find(stream_name);
  if (find_it == streams_.end()) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  }

  // Erase and implicitly stop the consumer.
  streams_.erase(find_it);

  // Remove stream_info in metadata_store_.
  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name, std::optional<int64_t> batch_limit) {
  std::lock_guard lock(mutex_);
  auto it = streams_.find(stream_name);
  if (it == streams_.end()) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  };

  it->second.consumer->Start(batch_limit);
  it->second.status.is_running = true;

  Persist(it->first, it->second.status);
}

void Streams::Stop(const std::string &stream_name) {
  std::lock_guard lock(mutex_);
  auto it = streams_.find(stream_name);
  if (it == streams_.end()) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  };

  it->second.consumer->Stop();
  it->second.status.is_running = false;

  Persist(it->first, it->second.status);
}

void Streams::StartAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto &[stream_name, stream_data] : streams_) {
    if (!stream_data.consumer->IsRunning()) {
      stream_data.consumer->Start({});
      stream_data.status.is_running = true;
      PersistNoThrow(stream_name, stream_data.status);
    }
  }
}

void Streams::StopAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto &[stream_name, stream_data] : streams_) {
    if (stream_data.consumer->IsRunning()) {
      stream_data.consumer->Stop();
      stream_data.status.is_running = false;
      PersistNoThrow(stream_name, stream_data.status);
    }
  }
}

std::vector<StreamStatus> Streams::Show() {
  std::vector<StreamStatus> result;
  {
    std::lock_guard lock(mutex_);
    for (auto &[stream_name, stream_data] : streams_) {
      result.emplace_back(stream_data.status);
    }
  }
  return result;
}

TransformationResult Streams::Test(const std::string &stream_name, std::optional<int64_t> batch_limit) {
  std::lock_guard lock(mutex_);
  // TODO(antaljanosbenjamin)
  return {};
}

Streams::StreamsMap::const_iterator Streams::CreateConsumer(const std::lock_guard<std::mutex> &lock,
                                                            StreamStatus stream_status) {
  MG_ASSERT(!streams_.contains(stream_status.name), "Stream already exists with name '{}'", stream_status.name);

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
      .consumer_name = stream_status.name,
      .topics = stream_status.topics,
      .consumer_group = stream_status.consumer_group,
      .batch_interval = stream_status.batch_interval,
      .batch_size = stream_status.batch_size,
  };

  auto consumer =
      std::make_unique<Consumer>(bootstrap_servers_, std::move(consumer_info), std::move(consumer_function));

  auto stream_name = stream_status.name;
  auto emplace_result =
      streams_.emplace(std::move(stream_name), StreamData{std::move(stream_status), std::move(consumer)});
  MG_ASSERT(emplace_result.second, "Unexpected error during storing Consumer");
  // TODO(antaljanosbenjamin) Should the consumer be started? How to handle if it was previously started with a limited
  // batch_size?
  return emplace_result.first;
}

void Streams::Persist(const std::string &stream_name, const StreamStatus &status) {
  if (!storage_.Put(stream_name, nlohmann::json(status).dump())) {
    throw StreamsException{"Couldn't persist steam data for stream '{}'", stream_name};
  }
}

void Streams::PersistNoThrow(const std::string &stream_name, const StreamStatus &status) {
  if (!storage_.Put(stream_name, nlohmann::json(status).dump())) {
    spdlog::warn("Couldn't persist stream data for stream '{}'", stream_name);
  }
}

}  // namespace query
