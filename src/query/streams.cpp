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
  if (!batch_size.is_null()) {
    info.batch_size = batch_size.get<decltype(info.batch_size)::value_type>();
  } else {
    info.batch_size = {};
  }

  data.at(kIsRunningKey).get_to(status.is_running);
}

bool operator==(const StreamData &lhs, const StreamData &rhs) { return lhs.name == rhs.name; }
bool operator<(const StreamData &lhs, const StreamData &rhs) { return lhs.name < rhs.name; }

bool operator==(const StreamData &stream, const std::string &stream_name) { return stream.name == stream_name; }
bool operator<(const StreamData &stream, const std::string &stream_name) { return stream.name < stream_name; }

Streams::Streams(InterpreterContext *interpreter_context, std::string bootstrap_servers,
                 std::filesystem::path directory)
    : interpreter_context_(interpreter_context),
      bootstrap_servers_(std::move(bootstrap_servers)),
      storage_(std::move(directory)) {}

void Streams::RestoreStreams() {
  spdlog::info("Loading streams...");
  auto accessor = streams_.access();
  MG_ASSERT(accessor.size() == 0, "Cannot restore streams when some streams already exist!");

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
      CreateConsumer(accessor, stream_name, std::move(status.info), status.is_running, false);
    } catch (const utils::BasicException &exception) {
      spdlog::warn(get_failed_message(stream_name, "unexpected error", exception.what()));
    }
  }
}

void Streams::Create(const std::string &stream_name, StreamInfo info) {
  auto accessor = streams_.access();
  CreateConsumer(accessor, stream_name, std::move(info), false, true);
}

void Streams::Drop(const std::string &stream_name) {
  auto accessor = streams_.access();

  if (!accessor.remove(stream_name)) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  }

  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name) {
  auto accessor = streams_.access();
  auto it = GetStream(accessor, stream_name);

  auto lock = it->consumer->Lock();
  lock->Start();

  Persist(it->name, it->transformation_name, *lock);
}

void Streams::Stop(const std::string &stream_name) {
  auto accessor = streams_.access();
  auto it = GetStream(accessor, stream_name);

  auto lock = it->consumer->Lock();
  lock->Stop();

  Persist(it->name, it->transformation_name, *lock);
}

void Streams::StartAll() {
  for (auto &stream_data : streams_.access()) {
    stream_data.consumer->WithLock([this, &stream_data](auto &consumer) {
      if (!consumer.IsRunning()) {
        consumer.Start();
        Persist(stream_data.name, stream_data.transformation_name, consumer);
      }
    });
  }
}

void Streams::StopAll() {
  for (auto &stream_data : streams_.access()) {
    stream_data.consumer->WithLock([this, &stream_data](auto &consumer) {
      if (consumer.IsRunning()) {
        consumer.Stop();
        Persist(stream_data.name, stream_data.transformation_name, consumer);
      }
    });
  }
}

std::vector<std::pair<std::string, StreamStatus>> Streams::Show() const {
  std::vector<std::pair<std::string, StreamStatus>> result;
  {
    for (const auto &stream_data : streams_.access()) {
      // Create string
      result.emplace_back(stream_data.name,
                          CreateStatus(stream_data.transformation_name, *stream_data.consumer->ReadLock()));
    }
  }
  return result;
}

TransformationResult Streams::Test(const std::string &stream_name, std::optional<int64_t> batch_limit) {
  const auto accessor = streams_.access();
  auto it = GetStream(accessor, stream_name);
  TransformationResult result;
  auto consumer_function = [&result](const std::vector<Message> &messages) {
    for (const auto &message : messages) {
      // TODO(antaljanosbenjamin) Update the logic with using the transform from modules
      const auto payload = message.Payload();
      const std::string_view payload_as_string_view{payload.data(), payload.size()};
      result[fmt::format("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view)] = "replace with params";
    }
  };

  it->consumer->Lock()->Test(batch_limit, consumer_function);

  return result;
}

StreamStatus Streams::CreateStatus(const std::string &transformation_name,
                                   const integrations::kafka::Consumer &consumer) {
  const auto &info = consumer.Info();
  return StreamStatus{StreamInfo{
                          info.topics,
                          info.consumer_group,
                          info.batch_interval,
                          info.batch_size,
                          transformation_name,
                      },
                      consumer.IsRunning()};
}

void Streams::CreateConsumer(utils::SkipList<StreamData>::Accessor &accessor, const std::string &stream_name,
                             StreamInfo info, const bool start_consumer, const bool persist_consumer) {
  if (accessor.contains(stream_name)) {
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

  auto consumer = std::make_unique<SynchronizedConsumer>(bootstrap_servers_, std::move(consumer_info),
                                                         std::move(consumer_function));
  auto locked_consumer = consumer->Lock();

  if (start_consumer) {
    locked_consumer->Start();
  }
  if (persist_consumer) {
    Persist(stream_name, info.transformation_name, *locked_consumer);
  }

  auto insert_result =
      accessor.insert(StreamData{stream_name, std::move(info.transformation_name), std::move(consumer)});
  MG_ASSERT(insert_result.second, "Unexpected error during storing consumer '{}'", stream_name);
}

utils::SkipList<StreamData>::Iterator Streams::GetStream(const utils::SkipList<StreamData>::Accessor &accessor,
                                                         const std::string &stream_name) {
  auto it = accessor.find(stream_name);
  if (it == accessor.end()) {
    throw StreamsException("Couldn't find stream '{}'", stream_name);
  }
  return it;
}

void Streams::Persist(const std::string &name, const std::string &transformation_name,
                      const integrations::kafka::Consumer &consumer) {
  if (!storage_.Put(name, nlohmann::json(CreateStatus(transformation_name, consumer)).dump())) {
    throw StreamsException{"Couldn't persist steam data for stream '{}'", name};
  }
}

}  // namespace query
