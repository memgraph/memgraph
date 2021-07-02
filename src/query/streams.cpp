#include "query/streams.hpp"

#include <shared_mutex>
#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>
#include <json/json.hpp>
#include "query/interpreter.hpp"
#include "utils/on_scope_exit.hpp"

namespace query {

namespace {
auto GetStream(auto &map, const std::string &stream_name) {
  if (auto it = map.find(stream_name); it != map.end()) {
    return it;
  }
  throw StreamsException("Couldn't find stream '{}'", stream_name);
}
}  // namespace

using Consumer = integrations::kafka::Consumer;
using ConsumerInfo = integrations::kafka::ConsumerInfo;
using Message = integrations::kafka::Message;

// nlohmann::json doesn't support string_view access yet
const std::string kStreamName{"name"};
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kIsRunningKey{"is_running"};

void to_json(nlohmann::json &data, StreamStatus &&status) {
  auto &info = status.info;
  data[kStreamName] = std::move(status.name);
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
  data.at(kStreamName).get_to(status.name);
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

Streams::Streams(InterpreterContext *interpreter_context, std::string bootstrap_servers,
                 std::filesystem::path directory)
    : interpreter_context_(interpreter_context),
      bootstrap_servers_(std::move(bootstrap_servers)),
      storage_(std::move(directory)) {}

void Streams::RestoreStreams() {
  spdlog::info("Loading streams...");
  auto locked_streams_map = streams_.Lock();
  MG_ASSERT(locked_streams_map->empty(), "Cannot restore streams when some streams already exist!");

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
    MG_ASSERT(status.name == stream_name, "Expected stream name is '{}', but got '{}'", stream_name, status.name);

    try {
      auto it = CreateConsumer(*locked_streams_map, stream_name, std::move(status.info));
      if (status.is_running) {
        it->second.consumer->Lock()->Start();
      }
    } catch (const utils::BasicException &exception) {
      spdlog::warn(get_failed_message(stream_name, "unexpected error", exception.what()));
    }
  }
}

void Streams::Create(const std::string &stream_name, StreamInfo info) {
  auto locked_streams = streams_.Lock();
  auto it = CreateConsumer(*locked_streams, stream_name, std::move(info));

  try {
    Persist(CreateStatus(stream_name, it->second.transformation_name, *it->second.consumer->ReadLock()));
  } catch (...) {
    locked_streams->erase(it);
    throw;
  }
}

void Streams::Drop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();

  auto it = GetStream(*locked_streams, stream_name);

  // streams_ is write locked, which means there is no access to it outside of this function, thus only the Test
  // function can be executing with the consumer, nothing else.
  // By acquiring the write lock here for the consumer, we make sure there is
  // no running Test function for this consumer, therefore it can be erased.
  it->second.consumer->Lock();
  locked_streams->erase(it);

  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  auto locked_consumer = it->second.consumer->Lock();
  locked_consumer->Start();

  Persist(CreateStatus(stream_name, it->second.transformation_name, *locked_consumer));
}

void Streams::Stop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  auto locked_consumer = it->second.consumer->Lock();
  locked_consumer->Stop();

  Persist(CreateStatus(stream_name, it->second.transformation_name, *locked_consumer));
}

void Streams::StartAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    auto locked_consumer = stream_data.consumer->Lock();
    if (!locked_consumer->IsRunning()) {
      locked_consumer->Start();
      Persist(CreateStatus(stream_name, stream_data.transformation_name, *locked_consumer));
    }
  }
}

void Streams::StopAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    auto locked_consumer = stream_data.consumer->Lock();
    if (locked_consumer->IsRunning()) {
      locked_consumer->Stop();
      Persist(CreateStatus(stream_name, stream_data.transformation_name, *locked_consumer));
    }
  }
}

std::vector<StreamStatus> Streams::GetStreamInfo() const {
  std::vector<StreamStatus> result;
  {
    for (auto locked_streams = streams_.ReadLock(); const auto &[stream_name, stream_data] : *locked_streams) {
      result.emplace_back(
          CreateStatus(stream_name, stream_data.transformation_name, *stream_data.consumer->ReadLock()));
    }
  }
  return result;
}

TransformationResult Streams::Test(const std::string &stream_name, std::optional<int64_t> batch_limit) const {
  TransformationResult result;
  auto consumer_function = [&result](const std::vector<Message> &messages) {
    for (const auto &message : messages) {
      // TODO(antaljanosbenjamin) Update the logic with using the transform from modules
      const auto payload = message.Payload();
      const std::string_view payload_as_string_view{payload.data(), payload.size()};
      spdlog::info("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view);
      result[fmt::format("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view)] = {};
    }
  };

  // This depends on the fact that Drop will first acquire a write lock to the consumer, and erase it only after that
  auto locked_consumer = [this, &stream_name] {
    auto locked_streams = streams_.ReadLock();
    auto it = GetStream(*locked_streams, stream_name);
    return it->second.consumer->ReadLock();
  }();

  locked_consumer->Test(batch_limit, consumer_function);

  return result;
}

StreamStatus Streams::CreateStatus(const std::string &name, const std::string &transformation_name,
                                   const integrations::kafka::Consumer &consumer) {
  const auto &info = consumer.Info();
  return StreamStatus{name,
                      StreamInfo{
                          info.topics,
                          info.consumer_group,
                          info.batch_interval,
                          info.batch_size,
                          transformation_name,
                      },
                      consumer.IsRunning()};
}

Streams::StreamsMap::iterator Streams::CreateConsumer(StreamsMap &map, const std::string &stream_name,
                                                      StreamInfo stream_info) {
  if (map.contains(stream_name)) {
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
      result[fmt::format("CREATE (n:MESSAGE {{payload: '{}'}})", payload_as_string_view)] = {};
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
      .topics = std::move(stream_info.topics),
      .consumer_group = std::move(stream_info.consumer_group),
      .batch_interval = stream_info.batch_interval,
      .batch_size = stream_info.batch_size,
  };

  auto insert_result = map.insert_or_assign(
      stream_name, StreamData{std::move(stream_info.transformation_name),
                              std::make_unique<SynchronizedConsumer>(bootstrap_servers_, std::move(consumer_info),
                                                                     std::move(consumer_function))});
  MG_ASSERT(insert_result.second, "Unexpected error during storing consumer '{}'", stream_name);
  return insert_result.first;
}

void Streams::Persist(StreamStatus &&status) {
  const std::string stream_name = status.name;
  if (!storage_.Put(stream_name, nlohmann::json(std::move(status)).dump())) {
    throw StreamsException{"Couldn't persist steam data for stream '{}'", stream_name};
  }
}

}  // namespace query
