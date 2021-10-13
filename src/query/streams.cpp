// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/streams.hpp"

#include <shared_mutex>
#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>
#include <json/json.hpp>
#include "query/db_accessor.hpp"
#include "query/discard_value_stream.hpp"
#include "query/interpreter.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/typed_value.hpp"
#include "utils/event_counter.hpp"
#include "utils/memory.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/pmr/string.hpp"

namespace EventCounter {
extern const Event MessagesConsumed;
}  // namespace EventCounter

namespace query {

using Consumer = integrations::kafka::Consumer;
using ConsumerInfo = integrations::kafka::ConsumerInfo;
using Message = integrations::kafka::Message;
namespace {
constexpr auto kExpectedTransformationResultSize = 2;
const utils::pmr::string query_param_name{"query", utils::NewDeleteResource()};
const utils::pmr::string params_param_name{"parameters", utils::NewDeleteResource()};
const std::map<std::string, storage::PropertyValue> empty_parameters{};

auto GetStream(auto &map, const std::string &stream_name) {
  if (auto it = map.find(stream_name); it != map.end()) {
    return it;
  }
  throw StreamsException("Couldn't find stream '{}'", stream_name);
}

void CallCustomTransformation(const std::string &transformation_name, const std::vector<Message> &messages,
                              mgp_result &result, storage::Storage::Accessor &storage_accessor,
                              utils::MemoryResource &memory_resource, const std::string &stream_name) {
  DbAccessor db_accessor{&storage_accessor};
  {
    auto maybe_transformation =
        procedure::FindTransformation(procedure::gModuleRegistry, transformation_name, utils::NewDeleteResource());

    if (!maybe_transformation) {
      throw StreamsException("Couldn't find transformation {} for stream '{}'", transformation_name, stream_name);
    };
    const auto &trans = *maybe_transformation->second;
    mgp_messages mgp_messages{mgp_messages::storage_type{&memory_resource}};
    std::transform(messages.begin(), messages.end(), std::back_inserter(mgp_messages.messages),
                   [](const integrations::kafka::Message &message) { return mgp_message{&message}; });
    mgp_graph graph{&db_accessor, storage::View::OLD, nullptr};
    mgp_memory memory{&memory_resource};
    result.rows.clear();
    result.error_msg.reset();
    result.signature = &trans.results;

    MG_ASSERT(result.signature->size() == kExpectedTransformationResultSize);
    MG_ASSERT(result.signature->contains(query_param_name));
    MG_ASSERT(result.signature->contains(params_param_name));

    spdlog::trace("Calling transformation in stream '{}'", stream_name);
    trans.cb(&mgp_messages, &graph, &result, &memory);
  }
  if (result.error_msg.has_value()) {
    throw StreamsException(result.error_msg->c_str());
  }
}

std::pair<TypedValue /*query*/, TypedValue /*parameters*/> ExtractTransformationResult(
    utils::pmr::map<utils::pmr::string, TypedValue> &&values, const std::string_view transformation_name,
    const std::string_view stream_name) {
  if (values.size() != kExpectedTransformationResultSize) {
    throw StreamsException(
        "Transformation '{}' in stream '{}' did not yield all fields (query, parameters) as required.",
        transformation_name, stream_name);
  }

  auto get_value = [&](const utils::pmr::string &field_name) mutable -> TypedValue & {
    auto it = values.find(field_name);
    if (it == values.end()) {
      throw StreamsException{"Transformation '{}' in stream '{}' did not yield a record with '{}' field.",
                             transformation_name, stream_name, field_name};
    };
    return it->second;
  };

  auto &query_value = get_value(query_param_name);
  MG_ASSERT(query_value.IsString());
  auto &params_value = get_value(params_param_name);
  MG_ASSERT(params_value.IsNull() || params_value.IsMap());
  return {std::move(query_value), std::move(params_value)};
}
}  // namespace

// nlohmann::json doesn't support string_view access yet
const std::string kStreamName{"name"};
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBatchIntervalKey{"batch_interval"};
const std::string kBatchSizeKey{"batch_size"};
const std::string kIsRunningKey{"is_running"};
const std::string kTransformationName{"transformation_name"};
const std::string kOwner{"owner"};
const std::string kBoostrapServers{"bootstrap_servers"};

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
  data[kTransformationName] = status.info.transformation_name;

  if (info.owner.has_value()) {
    data[kOwner] = std::move(*info.owner);
  } else {
    data[kOwner] = nullptr;
  }

  data[kBoostrapServers] = std::move(info.bootstrap_servers);
}

void from_json(const nlohmann::json &data, StreamStatus &status) {
  auto &info = status.info;
  data.at(kStreamName).get_to(status.name);
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kConsumerGroupKey).get_to(info.consumer_group);

  if (const auto batch_interval = data.at(kBatchIntervalKey); !batch_interval.is_null()) {
    using BatchInterval = decltype(info.batch_interval)::value_type;
    info.batch_interval = BatchInterval{batch_interval.get<BatchInterval::rep>()};
  } else {
    info.batch_interval = {};
  }

  if (const auto batch_size = data.at(kBatchSizeKey); !batch_size.is_null()) {
    info.batch_size = batch_size.get<decltype(info.batch_size)::value_type>();
  } else {
    info.batch_size = {};
  }

  data.at(kIsRunningKey).get_to(status.is_running);
  data.at(kTransformationName).get_to(status.info.transformation_name);

  if (const auto &owner = data.at(kOwner); !owner.is_null()) {
    info.owner = owner.get<decltype(info.owner)::value_type>();
  } else {
    info.owner = {};
  }

  data.at(kBoostrapServers).get_to(info.bootstrap_servers);
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
    const auto get_failed_message = [&stream_name = stream_name](const std::string_view message,
                                                                 const std::string_view nested_message) {
      return fmt::format("Failed to load stream '{}', because: {} caused by {}", stream_name, message, nested_message);
    };

    StreamStatus status;
    try {
      nlohmann::json::parse(stream_data).get_to(status);
    } catch (const nlohmann::json::type_error &exception) {
      spdlog::warn(get_failed_message("invalid type conversion", exception.what()));
      continue;
    } catch (const nlohmann::json::out_of_range &exception) {
      spdlog::warn(get_failed_message("non existing field", exception.what()));
      continue;
    }
    MG_ASSERT(status.name == stream_name, "Expected stream name is '{}', but got '{}'", status.name, stream_name);

    try {
      auto it = CreateConsumer(*locked_streams_map, stream_name, std::move(status.info));
      if (status.is_running) {
        it->second.consumer->Lock()->Start();
      }
      spdlog::info("Stream '{}' is loaded", stream_name);
    } catch (const utils::BasicException &exception) {
      spdlog::warn(get_failed_message("unexpected error", exception.what()));
    }
  }
}

void Streams::Create(const std::string &stream_name, StreamInfo info) {
  auto locked_streams = streams_.Lock();
  auto it = CreateConsumer(*locked_streams, stream_name, std::move(info));

  try {
    Persist(
        CreateStatus(stream_name, it->second.transformation_name, it->second.owner, *it->second.consumer->ReadLock()));
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

  Persist(CreateStatus(stream_name, it->second.transformation_name, it->second.owner, *locked_consumer));
}

void Streams::Stop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  auto locked_consumer = it->second.consumer->Lock();
  locked_consumer->Stop();

  Persist(CreateStatus(stream_name, it->second.transformation_name, it->second.owner, *locked_consumer));
}

void Streams::StartAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    auto locked_consumer = stream_data.consumer->Lock();
    if (!locked_consumer->IsRunning()) {
      locked_consumer->Start();
      Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *locked_consumer));
    }
  }
}

void Streams::StopAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    auto locked_consumer = stream_data.consumer->Lock();
    if (locked_consumer->IsRunning()) {
      locked_consumer->Stop();
      Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *locked_consumer));
    }
  }
}

std::vector<StreamStatus> Streams::GetStreamInfo() const {
  std::vector<StreamStatus> result;
  {
    for (auto locked_streams = streams_.ReadLock(); const auto &[stream_name, stream_data] : *locked_streams) {
      result.emplace_back(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner,
                                       *stream_data.consumer->ReadLock()));
    }
  }
  return result;
}

TransformationResult Streams::Check(const std::string &stream_name, std::optional<std::chrono::milliseconds> timeout,
                                    std::optional<int64_t> batch_limit) const {
  // This depends on the fact that Drop will first acquire a write lock to the consumer, and erase it only after that
  auto [locked_consumer,
        transformation_name] = [this, &stream_name]() -> std::pair<SynchronizedConsumer::ReadLockedPtr, std::string> {
    auto locked_streams = streams_.ReadLock();
    auto it = GetStream(*locked_streams, stream_name);
    return {it->second.consumer->ReadLock(), it->second.transformation_name};
  }();

  auto *memory_resource = utils::NewDeleteResource();
  mgp_result result{nullptr, memory_resource};
  TransformationResult test_result;

  auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, &stream_name,
                            &transformation_name = transformation_name, &result,
                            &test_result](const std::vector<Message> &messages) mutable {
    auto accessor = interpreter_context->db->Access();
    CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource, stream_name);

    for (auto &row : result.rows) {
      auto [query, parameters] = ExtractTransformationResult(std::move(row.values), transformation_name, stream_name);
      std::vector<TypedValue> result_row;
      result_row.reserve(kExpectedTransformationResultSize);
      result_row.push_back(std::move(query));
      result_row.push_back(std::move(parameters));

      test_result.push_back(std::move(result_row));
    }
  };

  locked_consumer->Check(timeout, batch_limit, consumer_function);

  return test_result;
}

StreamStatus Streams::CreateStatus(const std::string &name, const std::string &transformation_name,
                                   const std::optional<std::string> &owner,
                                   const integrations::kafka::Consumer &consumer) {
  const auto &info = consumer.Info();
  return StreamStatus{name,
                      StreamInfo{
                          info.topics,
                          info.consumer_group,
                          info.batch_interval,
                          info.batch_size,
                          transformation_name,
                          owner,
                      },
                      consumer.IsRunning()};
}

Streams::StreamsMap::iterator Streams::CreateConsumer(StreamsMap &map, const std::string &stream_name,
                                                      StreamInfo stream_info) {
  if (map.contains(stream_name)) {
    throw StreamsException{"Stream already exists with name '{}'", stream_name};
  }

  auto *memory_resource = utils::NewDeleteResource();

  auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, stream_name,
                            transformation_name = stream_info.transformation_name, owner = stream_info.owner,
                            interpreter = std::make_shared<Interpreter>(interpreter_context_),
                            result = mgp_result{nullptr, memory_resource}](
                               const std::vector<integrations::kafka::Message> &messages) mutable {
    auto accessor = interpreter_context->db->Access();
    EventCounter::IncrementCounter(EventCounter::MessagesConsumed, messages.size());
    CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource, stream_name);

    DiscardValueResultStream stream;

    spdlog::trace("Start transaction in stream '{}'", stream_name);
    utils::OnScopeExit cleanup{[&interpreter, &result]() {
      result.rows.clear();
      interpreter->Abort();
    }};
    interpreter->BeginTransaction();

    for (auto &row : result.rows) {
      spdlog::trace("Processing row in stream '{}'", stream_name);
      auto [query_value, params_value] =
          ExtractTransformationResult(std::move(row.values), transformation_name, stream_name);
      storage::PropertyValue params_prop{params_value};

      std::string query{query_value.ValueString()};
      spdlog::trace("Executing query '{}' in stream '{}'", query, stream_name);
      auto prepare_result =
          interpreter->Prepare(query, params_prop.IsNull() ? empty_parameters : params_prop.ValueMap(), nullptr);
      if (!interpreter_context->auth_checker->IsUserAuthorized(owner, prepare_result.privileges)) {
        throw StreamsException{
            "Couldn't execute query '{}' for stream '{}' becuase the owner is not authorized to execute the "
            "query!",
            query, stream_name};
      }
      interpreter->PullAll(&stream);
    }

    spdlog::trace("Commit transaction in stream '{}'", stream_name);
    interpreter->CommitTransaction();
    result.rows.clear();
  };

  ConsumerInfo consumer_info{
      .consumer_name = stream_name,
      .topics = std::move(stream_info.topics),
      .consumer_group = std::move(stream_info.consumer_group),
      .batch_interval = stream_info.batch_interval,
      .batch_size = stream_info.batch_size,
  };

  auto bootstrap_servers = stream_info.bootstrap_servers.empty() ? bootstrap_servers_ : stream_info.bootstrap_servers;
  auto insert_result = map.insert_or_assign(
      stream_name,
      StreamData{std::move(stream_info.transformation_name), std::move(stream_info.owner),
                 std::make_unique<SynchronizedConsumer>(std::move(bootstrap_servers), std::move(consumer_info),
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
