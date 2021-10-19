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
const std::map<std::string, storage::PropertyValue> empty_parameters{};

auto GetStream(auto &map, const std::string &stream_name) {
  if (auto it = map.find(stream_name); it != map.end()) {
    return it;
  }
  throw StreamsException("Couldn't find stream '{}'", stream_name);
}

std::pair<TypedValue /*query*/, TypedValue /*parameters*/> ExtractTransformationResult(
    utils::pmr::map<utils::pmr::string, TypedValue> &&values, const std::string_view transformation_name,
    const std::string_view stream_name) {
  if (values.size() != details::kExpectedTransformationResultSize) {
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

  auto &query_value = get_value(details::query_param_name);
  MG_ASSERT(query_value.IsString());
  auto &params_value = get_value(details::params_param_name);
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

template <typename T>
void to_json(nlohmann::json &data, StreamStatus<T> &&status) {
  auto &info = status.info;
  data[kStreamName] = std::move(status.name);
  data["type"] = std::move(status.type);
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

template <typename T>
void from_json(const nlohmann::json &data, StreamStatus<T> &status) {
  auto &info = status.info;
  data.at(kStreamName).get_to(status.name);
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kConsumerGroupKey).get_to(info.consumer_group);

  if (const auto batch_interval = data.at(kBatchIntervalKey); !batch_interval.is_null()) {
    using BatchInterval = typename decltype(info.batch_interval)::value_type;
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

  info.owner = data.value(kBoostrapServers, "");
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

    const auto create_consumer = [&, &stream_name = stream_name, this]<typename T>(StreamStatus<T> status,
                                                                                   auto &&stream_json_data) {
      try {
        nlohmann::json::parse(stream_json_data).get_to(status);
      } catch (const nlohmann::json::type_error &exception) {
        spdlog::warn(get_failed_message("invalid type conversion", exception.what()));
        return;
      } catch (const nlohmann::json::out_of_range &exception) {
        spdlog::warn(get_failed_message("non existing field", exception.what()));
        return;
      }
      MG_ASSERT(status.name == stream_name, "Expected stream name is '{}', but got '{}'", status.name, stream_name);

      try {
        auto it = CreateConsumer<T>(*locked_streams_map, stream_name, std::move(status.info), {});
        if (status.is_running) {
          std::visit(
              [&](auto &&stream_data) {
                auto stream_source_ptr = stream_data.stream_source.Lock();
                stream_source_ptr->Start();
              },
              it->second);
        }
        spdlog::info("Stream '{}' is loaded", stream_name);
      } catch (const utils::BasicException &exception) {
        spdlog::warn(get_failed_message("unexpected error", exception.what()));
      }
    };

    auto stream_json_data = nlohmann::json::parse(stream_data);
    const auto stream_type = static_cast<StreamSourceType>(stream_json_data.at("type"));

    switch (stream_type) {
      case StreamSourceType::KAFKA:
        create_consumer(StreamStatus<KafkaStream>{}, std::move(stream_json_data));
        break;
    }
  }
}

void Streams::Drop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();

  auto it = GetStream(*locked_streams, stream_name);

  // streams_ is write locked, which means there is no access to it outside of this function, thus only the Test
  // function can be executing with the consumer, nothing else.
  // By acquiring the write lock here for the consumer, we make sure there is
  // no running Test function for this consumer, therefore it can be erased.
  std::visit([&](auto &&stream_data) { stream_data.stream_source.Lock(); }, it->second);

  locked_streams->erase(it);
  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

namespace {

template <typename T>
StreamStatus<T> CreateStatus(std::string stream_name, std::string transformation_name, std::optional<std::string> owner,
                             const T &stream) {
  return {.name = std::move(stream_name),
          .type = StreamType(stream),
          .is_running = stream.IsRunning(),
          .info = stream.Info(std::move(transformation_name), std::move(owner))};
}

}  // namespace

void Streams::Start(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  std::visit(
      [&, this](auto &&stream_data) {
        auto stream_source_ptr = stream_data.stream_source.Lock();
        stream_source_ptr->Start();
        Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
      },
      it->second);
}

void Streams::Stop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  std::visit(
      [&, this](auto &&stream_data) {
        auto stream_source_ptr = stream_data.stream_source.Lock();
        stream_source_ptr->Stop();

        Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
      },
      it->second);
}

void Streams::StartAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    std::visit(
        [&stream_name = stream_name, this](auto &&stream_data) {
          auto locked_stream_source = stream_data.stream_source.Lock();
          if (!locked_stream_source->IsRunning()) {
            locked_stream_source->Start();
            Persist(
                CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *locked_stream_source));
          }
        },
        stream_data);
  }
}

void Streams::StopAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    std::visit(
        [&stream_name = stream_name, this](auto &&stream_data) {
          auto locked_stream_source = stream_data.stream_source.Lock();
          if (locked_stream_source->IsRunning()) {
            locked_stream_source->Stop();
            Persist(
                CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *locked_stream_source));
          }
        },
        stream_data);
  }
}

std::vector<StreamStatus<>> Streams::GetStreamInfo() const {
  std::vector<StreamStatus<>> result;
  {
    for (auto locked_streams = streams_.ReadLock(); const auto &[stream_name, stream_data] : *locked_streams) {
      std::visit(
          [&, &stream_name = stream_name](auto &&stream_data) {
            auto locked_stream_source = stream_data.stream_source.ReadLock();
            auto info = locked_stream_source->Info(stream_data.transformation_name, stream_data.owner);
            result.emplace_back(StreamStatus<>{stream_name, StreamType(*locked_stream_source),
                                               locked_stream_source->IsRunning(), std::move(info.common_info)});
          },
          stream_data);
    }
  }
  return result;
}

TransformationResult Streams::Check(const std::string &stream_name, std::optional<std::chrono::milliseconds> timeout,
                                    std::optional<int64_t> batch_limit) const {
  std::optional locked_streams{streams_.ReadLock()};
  auto it = GetStream(**locked_streams, stream_name);

  return std::visit(
      [&](auto &&stream_data) {
        // This depends on the fact that Drop will first acquire a write lock to the consumer, and erase it only after
        // that
        const auto locked_stream_source = stream_data.stream_source.ReadLock();
        const auto transformation_name = stream_data.transformation_name;
        locked_streams.reset();

        auto *memory_resource = utils::NewDeleteResource();
        mgp_result result{nullptr, memory_resource};
        TransformationResult test_result;

        auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, &stream_name,
                                  &transformation_name = transformation_name, &result,
                                  &test_result](const std::vector<Message> &messages) mutable {
          auto accessor = interpreter_context->db->Access();
          details::CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource,
                                            stream_name);

          for (auto &row : result.rows) {
            auto [query, parameters] =
                ExtractTransformationResult(std::move(row.values), transformation_name, stream_name);
            std::vector<TypedValue> result_row;
            result_row.reserve(details::kExpectedTransformationResultSize);
            result_row.push_back(std::move(query));
            result_row.push_back(std::move(parameters));

            test_result.push_back(std::move(result_row));
          }
        };

        locked_stream_source->Check(timeout, batch_limit, consumer_function);
        return test_result;
      },
      it->second);
}

std::string_view Streams::BootstrapServers() const { return bootstrap_servers_; }
}  // namespace query
