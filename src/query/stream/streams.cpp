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

#include "query/stream/streams.hpp"

#include <shared_mutex>
#include <string_view>
#include <utility>

#include <spdlog/spdlog.h>
#include <json/json.hpp>

#include "query/db_accessor.hpp"
#include "query/discard_value_stream.hpp"
#include "query/exceptions.hpp"
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

template <typename TMessage>
void CallCustomTransformation(const std::string &transformation_name, const std::vector<TMessage> &messages,
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
                   [](const TMessage &message) { return mgp_message{message}; });
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

template <Stream TStream>
StreamStatus<TStream> CreateStatus(std::string stream_name, std::string transformation_name,
                                   std::optional<std::string> owner, const TStream &stream) {
  return {.name = std::move(stream_name),
          .type = StreamType(stream),
          .is_running = stream.IsRunning(),
          .info = stream.Info(std::move(transformation_name)),
          .owner = std::move(owner)};
}

// nlohmann::json doesn't support string_view access yet
const std::string kStreamName{"name"};
const std::string kIsRunningKey{"is_running"};
const std::string kOwner{"owner"};
const std::string kType{"type"};
}  // namespace

template <Stream TStream>
void to_json(nlohmann::json &data, StreamStatus<TStream> &&status) {
  data[kStreamName] = std::move(status.name);
  data[kType] = status.type;
  data[kIsRunningKey] = status.is_running;

  if (status.owner.has_value()) {
    data[kOwner] = std::move(*status.owner);
  } else {
    data[kOwner] = nullptr;
  }

  to_json(data, std::move(status.info));
}

template <Stream TStream>
void from_json(const nlohmann::json &data, StreamStatus<TStream> &status) {
  data.at(kStreamName).get_to(status.name);
  data.at(kIsRunningKey).get_to(status.is_running);

  if (const auto &owner = data.at(kOwner); !owner.is_null()) {
    status.owner = owner.get<typename decltype(status.owner)::value_type>();
  } else {
    status.owner = {};
  }

  from_json(data, status.info);
}

Streams::Streams(InterpreterContext *interpreter_context, std::filesystem::path directory)
    : interpreter_context_(interpreter_context), storage_(std::move(directory)) {}

template <Stream TStream>
void Streams::Create(const std::string &stream_name, typename TStream::StreamInfo info,
                     std::optional<std::string> owner) {
  auto locked_streams = streams_.Lock();
  auto it = CreateConsumer<TStream>(*locked_streams, stream_name, std::move(info), std::move(owner));

  try {
    std::visit(
        [&](auto &&stream_data) {
          const auto stream_source_ptr = stream_data.stream_source->ReadLock();
          Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
        },
        it->second);
  } catch (...) {
    locked_streams->erase(it);
    throw;
  }
}

template void Streams::Create<KafkaStream>(const std::string &stream_name, KafkaStream::StreamInfo info,
                                           std::optional<std::string> owner);
template void Streams::Create<PulsarStream>(const std::string &stream_name, PulsarStream::StreamInfo info,
                                            std::optional<std::string> owner);

template <Stream TStream>
Streams::StreamsMap::iterator Streams::CreateConsumer(StreamsMap &map, const std::string &stream_name,
                                                      typename TStream::StreamInfo stream_info,
                                                      std::optional<std::string> owner) {
  if (map.contains(stream_name)) {
    throw StreamsException{"Stream already exists with name '{}'", stream_name};
  }

  auto *memory_resource = utils::NewDeleteResource();

  auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, stream_name,
                            transformation_name = stream_info.common_info.transformation_name, owner = owner,
                            interpreter = std::make_shared<Interpreter>(interpreter_context_),
                            result = mgp_result{nullptr, memory_resource},
                            total_retries = interpreter_context_->config.transaction_conflict_retries](
                               const std::vector<typename TStream::Message> &messages) mutable {
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

    const static std::map<std::string, storage::PropertyValue> empty_parameters{};

    for (uint32_t i = 1; i != total_retries; ++i) {
      try {
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
                "Couldn't execute query '{}' for stream '{}' because the owner is not authorized to execute the "
                "query!",
                query, stream_name};
          }
          interpreter->PullAll(&stream);
        }

        spdlog::trace("Commit transaction in stream '{}'", stream_name);
        interpreter->CommitTransaction();
        result.rows.clear();
      } catch (const query::TransactionSerializationException &e) {
        if (i == total_retries) {
          throw;
        }
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(500ms);
      }
    }
  };
  auto insert_result = map.try_emplace(
      stream_name, StreamData<TStream>{std::move(stream_info.common_info.transformation_name), std::move(owner),
                                       std::make_unique<SynchronizedStreamSource<TStream>>(
                                           stream_name, std::move(stream_info), std::move(consumer_function))});
  MG_ASSERT(insert_result.second, "Unexpected error during storing consumer '{}'", stream_name);
  return insert_result.first;
}

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
        stream_json_data.get_to(status);
      } catch (const nlohmann::json::type_error &exception) {
        spdlog::warn(get_failed_message("invalid type conversion", exception.what()));
        return;
      } catch (const nlohmann::json::out_of_range &exception) {
        spdlog::warn(get_failed_message("non existing field", exception.what()));
        return;
      }
      MG_ASSERT(status.name == stream_name, "Expected stream name is '{}', but got '{}'", status.name, stream_name);

      try {
        auto it = CreateConsumer<T>(*locked_streams_map, stream_name, std::move(status.info), std::move(status.owner));
        if (status.is_running) {
          std::visit(
              [&](auto &&stream_data) {
                auto stream_source_ptr = stream_data.stream_source->Lock();
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
      case StreamSourceType::PULSAR:
        create_consumer(StreamStatus<PulsarStream>{}, std::move(stream_json_data));
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
  std::visit([&](auto &&stream_data) { stream_data.stream_source->Lock(); }, it->second);

  locked_streams->erase(it);
  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  std::visit(
      [&, this](auto &&stream_data) {
        auto stream_source_ptr = stream_data.stream_source->Lock();
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
        auto stream_source_ptr = stream_data.stream_source->Lock();
        stream_source_ptr->Stop();

        Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
      },
      it->second);
}

void Streams::StartAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    std::visit(
        [&stream_name = stream_name, this](auto &&stream_data) {
          auto locked_stream_source = stream_data.stream_source->Lock();
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
          auto locked_stream_source = stream_data.stream_source->Lock();
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
            auto locked_stream_source = stream_data.stream_source->ReadLock();
            auto info = locked_stream_source->Info(stream_data.transformation_name);
            result.emplace_back(StreamStatus<>{stream_name, StreamType(*locked_stream_source),
                                               locked_stream_source->IsRunning(), std::move(info.common_info),
                                               stream_data.owner});
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
        const auto locked_stream_source = stream_data.stream_source->ReadLock();
        const auto transformation_name = stream_data.transformation_name;
        locked_streams.reset();

        auto *memory_resource = utils::NewDeleteResource();
        mgp_result result{nullptr, memory_resource};
        TransformationResult test_result;

        auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, &stream_name,
                                  &transformation_name = transformation_name, &result,
                                  &test_result]<typename T>(const std::vector<T> &messages) mutable {
          auto accessor = interpreter_context->db->Access();
          CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource, stream_name);

          for (auto &row : result.rows) {
            auto [query, parameters] =
                ExtractTransformationResult(std::move(row.values), transformation_name, stream_name);
            std::vector<TypedValue> result_row;
            result_row.reserve(kExpectedTransformationResultSize);
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

}  // namespace query
