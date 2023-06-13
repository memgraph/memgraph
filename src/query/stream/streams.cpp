// Copyright 2023 Memgraph Ltd.
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

#include "integrations/constants.hpp"
#include "mg_procedure.h"
#include "query/db_accessor.hpp"
#include "query/discard_value_stream.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/procedure/mg_procedure_helpers.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/stream/sources.hpp"
#include "query/typed_value.hpp"
#include "utils/event_counter.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/pmr/string.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::metrics {
extern const Event MessagesConsumed;
}  // namespace memgraph::metrics

namespace memgraph::query::stream {
namespace {
inline constexpr auto kExpectedTransformationResultSize = 2;
inline constexpr auto kCheckStreamResultSize = 2;
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
    const utils::pmr::map<utils::pmr::string, TypedValue> &values, const std::string_view transformation_name,
    const std::string_view stream_name) {
  if (values.size() != kExpectedTransformationResultSize) {
    throw StreamsException(
        "Transformation '{}' in stream '{}' did not yield all fields (query, parameters) as required.",
        transformation_name, stream_name);
  }

  auto get_value = [&](const utils::pmr::string &field_name) mutable -> const TypedValue & {
    auto it = values.find(field_name);
    if (it == values.end()) {
      throw StreamsException{"Transformation '{}' in stream '{}' did not yield a record with '{}' field.",
                             transformation_name, stream_name, field_name};
    };
    return it->second;
  };

  const auto &query_value = get_value(query_param_name);
  MG_ASSERT(query_value.IsString());
  const auto &params_value = get_value(params_param_name);
  MG_ASSERT(params_value.IsNull() || params_value.IsMap());
  return {query_value, params_value};
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
    : interpreter_context_(interpreter_context), storage_(std::move(directory)) {
  RegisterProcedures();
}

void Streams::RegisterProcedures() {
  RegisterKafkaProcedures();
  RegisterPulsarProcedures();
}

void Streams::RegisterKafkaProcedures() {
  {
    static constexpr std::string_view proc_name = "kafka_set_stream_offset";
    auto set_stream_offset = [this](mgp_list *args, mgp_graph * /*graph*/, mgp_result *result,
                                    mgp_memory * /*memory*/) {
      auto *arg_stream_name = procedure::Call<mgp_value *>(mgp_list_at, args, 0);
      const auto *stream_name = procedure::Call<const char *>(mgp_value_get_string, arg_stream_name);
      auto *arg_offset = procedure::Call<mgp_value *>(mgp_list_at, args, 1);
      const auto offset = procedure::Call<int64_t>(mgp_value_get_int, arg_offset);
      auto lock_ptr = streams_.Lock();
      auto it = GetStream(*lock_ptr, std::string(stream_name));
      std::visit(utils::Overloaded{[&](StreamData<KafkaStream> &kafka_stream) {
                                     auto stream_source_ptr = kafka_stream.stream_source->Lock();
                                     const auto error = stream_source_ptr->SetStreamOffset(offset);
                                     if (error.HasError()) {
                                       MG_ASSERT(mgp_result_set_error_msg(result, error.GetError().c_str()) ==
                                                     mgp_error::MGP_ERROR_NO_ERROR,
                                                 "Unable to set procedure error message of procedure: {}", proc_name);
                                     }
                                   },
                                   [](auto && /*other*/) {
                                     throw QueryRuntimeException("'{}' can be only used for Kafka stream sources",
                                                                 proc_name);
                                   }},
                 it->second);
    };

    mgp_proc proc(proc_name, set_stream_offset, utils::NewDeleteResource());
    MG_ASSERT(mgp_proc_add_arg(&proc, "stream_name", procedure::Call<mgp_type *>(mgp_type_string)) ==
              mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_arg(&proc, "offset", procedure::Call<mgp_type *>(mgp_type_int)) ==
              mgp_error::MGP_ERROR_NO_ERROR);

    procedure::gModuleRegistry.RegisterMgProcedure(proc_name, std::move(proc));
  }

  {
    static constexpr std::string_view proc_name = "kafka_stream_info";

    static constexpr std::string_view consumer_group_result_name = "consumer_group";
    static constexpr std::string_view topics_result_name = "topics";
    static constexpr std::string_view bootstrap_servers_result_name = "bootstrap_servers";
    static constexpr std::string_view configs_result_name = "configs";
    static constexpr std::string_view credentials_result_name = "credentials";

    auto get_stream_info = [this](mgp_list *args, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
      auto *arg_stream_name = procedure::Call<mgp_value *>(mgp_list_at, args, 0);
      const auto *stream_name = procedure::Call<const char *>(mgp_value_get_string, arg_stream_name);
      auto lock_ptr = streams_.Lock();
      auto it = GetStream(*lock_ptr, std::string(stream_name));
      std::visit(
          utils::Overloaded{
              [&](StreamData<KafkaStream> &kafka_stream) {
                auto stream_source_ptr = kafka_stream.stream_source->Lock();
                const auto info = stream_source_ptr->Info(kafka_stream.transformation_name);
                mgp_result_record *record{nullptr};
                if (!procedure::TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
                  return;
                }

                const auto consumer_group_value =
                    procedure::GetStringValueOrSetError(info.consumer_group.c_str(), memory, result);
                if (!consumer_group_value) {
                  return;
                }

                procedure::MgpUniquePtr<mgp_list> topic_names{nullptr, mgp_list_destroy};
                if (!procedure::TryOrSetError(
                        [&] {
                          return procedure::CreateMgpObject(topic_names, mgp_list_make_empty, info.topics.size(),
                                                            memory);
                        },
                        result)) {
                  return;
                }

                for (const auto &topic : info.topics) {
                  auto topic_value = procedure::GetStringValueOrSetError(topic.c_str(), memory, result);
                  if (!topic_value) {
                    return;
                  }
                  topic_names->elems.push_back(std::move(*topic_value));
                }

                procedure::MgpUniquePtr<mgp_value> topics_value{nullptr, mgp_value_destroy};
                if (!procedure::TryOrSetError(
                        [&] {
                          return procedure::CreateMgpObject(topics_value, mgp_value_make_list, topic_names.get());
                        },
                        result)) {
                  return;
                }
                static_cast<void>(topic_names.release());

                const auto bootstrap_servers_value =
                    procedure::GetStringValueOrSetError(info.bootstrap_servers.c_str(), memory, result);
                if (!bootstrap_servers_value) {
                  return;
                }

                const auto convert_config_map =
                    [result, memory](const std::unordered_map<std::string, std::string> &configs_to_convert)
                    -> procedure::MgpUniquePtr<mgp_value> {
                  procedure::MgpUniquePtr<mgp_value> configs_value{nullptr, mgp_value_destroy};
                  procedure::MgpUniquePtr<mgp_map> configs{nullptr, mgp_map_destroy};
                  if (!procedure::TryOrSetError(
                          [&] { return procedure::CreateMgpObject(configs, mgp_map_make_empty, memory); }, result)) {
                    return configs_value;
                  }

                  for (const auto &[key, value] : configs_to_convert) {
                    auto value_value = procedure::GetStringValueOrSetError(value.c_str(), memory, result);
                    if (!value_value) {
                      return configs_value;
                    }
                    configs->items.emplace(key, std::move(*value_value));
                  }

                  if (!procedure::TryOrSetError(
                          [&] { return procedure::CreateMgpObject(configs_value, mgp_value_make_map, configs.get()); },
                          result)) {
                    return configs_value;
                  }
                  static_cast<void>(configs.release());
                  return configs_value;
                };

                const auto configs_value = convert_config_map(info.configs);
                if (configs_value == nullptr) {
                  return;
                }

                using CredentialsType = decltype(KafkaStream::StreamInfo::credentials);
                CredentialsType reducted_credentials;
                std::transform(info.credentials.begin(), info.credentials.end(),
                               std::inserter(reducted_credentials, reducted_credentials.end()),
                               [](const auto &pair) -> CredentialsType::value_type {
                                 return {pair.first, integrations::kReducted};
                               });

                const auto credentials_value = convert_config_map(reducted_credentials);
                if (credentials_value == nullptr) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, consumer_group_result_name.data(),
                                                       consumer_group_value.get())) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, topics_result_name.data(), topics_value.get())) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, bootstrap_servers_result_name.data(),
                                                       bootstrap_servers_value.get())) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, configs_result_name.data(),
                                                       configs_value.get())) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, credentials_result_name.data(),
                                                       credentials_value.get())) {
                  return;
                }
              },
              [](auto && /*other*/) {
                throw QueryRuntimeException("'{}' can be only used for Kafka stream sources", proc_name);
              }},
          it->second);
    };

    mgp_proc proc(proc_name, get_stream_info, utils::NewDeleteResource());
    MG_ASSERT(mgp_proc_add_arg(&proc, "stream_name", procedure::Call<mgp_type *>(mgp_type_string)) ==
              mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_result(&proc, consumer_group_result_name.data(),
                                  procedure::Call<mgp_type *>(mgp_type_string)) == mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(
        mgp_proc_add_result(&proc, topics_result_name.data(),
                            procedure::Call<mgp_type *>(mgp_type_list, procedure::Call<mgp_type *>(mgp_type_string))) ==
        mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_result(&proc, bootstrap_servers_result_name.data(),
                                  procedure::Call<mgp_type *>(mgp_type_string)) == mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_result(&proc, configs_result_name.data(), procedure::Call<mgp_type *>(mgp_type_map)) ==
              mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_result(&proc, credentials_result_name.data(), procedure::Call<mgp_type *>(mgp_type_map)) ==
              mgp_error::MGP_ERROR_NO_ERROR);

    procedure::gModuleRegistry.RegisterMgProcedure(proc_name, std::move(proc));
  }
}

void Streams::RegisterPulsarProcedures() {
  {
    static constexpr std::string_view proc_name = "pulsar_stream_info";
    static constexpr std::string_view service_url_result_name = "service_url";
    static constexpr std::string_view topics_result_name = "topics";
    auto get_stream_info = [this](mgp_list *args, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
      auto *arg_stream_name = procedure::Call<mgp_value *>(mgp_list_at, args, 0);
      const auto *stream_name = procedure::Call<const char *>(mgp_value_get_string, arg_stream_name);
      auto lock_ptr = streams_.Lock();
      auto it = GetStream(*lock_ptr, std::string(stream_name));
      std::visit(
          utils::Overloaded{
              [&](StreamData<PulsarStream> &pulsar_stream) {
                auto stream_source_ptr = pulsar_stream.stream_source->Lock();
                const auto info = stream_source_ptr->Info(pulsar_stream.transformation_name);
                mgp_result_record *record{nullptr};
                if (!procedure::TryOrSetError([&] { return mgp_result_new_record(result, &record); }, result)) {
                  return;
                }

                auto service_url_value = procedure::GetStringValueOrSetError(info.service_url.c_str(), memory, result);
                if (!service_url_value) {
                  return;
                }

                procedure::MgpUniquePtr<mgp_list> topic_names{nullptr, mgp_list_destroy};
                if (!procedure::TryOrSetError(
                        [&] {
                          return procedure::CreateMgpObject(topic_names, mgp_list_make_empty, info.topics.size(),
                                                            memory);
                        },
                        result)) {
                  return;
                }

                for (const auto &topic : info.topics) {
                  auto topic_value = procedure::GetStringValueOrSetError(topic.c_str(), memory, result);
                  if (!topic_value) {
                    return;
                  }
                  topic_names->elems.push_back(std::move(*topic_value));
                }

                procedure::MgpUniquePtr<mgp_value> topics_value{nullptr, mgp_value_destroy};
                if (!procedure::TryOrSetError(
                        [&] {
                          return procedure::CreateMgpObject(topics_value, mgp_value_make_list, topic_names.release());
                        },
                        result)) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, topics_result_name.data(), topics_value.get())) {
                  return;
                }

                if (!procedure::InsertResultOrSetError(result, record, service_url_result_name.data(),
                                                       service_url_value.get())) {
                  return;
                }
              },
              [](auto && /*other*/) {
                throw QueryRuntimeException("'{}' can be only used for Pulsar stream sources", proc_name);
              }},
          it->second);
    };

    mgp_proc proc(proc_name, get_stream_info, utils::NewDeleteResource());
    MG_ASSERT(mgp_proc_add_arg(&proc, "stream_name", procedure::Call<mgp_type *>(mgp_type_string)) ==
              mgp_error::MGP_ERROR_NO_ERROR);
    MG_ASSERT(mgp_proc_add_result(&proc, service_url_result_name.data(),
                                  procedure::Call<mgp_type *>(mgp_type_string)) == mgp_error::MGP_ERROR_NO_ERROR);

    MG_ASSERT(
        mgp_proc_add_result(&proc, topics_result_name.data(),
                            procedure::Call<mgp_type *>(mgp_type_list, procedure::Call<mgp_type *>(mgp_type_string))) ==
        mgp_error::MGP_ERROR_NO_ERROR);

    procedure::gModuleRegistry.RegisterMgProcedure(proc_name, std::move(proc));
  }
}

template <Stream TStream>
void Streams::Create(const std::string &stream_name, typename TStream::StreamInfo info,
                     std::optional<std::string> owner) {
  auto locked_streams = streams_.Lock();
  auto it = CreateConsumer<TStream>(*locked_streams, stream_name, std::move(info), std::move(owner));

  try {
    std::visit(
        [&](const auto &stream_data) {
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
                            total_retries = interpreter_context_->config.stream_transaction_conflict_retries,
                            retry_interval = interpreter_context_->config.stream_transaction_retry_interval](
                               const std::vector<typename TStream::Message> &messages) mutable {
    auto accessor = interpreter_context->db->Access();
    // register new interpreter into interpreter_context_
    interpreter_context->interpreters->insert(interpreter.get());
    utils::OnScopeExit interpreter_cleanup{
        [interpreter_context, interpreter]() { interpreter_context->interpreters->erase(interpreter.get()); }};

    memgraph::metrics::IncrementCounter(memgraph::metrics::MessagesConsumed, messages.size());
    CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource, stream_name);

    DiscardValueResultStream stream;

    spdlog::trace("Start transaction in stream '{}'", stream_name);
    utils::OnScopeExit cleanup{[&interpreter, &result]() {
      result.rows.clear();
      interpreter->Abort();
    }};

    const static std::map<std::string, storage::PropertyValue> empty_parameters{};
    uint32_t i = 0;
    while (true) {
      try {
        interpreter->BeginTransaction();
        for (auto &row : result.rows) {
          spdlog::trace("Processing row in stream '{}'", stream_name);
          auto [query_value, params_value] = ExtractTransformationResult(row.values, transformation_name, stream_name);
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
        break;
      } catch (const query::TransactionSerializationException &e) {
        interpreter->Abort();
        if (i == total_retries) {
          throw;
        }
        ++i;
        std::this_thread::sleep_for(retry_interval);
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
        spdlog::warn(get_failed_message("nonexistent field", exception.what()));
        return;
      }
      MG_ASSERT(status.name == stream_name, "Expected stream name is '{}', but got '{}'", status.name, stream_name);

      try {
        auto it = CreateConsumer<T>(*locked_streams_map, stream_name, std::move(status.info), std::move(status.owner));
        if (status.is_running) {
          std::visit(
              [&](const auto &stream_data) {
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
    if (const auto it = stream_json_data.find(kType); it != stream_json_data.end()) {
      const auto stream_type = static_cast<StreamSourceType>(*it);
      switch (stream_type) {
        case StreamSourceType::KAFKA:
          create_consumer(StreamStatus<KafkaStream>{}, std::move(stream_json_data));
          break;
        case StreamSourceType::PULSAR:
          create_consumer(StreamStatus<PulsarStream>{}, std::move(stream_json_data));
          break;
      }
    } else {
      spdlog::warn(
          "Unable to load stream '{}', because it does not contain the type of the stream. Most probably the stream "
          "was saved before Memgraph 2.1. Please recreate the stream manually to make it work. For more information "
          "please check https://memgraph.com/docs/memgraph/changelog#v210---nov-22-2021 .",
          stream_json_data.value(kStreamName, "<invalid format>"));
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
  std::visit([&](const auto &stream_data) { stream_data.stream_source->Lock(); }, it->second);

  if (!storage_.Delete(stream_name)) {
    throw StreamsException("Couldn't delete stream '{}' from persistent store!", stream_name);
  }
  locked_streams->erase(it);

  // TODO(antaljanosbenjamin) Release the transformation
}

void Streams::Start(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  std::visit(
      [&, this](const auto &stream_data) {
        auto stream_source_ptr = stream_data.stream_source->Lock();
        stream_source_ptr->Start();
        Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
      },
      it->second);
}

void Streams::StartWithLimit(const std::string &stream_name, uint64_t batch_limit,
                             std::optional<std::chrono::milliseconds> timeout) const {
  std::optional locked_streams{streams_.ReadLock()};
  auto it = GetStream(**locked_streams, stream_name);

  std::visit(
      [&](const auto &stream_data) {
        const auto locked_stream_source = stream_data.stream_source->ReadLock();
        locked_streams.reset();

        locked_stream_source->StartWithLimit(batch_limit, timeout);
      },
      it->second);
}

void Streams::Stop(const std::string &stream_name) {
  auto locked_streams = streams_.Lock();
  auto it = GetStream(*locked_streams, stream_name);

  std::visit(
      [&, this](const auto &stream_data) {
        auto stream_source_ptr = stream_data.stream_source->Lock();
        stream_source_ptr->Stop();

        Persist(CreateStatus(stream_name, stream_data.transformation_name, stream_data.owner, *stream_source_ptr));
      },
      it->second);
}

void Streams::StartAll() {
  for (auto locked_streams = streams_.Lock(); auto &[stream_name, stream_data] : *locked_streams) {
    std::visit(
        [&stream_name = stream_name, this](const auto &stream_data) {
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
        [&stream_name = stream_name, this](const auto &stream_data) {
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
          [&, &stream_name = stream_name](const auto &stream_data) {
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
                                    std::optional<uint64_t> batch_limit) const {
  std::optional locked_streams{streams_.ReadLock()};
  auto it = GetStream(**locked_streams, stream_name);

  return std::visit(
      [&](const auto &stream_data) {
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

          auto result_row = std::vector<TypedValue>();
          result_row.reserve(kCheckStreamResultSize);

          auto queries_and_parameters = std::vector<TypedValue>(result.rows.size());
          std::transform(
              result.rows.cbegin(), result.rows.cend(), queries_and_parameters.begin(), [&](const auto &row) {
                auto [query, parameters] = ExtractTransformationResult(row.values, transformation_name, stream_name);

                return std::map<std::string, TypedValue>{{"query", std::move(query)},
                                                         {"parameters", std::move(parameters)}};
              });
          result_row.emplace_back(std::move(queries_and_parameters));

          auto messages_list = std::vector<TypedValue>(messages.size());
          std::transform(messages.cbegin(), messages.cend(), messages_list.begin(), [](const auto &message) {
            return std::string_view(message.Payload().data(), message.Payload().size());
          });

          result_row.emplace_back(std::move(messages_list));

          test_result.emplace_back(std::move(result_row));
        };

        locked_stream_source->Check(timeout, batch_limit, consumer_function);
        return test_result;
      },
      it->second);
}

}  // namespace memgraph::query::stream
