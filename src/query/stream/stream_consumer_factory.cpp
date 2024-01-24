// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/stream/stream_consumer_factory.hpp"

#include <memory>
#include "query/discard_value_stream.hpp"
#include "query/interpreter.hpp"
#include "query/procedure/module.hpp"

namespace memgraph::metrics {
extern const Event MessagesConsumed;
}  // namespace memgraph::metrics

using memgraph::storage::Storage;
using memgraph::storage::View;
using memgraph::utils::MemoryResource;
using memgraph::utils::NewDeleteResource;

namespace memgraph::query::stream {

namespace {

inline constexpr auto kExpectedTransformationResultSize = 2;
inline constexpr auto kCheckStreamResultSize = 2;

constexpr std::string_view query_param_name{"query"};
constexpr std::string_view params_param_name{"parameters"};

std::pair<TypedValue /*query*/, TypedValue /*parameters*/> ExtractTransformationResult(
    const utils::pmr::map<utils::pmr::string, TypedValue> &values, std::string_view transformation_name,
    std::string_view stream_name) {
  if (values.size() != kExpectedTransformationResultSize) {
    throw StreamsException(
        "Transformation '{}' in stream '{}' did not yield all fields (query, parameters) as required.",
        transformation_name, stream_name);
  }

  auto get_value = [&](std::string_view field_name) mutable -> const TypedValue & {
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
void CallCustomTransformation(std::string_view transformation_name, const std::vector<TMessage> &messages,
                              mgp_result &result, Storage::Accessor &storage_accessor, MemoryResource &memory_resource,
                              std::string_view stream_name) {
  DbAccessor db_accessor{&storage_accessor};
  {
    auto maybe_transformation =
        procedure::FindTransformation(procedure::gModuleRegistry, transformation_name, NewDeleteResource());

    if (!maybe_transformation) {
      throw StreamsException("Couldn't find transformation {} for stream '{}'", transformation_name, stream_name);
    };
    const auto &trans = *maybe_transformation->second;
    mgp_messages mgp_messages{mgp_messages::storage_type{&memory_resource}};
    std::transform(messages.begin(), messages.end(), std::back_inserter(mgp_messages.messages),
                   [](const TMessage &message) { return mgp_message{message}; });
    mgp_graph graph{&db_accessor, View::OLD, nullptr, db_accessor.GetStorageMode()};
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
}  // namespace

template <typename TStream>
auto make_consumer_impl(memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                        const std::string &transformation_name, std::optional<std::string> owner,
                        memgraph::query::InterpreterContext *interpreter_context)
    -> IStreamConsumerFactory::Consumer<TStream> {
  auto interpreter = std::make_shared<memgraph::query::Interpreter>(interpreter_context, std::move(db_acc));

  auto *memory_resource = memgraph::utils::NewDeleteResource();
  // there is an advantage of with the result being reused (retains internal buffers)
  auto result = mgp_result{nullptr, memory_resource};

  return [result = std::move(result), memory_resource, interpreter_context, interpreter = std::move(interpreter),
          stream_name, transformation_name,
          owner = std::move(owner)](const std::vector<typename TStream::Message> &messages) mutable {
#ifdef MG_ENTERPRISE
    interpreter->OnChangeCB([](auto) { return false; });  // Disable database change
#endif
    auto accessor = interpreter->current_db_.db_acc_->get()->Access();
    // register new interpreter into interpreter_context
    interpreter_context->interpreters->insert(interpreter.get());
    memgraph::utils::OnScopeExit interpreter_cleanup{
        [interpreter_context, interpreter]() { interpreter_context->interpreters->erase(interpreter.get()); }};

    memgraph::metrics::IncrementCounter(memgraph::metrics::MessagesConsumed, messages.size());

    CallCustomTransformation(transformation_name, messages, result, *accessor, *memory_resource, stream_name);

    memgraph::query::DiscardValueResultStream stream;

    spdlog::trace("Start transaction in stream '{}'", stream_name);
    memgraph::utils::OnScopeExit cleanup{[&interpreter, &result]() {
      result.rows.clear();
      interpreter->Abort();
    }};

    uint32_t i = 0;
    auto const total_retries = interpreter_context->config.stream_transaction_conflict_retries;
    const auto retry_interval = interpreter_context->config.stream_transaction_retry_interval;
    while (true) {
      try {
        interpreter->BeginTransaction();
        for (auto &row : result.rows) {
          spdlog::trace("Processing row in stream '{}'", stream_name);
          auto [query_value, params_value] = ExtractTransformationResult(row.values, transformation_name, stream_name);
          memgraph::storage::PropertyValue params_prop{params_value};

          std::string query{query_value.ValueString()};
          spdlog::trace("Executing query '{}' in stream '{}'", query, stream_name);
          auto prepare_result = interpreter->Prepare(
              query,
              params_prop.IsNull() ? std::map<std::string, memgraph::storage::PropertyValue>{} : params_prop.ValueMap(),
              {});
          if (!interpreter_context->auth_checker->IsUserAuthorized(owner, prepare_result.privileges, "")) {
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
      } catch (const memgraph::query::TransactionSerializationException &e) {
        interpreter->Abort();

        if (i == total_retries) {
          throw;
        }
        ++i;

        std::this_thread::sleep_for(retry_interval);
      }
    }
  };
};

template <typename TStream>
auto make_check_consumer_impl(memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                              const std::string &transformation_name, TransformationResult &test_result)
    -> IStreamConsumerFactory::Consumer<TStream> {
  auto *memory_resource = memgraph::utils::NewDeleteResource();
  return [db_acc = std::move(db_acc), memory_resource, stream_name, transformation_name,
          result = mgp_result{nullptr, memory_resource},
          &test_result](const std::vector<typename TStream::Message> &messages) mutable {
    auto accessor = db_acc->Access();
    CallCustomTransformation(transformation_name, messages, result, *accessor, *memory_resource, stream_name);

    auto result_row = std::vector<TypedValue>();
    result_row.reserve(kCheckStreamResultSize);

    auto queries_and_parameters = std::vector<TypedValue>(result.rows.size());
    std::transform(result.rows.cbegin(), result.rows.cend(), queries_and_parameters.begin(), [&](const auto &row) {
      auto [query, parameters] = ExtractTransformationResult(row.values, transformation_name, stream_name);

      return std::map<std::string, TypedValue>{{"query", std::move(query)}, {"parameters", std::move(parameters)}};
    });
    result_row.emplace_back(std::move(queries_and_parameters));

    auto messages_list = std::vector<TypedValue>(messages.size());
    std::transform(messages.cbegin(), messages.cend(), messages_list.begin(), [](const auto &message) {
      return std::string_view(message.Payload().data(), message.Payload().size());
    });

    result_row.emplace_back(std::move(messages_list));

    test_result.emplace_back(std::move(result_row));
  };
};

auto StreamConsumerFactory::make_consumer(memgraph::utils::Tag<KafkaStream> tag, memgraph::dbms::DatabaseAccess db_acc,
                                          const std::string &stream_name, const std::string &transformation_name,
                                          std::optional<std::string> owner) -> Consumer<KafkaStream> {
  return make_consumer_impl<KafkaStream>(std::move(db_acc), stream_name, transformation_name, std::move(owner),
                                         interpreter_context);
}
auto StreamConsumerFactory::make_consumer(memgraph::utils::Tag<PulsarStream> tag, memgraph::dbms::DatabaseAccess db_acc,
                                          const std::string &stream_name, const std::string &transformation_name,
                                          std::optional<std::string> owner) -> Consumer<PulsarStream> {
  return make_consumer_impl<PulsarStream>(std::move(db_acc), stream_name, transformation_name, std::move(owner),
                                          interpreter_context);
}
auto StreamConsumerFactory::make_check_consumer(memgraph::utils::Tag<KafkaStream> tag,
                                                memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                                                const std::string &transformation_name,
                                                TransformationResult &test_result) -> Consumer<KafkaStream> {
  return make_check_consumer_impl<KafkaStream>(std::move(db_acc), stream_name, transformation_name, test_result);
}
auto StreamConsumerFactory::make_check_consumer(memgraph::utils::Tag<PulsarStream> tag,
                                                memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                                                const std::string &transformation_name,
                                                TransformationResult &test_result) -> Consumer<PulsarStream> {
  return make_check_consumer_impl<PulsarStream>(std::move(db_acc), stream_name, transformation_name, test_result);
}

}  // namespace memgraph::query::stream
