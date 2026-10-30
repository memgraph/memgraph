// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/structured_logging.hpp"

#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace memgraph::logging {
namespace {

struct EventContext {
  const spdlog::details::log_msg *message;
  const nlohmann::json *event;
};

thread_local const EventContext *current_event = nullptr;

class EventScope {
 public:
  explicit EventScope(const EventContext &context) : previous_(std::exchange(current_event, &context)) {}

  ~EventScope() { current_event = previous_; }

  EventScope(const EventScope &) = delete;
  EventScope &operator=(const EventScope &) = delete;

 private:
  const EventContext *previous_;
};

class EventSink final : public spdlog::sinks::base_sink<std::mutex> {
 public:
  explicit EventSink(spdlog::sink_ptr output) : output_(std::move(output)) {}

 private:
  void sink_it_(const spdlog::details::log_msg &message) override {
    const auto event = nlohmann::json::from_cbor(message.payload.begin(), message.payload.end());
    const auto &bytes = event.at("message").get_binary();
    const std::string text{bytes.begin(), bytes.end()};
    auto original = message;
    original.logger_name = event.at("logger").get_ref<const std::string &>();
    original.payload = text;
    const EventContext context{&original, &event};
    const EventScope scope{context};
    if (output_->should_log(original.level)) output_->log(original);
  }

  void flush_() override { output_->flush(); }

  spdlog::sink_ptr output_;
};

// The primary logger owns the transport; the transport only references the inner output sink, avoiding a cycle.
class EventSinkOwner final : public spdlog::sinks::dist_sink_mt {
 public:
  EventSinkOwner(std::shared_ptr<spdlog::sinks::dist_sink_mt> output, std::shared_ptr<spdlog::logger> events)
      : spdlog::sinks::dist_sink_mt({output}), output_(std::move(output)), events_(std::move(events)) {
    events_->sinks() = {std::make_shared<EventSink>(output_)};
    events_->set_level(spdlog::level::trace);
  }

  void AddSink(spdlog::sink_ptr sink) { output_->add_sink(std::move(sink)); }

  void Log(spdlog::level::level_enum level, std::string_view name, std::string_view message,
           const nlohmann::json &attributes, std::string_view logger_name) {
    if (name.empty() || !attributes.is_object()) {
      throw std::invalid_argument("A structured log event requires a name and an attributes object");
    }
    // The private transport preserves arbitrary message bytes; only the JSON output formatter replaces invalid UTF-8.
    const auto payload = nlohmann::json::to_cbor(
        {{"name", name},
         {"logger", logger_name},
         {"message", nlohmann::json::binary(std::vector<uint8_t>{message.begin(), message.end()})},
         {"attributes", attributes}});
    events_->log(level, spdlog::string_view_t{reinterpret_cast<const char *>(payload.data()), payload.size()});
  }

 private:
  std::shared_ptr<spdlog::sinks::dist_sink_mt> output_;
  std::shared_ptr<spdlog::logger> events_;
};

EventSinkOwner *EventOwner(spdlog::logger &logger) {
  for (const auto &sink : logger.sinks()) {
    if (auto *owner = dynamic_cast<EventSinkOwner *>(sink.get())) return owner;
  }
  return nullptr;
}

}  // namespace

std::shared_ptr<spdlog::logger> EnableStructuredEvents(std::shared_ptr<spdlog::logger> logger) {
  if (!logger) throw std::invalid_argument("Structured logging requires a logger");
  if (EventOwner(*logger)) return logger;
  std::shared_ptr<spdlog::sinks::dist_sink_mt> output;
  if (logger->sinks().size() == 1) {
    output = std::dynamic_pointer_cast<spdlog::sinks::dist_sink_mt>(logger->sinks().front());
  }
  if (!output) output = std::make_shared<spdlog::sinks::dist_sink_mt>(logger->sinks());
  auto events = logger->clone("memgraph_events");
  logger->sinks() = {std::make_shared<EventSinkOwner>(std::move(output), std::move(events))};
  return logger;
}

bool IsStructuredLoggingEnabled() { return EventOwner(*spdlog::default_logger_raw()) != nullptr; }

bool AddStructuredLogSink(spdlog::logger &logger, spdlog::sink_ptr sink) {
  if (auto *owner = EventOwner(logger)) {
    owner->AddSink(std::move(sink));
    return true;
  }
  return false;
}

void LogEvent(spdlog::level::level_enum level, std::string_view name, std::string_view message,
              const nlohmann::json &attributes) {
  const auto logger = spdlog::default_logger();
  if (!logger->should_log(level)) return;
  if (auto *events = EventOwner(*logger)) {
    events->Log(level, name, message, attributes, logger->name());
  } else {
    logger->log(level, spdlog::string_view_t{message.data(), message.size()});
  }
}

void AddStructuredEventFields(const spdlog::details::log_msg &message, nlohmann::json &record) {
  // Match the exact dispatched message so nested ordinary logging cannot inherit another event's attributes.
  if (current_event == nullptr || current_event->message != &message) return;
  record["event"] = {{"name", current_event->event->at("name")}};
  record["attributes"] = current_event->event->at("attributes");
}

void EmitAuthenticationFailure(std::string_view session_id, std::string_view reason) {
  if (!IsStructuredLoggingEnabled()) return;
  LogEvent(spdlog::level::warn,
           "authentication.failed",
           "Authentication failure",
           {{"session.id", session_id}, {"outcome", "failure"}, {"reason", reason}});
}

}  // namespace memgraph::logging
