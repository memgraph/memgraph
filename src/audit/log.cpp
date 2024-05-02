// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "audit/log.hpp"

#include <chrono>
#include <sstream>

#include <fmt/format.h>
#include <json/json.hpp>
#include <utility>

#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"

namespace memgraph::audit {

// Helper function that converts a `storage::PropertyValue` to `nlohmann::json`.
inline nlohmann::json PropertyValueToJson(const storage::PropertyValue &pv) {
  nlohmann::json ret;
  switch (pv.type()) {
    case storage::PropertyValue::Type::Null:
      break;
    case storage::PropertyValue::Type::Bool:
      ret = pv.ValueBool();
      break;
    case storage::PropertyValue::Type::Int:
      ret = pv.ValueInt();
      break;
    case storage::PropertyValue::Type::Double:
      ret = pv.ValueDouble();
      break;
    case storage::PropertyValue::Type::String:
      ret = pv.ValueString();
      break;
    case storage::PropertyValue::Type::List: {
      ret = nlohmann::json::array();
      for (const auto &item : pv.ValueList()) {
        ret.push_back(PropertyValueToJson(item));
      }
      break;
    }
    case storage::PropertyValue::Type::Map: {
      ret = nlohmann::json::object();
      for (const auto &item : pv.ValueMap()) {
        ret.push_back(nlohmann::json::object_t::value_type(item.first, PropertyValueToJson(item.second)));
      }
      break;
    }
    case storage::PropertyValue::Type::TemporalData: {
      const auto temporal_data = pv.ValueTemporalData();
      auto to_string = [](auto temporal_data) {
        std::stringstream ss;
        const auto ms = temporal_data.microseconds;
        switch (temporal_data.type) {
          case storage::TemporalType::Date: {
            const auto date = utils::Date(ms);
            ss << date;
            return ss.str();
          }
          case storage::TemporalType::Duration: {
            const auto dur = utils::Duration(ms);
            ss << dur;
            return ss.str();
          }
          case storage::TemporalType::LocalTime: {
            const auto lt = utils::LocalTime(ms);
            ss << lt;
            return ss.str();
          }
          case storage::TemporalType::LocalDateTime: {
            const auto ldt = utils::LocalDateTime(ms);
            ss << ldt;
            return ss.str();
          }
        }
      };
      ret = to_string(temporal_data);
      break;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      const auto zoned_temporal_data = pv.ValueZonedTemporalData();
      auto to_string = [](auto zoned_temporal_data) {
        std::stringstream ss;
        switch (zoned_temporal_data.type) {
          case storage::ZonedTemporalType::ZonedDateTime: {
            const auto zdt = utils::ZonedDateTime(zoned_temporal_data.microseconds, zoned_temporal_data.timezone);
            ss << zdt;
            return ss.str();
          }
        }
      };
      ret = to_string(zoned_temporal_data);
      break;
    }
  }
  return ret;
}

Log::Log(std::filesystem::path storage_directory, int32_t buffer_size, int32_t buffer_flush_interval_millis)
    : storage_directory_(std::move(storage_directory)),
      buffer_size_(buffer_size),
      buffer_flush_interval_millis_(buffer_flush_interval_millis),
      started_(false) {}

void Log::Start() {
  MG_ASSERT(!started_, "Trying to start an already started audit log!");

  utils::EnsureDirOrDie(storage_directory_);

  buffer_.emplace(buffer_size_);
  started_ = true;

  ReopenLog();
  scheduler_.Run("Audit", std::chrono::milliseconds(buffer_flush_interval_millis_), [&] { Flush(); });
}

Log::~Log() {
  if (!started_) return;

  started_ = false;
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  scheduler_.Stop();
  Flush();
}

void Log::Record(const std::string &address, const std::string &username, const std::string &query,
                 const storage::PropertyValue &params, const std::string &db) {
  if (!started_.load(std::memory_order_relaxed)) return;
  auto timestamp =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  buffer_->emplace(Item{timestamp, address, username, query, params, db});
}

void Log::ReopenLog() {
  if (!started_.load(std::memory_order_relaxed)) return;
  std::lock_guard<std::mutex> guard(lock_);
  if (log_.IsOpen()) log_.Close();
  log_.Open(storage_directory_ / "audit.log", utils::OutputFile::Mode::APPEND_TO_EXISTING);
}

void Log::Flush() {
  std::lock_guard<std::mutex> guard(lock_);
  for (uint64_t i = 0; i < buffer_size_; ++i) {
    auto item = buffer_->pop();
    if (!item) break;
    log_.Write(fmt::format("{}.{:06d},{},{},{},{},{}\n", item->timestamp / 1000000, item->timestamp % 1000000,
                           item->address, item->username, item->db, utils::Escape(item->query),
                           utils::Escape(PropertyValueToJson(item->params).dump())));
  }
  log_.Sync();
}

}  // namespace memgraph::audit
