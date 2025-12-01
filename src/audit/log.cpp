// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "audit/log.hpp"

#include <sstream>

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <utility>

#include "communication/bolt/v1/mg_types.hpp"
#include "query/string_helpers.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"

#include <mutex>

namespace memgraph::audit {

// Helper function that converts a `communication::bolt::Value` to `nlohmann::json`.
inline nlohmann::json BoltValueToJson(const communication::bolt::Value &value) {
  nlohmann::json ret;
  switch (value.type()) {
    using enum memgraph::communication::bolt::Value::Type;
    case Null:
      break;
    case Bool:
      ret = value.ValueBool();
      break;
    case Int:
      ret = value.ValueInt();
      break;
    case Double:
      ret = value.ValueDouble();
      break;
    case String:
      ret = value.ValueString();
      break;
    case List: {
      ret = nlohmann::json::array();
      for (const auto &item : value.ValueList()) {
        ret.push_back(BoltValueToJson(item));
      }
      break;
    }
    case Map: {
      auto const &bolt_value_map = value.ValueMap();
      auto const info = memgraph::communication::bolt::BoltMapToMgTypeInfo(bolt_value_map);
      if (info) {
        switch (info->type) {
          case communication::bolt::MgType::Enum: {
            ret = info->value_str;
            break;
          }
        }
      } else {
        ret = nlohmann::json::object();
        for (const auto &[map_k, map_v] : bolt_value_map) {
          ret.push_back(nlohmann::json::object_t::value_type(map_k, BoltValueToJson(map_v)));
        }
      }
      break;
    }
    case Date: {
      std::stringstream ss;
      ss << utils::Date(std::chrono::microseconds{value.ValueDate().MicrosecondsSinceEpoch()});
      ret = ss.str();
      break;
    }
    case Duration: {
      std::stringstream ss;
      ss << utils::Duration(value.ValueDuration().microseconds);
      ret = ss.str();
      break;
    }
    case LocalTime: {
      std::stringstream ss;
      ss << utils::LocalTime(value.ValueLocalTime().MicrosecondsSinceEpoch());
      ret = ss.str();
      break;
    }
    case LocalDateTime: {
      std::stringstream ss;
      ss << value.ValueLocalDateTime();
      ret = ss.str();
      break;
    }
    case ZonedDateTime: {
      const auto &temp_value = value.ValueZonedDateTime();
      std::stringstream ss;
      ss << utils::ZonedDateTime(temp_value.SysTimeSinceEpoch(), temp_value.GetTimezone());
      ret = ss.str();
      break;
    }
    case Point2d: {
      std::stringstream ss;
      ss << query::CypherConstructionFor(value.ValuePoint2d());
      ret = ss.str();
      break;
    }
    case Point3d: {
      std::stringstream ss;
      ss << query::CypherConstructionFor(value.ValuePoint3d());
      ret = ss.str();
      break;
    }
    case Vertex:
    case Edge:
    case UnboundedEdge:
    case Path: {
      // Should not be sent for audit
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
  MG_ASSERT(!started_.load(std::memory_order_acquire), "Trying to start an already started audit log!");

  utils::EnsureDirOrDie(storage_directory_);

  buffer_.emplace(buffer_size_);
  started_.store(true, std::memory_order_release);

  ReopenLog();
  scheduler_.SetInterval(std::chrono::milliseconds(buffer_flush_interval_millis_));
  scheduler_.Run("Audit", [&] { Flush(); });
}

Log::~Log() {
  if (!started_.load(std::memory_order_acquire)) return;

  started_.store(false, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  scheduler_.Stop();
  Flush();
}

void Log::Record(const std::string &address, const std::string &username, const std::string &query,
                 const memgraph::communication::bolt::map_t &params, const std::string &db) {
  if (!started_.load(std::memory_order_relaxed)) return;
  auto timestamp =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  buffer_->emplace(Item{timestamp, address, username, query, params, db});
}

void Log::ReopenLog() {
  if (!started_.load(std::memory_order_relaxed)) return;
  auto guard = std::lock_guard{lock_};
  if (log_.IsOpen()) log_.Close();
  log_.Open(storage_directory_ / "audit.log", utils::OutputFile::Mode::APPEND_TO_EXISTING);
}

void Log::Flush() {
  auto guard = std::lock_guard{lock_};
  for (uint64_t i = 0; i < buffer_size_; ++i) {
    auto item = buffer_->pop();
    if (!item) break;

    auto params_json = nlohmann::json::object();
    for (const auto &[k, v] : item->params) {
      params_json.push_back(nlohmann::json::object_t::value_type(k, BoltValueToJson(v)));
    }

    log_.Write(fmt::format("{}.{:06d},{},{},{},{},{}\n", item->timestamp / 1000000, item->timestamp % 1000000,
                           item->address, item->username, item->db, utils::Escape(item->query),
                           utils::Escape(params_json.dump())));
  }
  log_.Sync();
}

}  // namespace memgraph::audit
