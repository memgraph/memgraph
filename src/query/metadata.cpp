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

#include <algorithm>
#include <compare>
#include <string_view>

#include "query/metadata.hpp"

namespace query {

namespace {
using namespace std::literals;

constexpr std::array severity_level_mapping{
    std::pair{SeverityLevel::INFO, "INFO"sv},
    std::pair{SeverityLevel::WARNING, "WARNING"sv},
};

constexpr std::array code_mapping{
    std::pair{NotificationCode::CREATE_CONSTRAINT, "CreateConstraint"sv},
    std::pair{NotificationCode::CREATE_INDEX, "CreateIndex"sv},
    std::pair{NotificationCode::CREATE_STREAM, "CreateStream"sv},
    std::pair{NotificationCode::CHECK_STREAM, "CheckStream"sv},
    std::pair{NotificationCode::CREATE_TRIGGER, "CreateTrigger"sv},
    std::pair{NotificationCode::DROP_CONSTRAINT, "DropConstraint"sv},
    std::pair{NotificationCode::DROP_REPLICA, "DropReplica"sv},
    std::pair{NotificationCode::DROP_INDEX, "DropIndex"sv},
    std::pair{NotificationCode::DROP_STREAM, "DropStream"sv},
    std::pair{NotificationCode::DROP_TRIGGER, "DropTrigger"sv},
    std::pair{NotificationCode::EXISTANT_CONSTRAINT, "ConstraintAlreadyExists"sv},
    std::pair{NotificationCode::EXISTANT_INDEX, "IndexAlreadyExists"sv},
    std::pair{NotificationCode::LOAD_CSV_TIP, "LoadCSVTip"sv},
    std::pair{NotificationCode::NONEXISTANT_INDEX, "IndexDoesNotExist"sv},
    std::pair{NotificationCode::NONEXISTANT_CONSTRAINT, "ConstraintDoesNotExist"sv},
    std::pair{NotificationCode::REGISTER_REPLICA, "RegisterReplica"sv},
    std::pair{NotificationCode::REPLICA_PORT_WARNING, "ReplicaPortWarning"sv},
    std::pair{NotificationCode::SET_REPLICA, "SetReplica"sv},
    std::pair{NotificationCode::START_STREAM, "StartStream"sv},
    std::pair{NotificationCode::START_ALL_STREAMS, "StartAllStreams"sv},
    std::pair{NotificationCode::STOP_STREAM, "StopStream"sv},
    std::pair{NotificationCode::STOP_ALL_STREAMS, "StopAllStreams"sv},
};

constexpr std::array execution_stats_mapping{
    std::pair{ExecutionStats::Key::CREATED_NODES, "nodes-created"sv},
    std::pair{ExecutionStats::Key::DELETED_NODES, "nodes-deleted"sv},
    std::pair{ExecutionStats::Key::CREATED_EDGES, "relationships-created"sv},
    std::pair{ExecutionStats::Key::DELETED_EDGES, "relationships-deleted"sv},
    std::pair{ExecutionStats::Key::CREATED_LABELS, "labels-added"sv},
    std::pair{ExecutionStats::Key::DELETED_LABELS, "labels-removed"sv},
    std::pair{ExecutionStats::Key::UPDATED_PROPERTIES, "properties-set"sv},
};
}  // namespace

template <typename Enum>
std::string EnumToString(Enum key, const auto &mappings) {
  const auto enum_string_pair =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &elem) { return elem.first == key; });
  return enum_string_pair == mappings.end() ? "" : std::string(enum_string_pair->second);
}

Notification::Notification(SeverityLevel level) : level{level} {};

Notification::Notification(SeverityLevel level, NotificationCode code, std::string &&title, std::string &&description)
    : level{level}, code{code}, title(std::move(title)), description(std::move(description)){};

Notification::Notification(SeverityLevel level, NotificationCode code, std::string &&title)
    : level{level}, code{code}, title(std::move(title)){};

std::map<std::string, TypedValue> Notification::ConvertToMap() const {
  return std::map<std::string, TypedValue>{{"severity", TypedValue(EnumToString(level, severity_level_mapping))},
                                           {"code", TypedValue(EnumToString(code, code_mapping))},
                                           {"title", TypedValue(title)},
                                           {"description", TypedValue(description)}};
}

std::string ExecutionStatsKeyToString(const ExecutionStats::Key key) {
  return EnumToString(key, execution_stats_mapping);
}

}  // namespace query