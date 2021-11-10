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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "query/typed_value.hpp"

namespace query {

enum class SeverityLevel : uint8_t { INFO, WARNING };

enum class NotificationCode : uint8_t {
  CREATE_CONSTRAINT,
  CREATE_INDEX,
  CHECK_STREAM,
  CREATE_STREAM,
  CREATE_TRIGGER,
  DROP_CONSTRAINT,
  DROP_INDEX,
  DROP_REPLICA,
  DROP_STREAM,
  DROP_TRIGGER,
  DEPRECATED_FUNCTION,
  EXISTANT_INDEX,
  EXISTANT_CONSTRAINT,
  LOAD_CSV_TIP,
  NONEXISTANT_INDEX,
  NONEXISTANT_CONSTRAINT,
  INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY,
  REPLICA_PORT_WARNING,
  REGISTER_REPLICA,
  SET_REPLICA,
  START_STREAM,
  START_ALL_STREAMS,
  STOP_STREAM,
  STOP_ALL_STREAMS,
};

struct Notification {
  SeverityLevel level;
  NotificationCode code;
  std::string title;
  std::string description;

  explicit Notification(SeverityLevel level);

  Notification(SeverityLevel level, NotificationCode code, std::string &&title, std::string &&description);

  Notification(SeverityLevel level, NotificationCode code, std::string &&title);

  std::map<std::string, TypedValue> ConvertToMap() const;
};

struct ExecutionStats {
  // All the stats are have specific key to be compatible with neo4j
  enum class Key : uint8_t {
    CREATED_NODES,
    DELETED_NODES,
    CREATED_EDGES,
    DELETED_EDGES,
    CREATED_LABELS,
    DELETED_LABELS,
    UPDATED_PROPERTIES,
    CREATED_INDEXES,
    DELETED_INDEXES,
    CREATED_CONSTRAINTS,
    DELETED_CONSTRAINTS,
  };

  int64_t &operator[](Key key) { return counters[key]; }

  std::map<Key, int64_t> counters{
      {Key::CREATED_NODES, 0},       {Key::DELETED_NODES, 0},       {Key::CREATED_EDGES, 0},
      {Key::DELETED_EDGES, 0},       {Key::CREATED_LABELS, 0},      {Key::DELETED_LABELS, 0},
      {Key::UPDATED_PROPERTIES, 0},  {Key::CREATED_INDEXES, 0},     {Key::DELETED_INDEXES, 0},
      {Key::CREATED_CONSTRAINTS, 0}, {Key::DELETED_CONSTRAINTS, 0},
  };
};

std::string ExecutionStatsKeyToString(ExecutionStats::Key key);

}  // namespace query