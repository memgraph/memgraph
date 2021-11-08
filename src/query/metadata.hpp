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

#include <map>
#include <string>
#include <string_view>

#include "query/typed_value.hpp"

namespace query {

enum class SeverityLevel : uint8_t { INFO, WARNING };

enum class NotificationCode : uint8_t {
  CHECK_STREAM,
  CREATE_CONSTRAINT,
  CREATE_INDEX,
  CREATE_STREAM,
  CREATE_TRIGGER,
  DROP_CONSTRAINT,
  DROP_REPLICATION,
  DROP_INDEX,
  DROP_REPLICA,
  DROP_STREAM,
  DROP_TRIGGER,
  DEPRECATED_FUNCTION,
  EXISTANT_INDEX,
  EXISTANT_CONSTRAINT,
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
  static constexpr std::string_view kCreatedNodes{"nodes-created"};
  static constexpr std::string_view kDeletedNodes{"nodes-deleted"};
  static constexpr std::string_view kCreatedEdges{"relationships-created"};
  static constexpr std::string_view kDeletedEdges{"relationships-deleted"};
  static constexpr std::string_view kPropertiesSet{"properties-set"};
  static constexpr std::string_view kCreatedLabels{"labels-added"};
  static constexpr std::string_view kDeletedLabels{"labels-removed"};
  static constexpr std::string_view kCreatedIndexes{"indexes-added"};
  static constexpr std::string_view kDeletedIndexes{"indexes-removed"};
  static constexpr std::string_view kCreatedConstraints{"constraints-added"};
  static constexpr std::string_view kDeletedConstraints{"constraints-removed"};

  int64_t &operator[](std::string_view key) { return counters[key]; }

  std::map<std::string_view, int64_t> counters{
      {kCreatedNodes, 0},   {kDeletedNodes, 0},       {kCreatedEdges, 0},       {kDeletedEdges, 0},
      {kPropertiesSet, 0},  {kCreatedLabels, 0},      {kDeletedLabels, 0},      {kCreatedIndexes, 0},
      {kDeletedIndexes, 0}, {kCreatedConstraints, 0}, {kDeletedConstraints, 0},
  };
};
}  // namespace query