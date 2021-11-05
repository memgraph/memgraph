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

#include "query/metadata.hpp"

namespace query {

using namespace std::literals;

constexpr std::array severity_level_mapping{
    std::pair{SeverityLevel::INFO, "INFO"sv},
    std::pair{SeverityLevel::WARNING, "INFO"sv},
};

constexpr std::array code_mapping{
    std::pair{NotificationCode::CREATE_CONSTRAINT, "CreateConstraint"sv},
    std::pair{NotificationCode::CREATE_REPLICATION, "CreateReplication"sv},
    std::pair{NotificationCode::CREATE_INDEX, "CreateIndex"sv},
    std::pair{NotificationCode::CREATE_STREAM, "CreateStream"sv},
    std::pair{NotificationCode::CREATE_TRIGGER, "CreateTrigger"sv},
    std::pair{NotificationCode::DROP_CONSTRAINT, "DropConstraint"sv},
    std::pair{NotificationCode::DROP_REPLICATION, "DropReplication"sv},
    std::pair{NotificationCode::DROP_INDEX, "DropIndex"sv},
    std::pair{NotificationCode::DROP_STREAM, "DropStream"sv},
    std::pair{NotificationCode::DROP_TRIGGER, "DropTrigger"sv},
    std::pair{NotificationCode::DEPRECATED_FUNCTION, "DeprecatedFunction"sv},
    std::pair{NotificationCode::INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY, "IndexLookupForDynamicProperty"sv},
};

template <typename Enum>
std::string_view EnumToString(Enum key, const auto &mappings) {
  const auto enum_string_pair =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &elem) { return elem.first == key; });
  return enum_string_pair == mappings.end() ? "" : enum_string_pair->second;
}

Notification::Notification(SeverityLevel level) : level{level} {};

Notification::Notification(SeverityLevel level, NotificationCode code, std::string &&title, std::string &&description)
    : level{level}, code{code}, title(std::move(title)), description(std::move(description)){};

std::map<std::string, TypedValue> Notification::ConvertToMap() const {
  return std::map<std::string, TypedValue>{{"severity", TypedValue(EnumToString(level, severity_level_mapping))},
                                           {"code", TypedValue(EnumToString(code, code_mapping))},
                                           {"title", TypedValue(title)},
                                           {"description", TypedValue(description)}};
}

}  // namespace query