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

#include "query/metadata.hpp"

namespace query {

namespace {
using namespace std::literals;

const std::array severity_level_mapping{
    std::pair{SeverityLevel::INFO, "INFO"s},
    std::pair{SeverityLevel::WARNING, "WARNING"s},
};

const std::array code_mapping{
    std::pair{NotificationCode::CREATE_CONSTRAINT, "CreateConstraint"s},
    std::pair{NotificationCode::CREATE_INDEX, "CreateIndex"s},
    std::pair{NotificationCode::CREATE_STREAM, "CreateStream"s},
    std::pair{NotificationCode::CREATE_TRIGGER, "CreateTrigger"s},
    std::pair{NotificationCode::DROP_CONSTRAINT, "DropConstraint"s},
    std::pair{NotificationCode::DROP_REPLICATION, "DropReplica"s},
    std::pair{NotificationCode::DROP_INDEX, "DropIndex"s},
    std::pair{NotificationCode::DROP_STREAM, "DropStream"s},
    std::pair{NotificationCode::DROP_TRIGGER, "DropTrigger"s},
    std::pair{NotificationCode::DEPRECATED_FUNCTION, "DeprecatedFunction"s},
    std::pair{NotificationCode::EXISTANT_CONSTRAINT, "ConstraintAlreadyExists"s},
    std::pair{NotificationCode::EXISTANT_INDEX, "IndexAlreadyExists"s},
    std::pair{NotificationCode::NONEXISTANT_INDEX, "IndexDoesNotExist"s},
    std::pair{NotificationCode::NONEXISTANT_CONSTRAINT, "ConstraintDoesNotExist"s},
    std::pair{NotificationCode::INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY, "IndexLookupForDynamicProperty"s},
    std::pair{NotificationCode::REGISTER_REPLICA, "RegisterReplica"s},
    std::pair{NotificationCode::SET_REPLICA, "SetReplica"s},
};
}  // namespace

template <typename Enum>
std::string EnumToString(Enum key, const auto &mappings) {
  const auto enum_string_pair =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &elem) { return elem.first == key; });
  return enum_string_pair == mappings.end() ? "" : enum_string_pair->second;
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

}  // namespace query