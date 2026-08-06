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

#include "algorithm/json_writer.hpp"

#include <algorithm>
#include <cstdlib>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

namespace Export {
namespace {

constexpr std::uint16_t kSridCartesian2d = 7203;
constexpr std::uint16_t kSridWgs842d = 4326;
constexpr std::uint16_t kSridCartesian3d = 9157;
constexpr std::uint16_t kSridWgs843d = 4979;

constexpr const char *kKeyType = "type";
constexpr const char *kKeyId = "id";
constexpr const char *kKeyLabels = "labels";
constexpr const char *kKeyLabel = "label";
constexpr const char *kKeyProperties = "properties";
constexpr const char *kKeyStart = "start";
constexpr const char *kKeyEnd = "end";
constexpr const char *kKeyNodes = "nodes";
constexpr const char *kKeyRels = "rels";
constexpr const char *kTypeNode = "node";
constexpr const char *kTypeRelationship = "relationship";

constexpr int64_t kMicrosPerSecond = 1000000;
constexpr int64_t kMicrosPerMinute = 60 * kMicrosPerSecond;
constexpr int64_t kMicrosPerHour = 60 * kMicrosPerMinute;
constexpr int64_t kMicrosPerDay = 24 * kMicrosPerHour;

// Property key order is not reproducible across implementations, so sort: `mgp` only exposes properties as an
// unordered_map, and hash order would reshuffle as the map grows.
std::vector<std::pair<std::string, mgp::Value>> SortedProperties(std::unordered_map<std::string, mgp::Value> &&props) {
  std::vector<std::pair<std::string, mgp::Value>> sorted(std::make_move_iterator(props.begin()),
                                                         std::make_move_iterator(props.end()));
  std::ranges::sort(sorted, {}, &std::pair<std::string, mgp::Value>::first);
  return sorted;
}

Json PropertiesToJson(std::unordered_map<std::string, mgp::Value> &&props) {
  Json object = Json::object();
  for (auto &[key, value] : SortedProperties(std::move(props))) {
    object[key] = ValueToJson(value);
  }
  return object;
}

std::vector<std::string_view> SortedLabels(const mgp::Node &node) {
  const auto labels = node.Labels();
  std::vector<std::string_view> sorted;
  sorted.reserve(labels.Size());
  // Indexed rather than range-for: mgp::Labels::begin() is non-const.
  for (size_t i = 0; i < labels.Size(); ++i) {
    sorted.emplace_back(labels[i]);
  }
  std::ranges::sort(sorted);
  return sorted;
}

// Sub-second digits for the ISO_LOCAL_TIME family: none when zero, 3 for a whole millisecond, 6 otherwise. This is not
// the same rule durations use, which strip trailing zeros instead.
std::string SubSecondSuffix(int millisecond, int microsecond) {
  if (millisecond == 0 && microsecond == 0) return "";
  if (microsecond == 0) return fmt::format(".{:03}", millisecond);
  return fmt::format(".{:06}", (millisecond * 1000) + microsecond);
}

// Seconds and below, with seconds themselves elided when the whole tail is zero.
std::string SecondsSuffix(int second, int millisecond, int microsecond) {
  if (second == 0 && millisecond == 0 && microsecond == 0) return "";
  return fmt::format(":{:02}{}", second, SubSecondSuffix(millisecond, microsecond));
}

std::string OffsetSuffix(int offset_minutes) {
  if (offset_minutes == 0) return "Z";
  const int magnitude = std::abs(offset_minutes);
  return fmt::format("{}{:02}:{:02}", offset_minutes < 0 ? '-' : '+', magnitude / 60, magnitude % 60);
}

Json PointToJson(double x, double y, Json z, std::uint16_t srid) {
  Json object = Json::object();
  object["crs"] = SridToCrs(srid);
  // WGS-84 is emitted with geographic key names, latitude first; Memgraph stores longitude in x and latitude in y.
  if (srid == kSridWgs842d || srid == kSridWgs843d) {
    object["latitude"] = y;
    object["longitude"] = x;
    object["height"] = std::move(z);
  } else {
    object["x"] = x;
    object["y"] = y;
    object["z"] = std::move(z);
  }
  return object;
}

}  // namespace

std::string_view SridToCrs(std::uint16_t srid) {
  switch (srid) {
    case kSridCartesian2d:
      return "cartesian";
    case kSridCartesian3d:
      return "cartesian-3d";
    case kSridWgs842d:
      return "wgs-84";
    case kSridWgs843d:
      return "wgs-84-3d";
    default:
      throw mgp::ValueException(fmt::format("Cannot export a point with unknown SRID {}", srid));
  }
}

std::string DateToString(const mgp::Date &date) {
  return fmt::format("{:04}-{:02}-{:02}", date.Year(), date.Month(), date.Day());
}

std::string LocalTimeToString(const mgp::LocalTime &local_time) {
  return fmt::format("{:02}:{:02}{}",
                     local_time.Hour(),
                     local_time.Minute(),
                     SecondsSuffix(local_time.Second(), local_time.Millisecond(), local_time.Microsecond()));
}

std::string LocalDateTimeToString(const mgp::LocalDateTime &local_date_time) {
  return fmt::format(
      "{:04}-{:02}-{:02}T{:02}:{:02}{}",
      local_date_time.Year(),
      local_date_time.Month(),
      local_date_time.Day(),
      local_date_time.Hour(),
      local_date_time.Minute(),
      SecondsSuffix(local_date_time.Second(), local_date_time.Millisecond(), local_date_time.Microsecond()));
}

std::string ZonedDateTimeToString(const mgp::ZonedDateTime &zoned_date_time) {
  // Two independent rules: the offset renders as `Z` iff it is zero, and `[Name]` is appended iff the zone is a named
  // one. `Timezone()` is empty exactly for offset-only zones.
  const std::string_view timezone{zoned_date_time.Timezone()};
  return fmt::format(
      "{:04}-{:02}-{:02}T{:02}:{:02}{}{}{}",
      zoned_date_time.Year(),
      zoned_date_time.Month(),
      zoned_date_time.Day(),
      zoned_date_time.Hour(),
      zoned_date_time.Minute(),
      SecondsSuffix(zoned_date_time.Second(), zoned_date_time.Millisecond(), zoned_date_time.Microsecond()),
      OffsetSuffix(zoned_date_time.Offset()),
      timezone.empty() ? std::string{} : fmt::format("[{}]", timezone));
}

std::string DurationToString(const mgp::Duration &duration) {
  int64_t remaining = duration.Microseconds();
  if (remaining == 0) return "PT0S";

  // Truncating division carries the sign into every component, which is what the reference emits (`PT-2H-30M`, not
  // `-PT2H30M`). Month/year components are unreachable: Memgraph cannot store them.
  const int64_t days = remaining / kMicrosPerDay;
  remaining %= kMicrosPerDay;
  const int64_t hours = remaining / kMicrosPerHour;
  remaining %= kMicrosPerHour;
  const int64_t minutes = remaining / kMicrosPerMinute;
  remaining %= kMicrosPerMinute;
  const int64_t seconds = remaining / kMicrosPerSecond;
  const int64_t micros = remaining % kMicrosPerSecond;

  std::string result = "P";
  if (days != 0) result += fmt::format("{}D", days);
  if (hours == 0 && minutes == 0 && seconds == 0 && micros == 0) return result;

  result += "T";
  if (hours != 0) result += fmt::format("{}H", hours);
  if (minutes != 0) result += fmt::format("{}M", minutes);
  if (seconds == 0 && micros == 0) return result;

  if (micros == 0) {
    result += fmt::format("{}S", seconds);
    return result;
  }
  // Fractional seconds strip trailing zeros (`PT1.5S`, `PT1H30.25S`) — unlike the local-time family's 3-digit groups.
  // The sign belongs to the whole number, so a sub-second-only negative renders as `-0.5`.
  std::string fraction = fmt::format("{:06}", std::abs(micros));
  fraction.erase(fraction.find_last_not_of('0') + 1);
  const bool negative = seconds < 0 || micros < 0;
  result += fmt::format("{}{}.{}S", negative ? "-" : "", std::abs(seconds), fraction);
  return result;
}

Json ValueToJson(const mgp::Value &value) {
  switch (value.Type()) {
    case mgp::Type::Null:
      return nullptr;
    case mgp::Type::Bool:
      return value.ValueBool();
    case mgp::Type::Int:
      return value.ValueInt();
    case mgp::Type::Double:
      return value.ValueDouble();
    case mgp::Type::String:
      return std::string{value.ValueString()};
    case mgp::Type::List: {
      Json array = Json::array();
      for (const auto element : value.ValueList()) {
        array.push_back(ValueToJson(element));
      }
      return array;
    }
    case mgp::Type::Map: {
      Json object = Json::object();
      for (const auto &[key, element] : value.ValueMap()) {
        object[std::string{key}] = ValueToJson(element);
      }
      return object;
    }
    case mgp::Type::Date:
      return DateToString(value.ValueDate());
    case mgp::Type::LocalTime:
      return LocalTimeToString(value.ValueLocalTime());
    case mgp::Type::LocalDateTime:
      return LocalDateTimeToString(value.ValueLocalDateTime());
    case mgp::Type::ZonedDateTime:
      return ZonedDateTimeToString(value.ValueZonedDateTime());
    case mgp::Type::Duration:
      return DurationToString(value.ValueDuration());
    case mgp::Type::Point2d: {
      const auto point = value.ValuePoint2d();
      return PointToJson(point.X(), point.Y(), nullptr, point.Srid());
    }
    case mgp::Type::Point3d: {
      const auto point = value.ValuePoint3d();
      return PointToJson(point.X(), point.Y(), point.Z(), point.Srid());
    }
    case mgp::Type::Enum:
      return value.ValueEnum().ToString();
    default:
      throw mgp::ValueException("Cannot export a property of this type to JSON");
  }
}

namespace {

// Appends {id, labels, properties} — the shape shared by a top-level node and an inlined relationship endpoint. Takes
// the object by reference so a top-level node can put its "type" key first without a merge.
void AppendNodeBody(Json &object, const mgp::Node &node, bool write_properties) {
  object[kKeyId] = std::to_string(node.Id().AsInt());
  object[kKeyLabels] = SortedLabels(node);
  if (write_properties) {
    // An empty `properties` is omitted entirely rather than emitted as {}.
    if (auto properties = PropertiesToJson(node.Properties()); !properties.empty()) {
      object[kKeyProperties] = std::move(properties);
    }
  }
}

Json NodeBody(const mgp::Node &node, bool write_properties) {
  Json object = Json::object();
  AppendNodeBody(object, node, write_properties);
  return object;
}

}  // namespace

Json NodeToJson(const mgp::Node &node, bool write_properties) {
  Json object = Json::object();
  object[kKeyType] = kTypeNode;
  AppendNodeBody(object, node, write_properties);
  return object;
}

Json RelationshipToJson(const mgp::Relationship &relationship, const WriteConfig &config) {
  Json object = Json::object();
  object[kKeyType] = kTypeRelationship;
  object[kKeyId] = std::to_string(relationship.Id().AsInt());
  object[kKeyLabel] = std::string{relationship.Type()};
  if (config.write_relationship_properties) {
    if (auto properties = PropertiesToJson(relationship.Properties()); !properties.empty()) {
      object[kKeyProperties] = std::move(properties);
    }
  }
  // Endpoints are inlined in full even when they are absent from the exported node set. `writeNodeProperties` governs
  // them; `writeRelationshipProperties` does not.
  object[kKeyStart] = NodeBody(relationship.From(), config.write_node_properties);
  object[kKeyEnd] = NodeBody(relationship.To(), config.write_node_properties);
  return object;
}

std::uint64_t CountProperties(const mgp::Node &node) { return node.Properties().size(); }

std::uint64_t CountProperties(const mgp::Relationship &relationship) { return relationship.Properties().size(); }

void JsonWriter::AddNode(const mgp::Node &node) {
  nodes_.push_back(NodeToJson(node, config_.write_node_properties));
  ++node_count_;
  property_count_ += CountProperties(node);
}

void JsonWriter::AddRelationship(const mgp::Relationship &relationship) {
  relationships_.push_back(RelationshipToJson(relationship, config_));
  ++relationship_count_;
  // Only the relationship's own properties count; the inlined endpoints' do not.
  property_count_ += CountProperties(relationship);
}

std::string JsonWriter::Dump() const {
  if (nodes_.empty() && relationships_.empty()) return "";

  switch (config_.format) {
    case JsonFormat::kJsonLines: {
      std::string result;
      const auto append_lines = [&result](const Json &elements) {
        for (const auto &element : elements) {
          if (!result.empty()) result += '\n';
          result += element.dump();
        }
      };
      append_lines(nodes_);
      append_lines(relationships_);
      return result;
    }
    case JsonFormat::kJson: {
      Json object = Json::object();
      object[kKeyNodes] = nodes_;
      object[kKeyRels] = relationships_;
      return object.dump();
    }
    case JsonFormat::kJsonIdAsKeys: {
      const auto by_id = [](const Json &elements) {
        Json object = Json::object();
        for (const auto &element : elements) {
          object[element.at(kKeyId).get<std::string>()] = element;
        }
        return object;
      };
      Json object = Json::object();
      object[kKeyNodes] = by_id(nodes_);
      object[kKeyRels] = by_id(relationships_);
      return object.dump();
    }
  }
  throw mgp::ValueException("Unhandled JSON output format");
}

}  // namespace Export
