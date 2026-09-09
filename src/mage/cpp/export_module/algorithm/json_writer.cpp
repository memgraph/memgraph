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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
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
std::vector<std::pair<std::string, mgp::Value>> SortedProperties(Properties &&props) {
  std::vector<std::pair<std::string, mgp::Value>> sorted(std::make_move_iterator(props.begin()),
                                                         std::make_move_iterator(props.end()));
  std::ranges::sort(sorted, {}, &std::pair<std::string, mgp::Value>::first);
  return sorted;
}

Json PropertiesToJson(Properties &&props) {
  Json object = Json::object();
  for (auto &[key, value] : SortedProperties(std::move(props))) {
    object[key] = ValueToJson(value);
  }
  return object;
}

// Returns owned strings, not views: mgp::Labels holds its own copy of the vertex and frees it on the way out, so a
// string_view into it would dangle for a virtual node (whose label text lives in that copy rather than in the
// long-lived name mapper).
std::vector<std::string> SortedLabels(const mgp::Node &node) {
  const auto labels = node.Labels();
  const auto size = labels.Size();
  std::vector<std::string> sorted;
  sorted.reserve(size);
  // Indexed rather than range-for: mgp::Labels::begin() is non-const.
  for (size_t i = 0; i < size; ++i) {
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

// Creates a temporary beside `target`, unique to this writer, and reports whether it managed to. `O_EXCL` so a stale
// temporary left by a crashed process whose pid has since been recycled is never adopted; mode 0666 so the kernel
// applies the umask exactly as it would for a file the stream created itself.
bool OpenTemporary(const std::string &target, const std::optional<std::filesystem::perms> &perms,
                   std::string &temp_path) {
  static std::atomic<std::uint64_t> counter{0};
  for (int attempt = 0; attempt < 8; ++attempt) {
    auto candidate = fmt::format("{}.{}.{}.part", target, ::getpid(), counter.fetch_add(1));
    const int fd = ::open(candidate.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0666);
    if (fd < 0) {
      if (errno != EEXIST) return false;
      continue;
    }
    // The rename replaces the target's inode, so without this the target's mode would become the temporary's and a
    // restricted export would silently widen on every rewrite.
    if (perms) ::fchmod(fd, static_cast<mode_t>(*perms) & 07777);
    ::close(fd);
    temp_path = std::move(candidate);
    return true;
  }
  return false;
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
    case mgp::Type::Double: {
      const auto number = value.ValueDouble();
      // JSON has no non-finite literals and the JSON library renders them as `null`, which is indistinguishable from
      // a stored null. Emit the textual forms instead, as the reference does.
      if (std::isnan(number)) return "NaN";
      if (std::isinf(number)) return number > 0 ? "Infinity" : "-Infinity";
      return number;
    }
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
    case mgp::Type::Enum: {
      // The property path hands enums over with empty type and value names — resolving them needs a database
      // accessor the C API does not carry there — so the only thing this could serialize is "::". Refuse rather
      // than write that into an export file.
      auto enum_value = value.ValueEnum();
      if (enum_value.TypeName().empty() || enum_value.ValueName().empty()) {
        throw mgp::ValueException("Cannot export an enum property: its type and value names are not available here");
      }
      return enum_value.ToString();
    }
    default:
      throw mgp::ValueException("Cannot export a property of this type to JSON");
  }
}

namespace {

// Appends {id, labels, properties} — the shape shared by a top-level node and an inlined relationship endpoint. Takes
// the object by reference so a top-level node can put its "type" key first without a merge. `properties` is null when
// property output is suppressed; otherwise it is consumed.
void AppendNodeBody(Json &object, const mgp::Node &node, Properties *properties) {
  object[kKeyId] = std::to_string(node.Id().AsInt());
  // `labels` and `properties` are both omitted entirely when empty rather than emitted as [] / {}.
  if (auto labels = SortedLabels(node); !labels.empty()) {
    object[kKeyLabels] = std::move(labels);
  }
  if (properties != nullptr && !properties->empty()) {
    object[kKeyProperties] = PropertiesToJson(std::move(*properties));
  }
}

// Names the element a failed serialization came from. The library reports "invalid UTF-8 byte" with no idea which of
// a whole database's elements carried it, which is unactionable on anything but a toy graph.
std::string Describe(const Json &element) {
  const auto text = [&element](const char *key) {
    const auto it = element.find(key);
    return it != element.end() && it->is_string() ? it->get<std::string>() : std::string{"?"};
  };
  return fmt::format("{} with id {}", text(kKeyType), text(kKeyId));
}

std::string DumpElement(const Json &element) {
  try {
    return element.dump();
  } catch (const nlohmann::json::exception &e) {
    throw mgp::ValueException(fmt::format("Cannot serialize the {}: {}", Describe(element), e.what()));
  }
}

Json NodeBody(const mgp::Node &node, bool write_properties) {
  Json object = Json::object();
  if (!write_properties) {
    AppendNodeBody(object, node, nullptr);
    return object;
  }
  auto properties = node.Properties();
  AppendNodeBody(object, node, &properties);
  return object;
}

}  // namespace

Json NodeToJson(const mgp::Node &node, Properties *properties) {
  Json object = Json::object();
  object[kKeyType] = kTypeNode;
  AppendNodeBody(object, node, properties);
  return object;
}

Json RelationshipToJson(const mgp::Relationship &relationship, Properties *properties, const WriteConfig &config) {
  Json object = Json::object();
  object[kKeyType] = kTypeRelationship;
  object[kKeyId] = std::to_string(relationship.Id().AsInt());
  object[kKeyLabel] = std::string{relationship.Type()};
  if (properties != nullptr && !properties->empty()) {
    object[kKeyProperties] = PropertiesToJson(std::move(*properties));
  }
  // Endpoints are inlined in full even when they are absent from the exported node set. `writeNodeProperties` governs
  // them; `writeRelationshipProperties` does not.
  object[kKeyStart] = NodeBody(relationship.From(), config.write_node_properties);
  object[kKeyEnd] = NodeBody(relationship.To(), config.write_node_properties);
  return object;
}

JsonWriter::JsonWriter(WriteConfig config, std::optional<std::string> file, bool retain)
    : config_(config), file_(std::move(file)), retain_(retain) {
  if (!file_) return;

  std::error_code error;
  // Write *through* a symlink rather than replacing it: `latest.json -> dumps/<date>.json` is an ordinary layout, and
  // renaming over the link would turn it into a regular file and leave what it pointed at stale.
  target_path_ = *file_;
  if (const auto link_status = std::filesystem::symlink_status(*file_, error);
      !error && std::filesystem::is_symlink(link_status)) {
    if (auto resolved = std::filesystem::weakly_canonical(*file_, error); !error) target_path_ = resolved.string();
  }

  const auto status = std::filesystem::status(target_path_, error);
  const bool exists = !error && std::filesystem::exists(status);
  const bool regular = exists && std::filesystem::is_regular_file(status);
  // A device or fifo cannot be renamed into place, and a file with more than one link would lose its other names, so
  // both are written directly. Everything else goes through a temporary beside the target — same filesystem, so the
  // rename in Finish() is atomic — which keeps a failure part-way through from destroying the previous export.
  bool in_place = exists && (!regular || std::filesystem::hard_link_count(target_path_, error) > 1);

  std::optional<std::filesystem::perms> target_perms;
  if (regular) {
    // Renaming a temporary into place needs write permission on the *directory*, not on the target, so without this
    // an export would overwrite a file the caller had deliberately made read-only. Probe it exactly as a direct write
    // would, and carry its permissions onto the temporary so the rename cannot widen them.
    const int probe = ::open(target_path_.c_str(), O_WRONLY | O_CLOEXEC);
    if (probe < 0) throw mgp::ValueException(fmt::format("Cannot open '{}' for writing", *file_));
    ::close(probe);
    target_perms = status.permissions();
  }

  if (!in_place && !OpenTemporary(target_path_, target_perms, temp_path_)) {
    // No temporary is possible here — a read-only directory, or a name the filesystem will not take once `.part` and
    // the uniquifier are appended. Writing in place is what this module did before the temporary existed, so fall
    // back to it rather than failing an export that would otherwise have worked. The atomicity guarantee is what is
    // lost, and only in this case.
    in_place = true;
  }

  out_.open(in_place ? target_path_ : temp_path_, std::ios::binary | std::ios::trunc);
  if (!out_) {
    const auto description = SinkDescription();
    if (!temp_path_.empty()) {
      std::error_code ignored;
      std::filesystem::remove(temp_path_, ignored);
      temp_path_.clear();
    }
    throw mgp::ValueException(fmt::format("Cannot open {} for writing", description));
  }
}

JsonWriter::~JsonWriter() {
  if (temp_path_.empty()) return;
  // Finish() was never reached, so the export failed: drop the partial file. Safe to remove unconditionally only
  // because the name is unique to this writer — a shared one would delete a concurrent export's in-flight file.
  out_.close();
  std::error_code ignored;
  std::filesystem::remove(temp_path_, ignored);
}

std::string JsonWriter::SinkDescription() const {
  if (temp_path_.empty()) return fmt::format("'{}'", *file_);
  return fmt::format("'{}' (temporary for '{}')", temp_path_, *file_);
}

void JsonWriter::Emit(std::string_view bytes) {
  if (out_.is_open()) {
    out_.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!out_) throw mgp::ValueException(fmt::format("Failed writing to {}", SinkDescription()));
  } else if (retain_) {
    payload_.append(bytes);
  }
}

void JsonWriter::EnterGroup(Group group) {
  if (group_ == group) return;
  if (group == Group::kNodes && group_ == Group::kRelationships) {
    throw mgp::ValueException("Cannot export a node after a relationship: the output groups them");
  }
  if (config_.format != JsonFormat::kJsonLines) {
    // JSON_LINES has no wrappers. The object shapes open "nodes" on first use and close it when relationships start,
    // so an export with no nodes at all still emits an empty node group.
    const bool keyed = config_.format == JsonFormat::kJsonIdAsKeys;
    if (group_ == Group::kNone) Emit(fmt::format("{{\"{}\":{}", kKeyNodes, keyed ? '{' : '['));
    if (group == Group::kRelationships) {
      Emit(fmt::format("{},\"{}\":{}", keyed ? '}' : ']', kKeyRels, keyed ? '{' : '['));
    }
  }
  group_ = group;
  group_has_elements_ = false;
  emitted_ids_.clear();
}

void JsonWriter::EmitElement(const Json &element) {
  const auto text = DumpElement(element);
  if (config_.format == JsonFormat::kJsonIdAsKeys) {
    auto id = element.at(kKeyId).get<std::string>();
    if (!emitted_ids_.insert(id).second) return;
    if (group_has_elements_) Emit(",");
    Emit(Json(id).dump());
    Emit(":");
  } else if (config_.format == JsonFormat::kJson) {
    if (group_has_elements_) Emit(",");
  } else if (wrote_any_) {
    Emit("\n");
  }
  Emit(text);
  wrote_any_ = true;
  group_has_elements_ = true;
}

void JsonWriter::AddNode(const mgp::Node &node) {
  // Fetched once and reused: mgp hands properties over as a deep copy of every value, so counting them separately
  // would materialize the whole property store a second time. The counter is inert to the write flags, so the fetch
  // happens even when the properties are not written.
  auto properties = node.Properties();
  property_count_ += properties.size();
  ++node_count_;
  // Built even with no sink: the value checks (enum, unknown SRID, unsupported type, invalid UTF-8) live on this
  // path, and skipping it made a sink-less call report success for a graph the very same call cannot export.
  EnterGroup(Group::kNodes);
  EmitElement(NodeToJson(node, config_.write_node_properties ? &properties : nullptr));
}

void JsonWriter::AddRelationship(const mgp::Relationship &relationship) {
  auto properties = relationship.Properties();
  // Only the relationship's own properties count; the inlined endpoints' do not.
  property_count_ += properties.size();
  ++relationship_count_;
  EnterGroup(Group::kRelationships);
  EmitElement(RelationshipToJson(relationship, config_.write_relationship_properties ? &properties : nullptr, config_));
}

std::string JsonWriter::Finish() && {
  // Normalising to the relationship group emits whatever wrappers were never opened, so an empty export still comes
  // out as {"nodes":[],"rels":[]} / {"nodes":{},"rels":{}} — and as "" under JSON_LINES.
  EnterGroup(Group::kRelationships);
  if (config_.format != JsonFormat::kJsonLines) {
    Emit(config_.format == JsonFormat::kJsonIdAsKeys ? "}}" : "]}");
  }

  if (out_.is_open()) {
    // Closed explicitly: anything still buffered is flushed here, and the destructor could not report a failure.
    // Without this a payload smaller than the stream buffer reports success on a full disk having written nothing.
    const auto description = SinkDescription();
    out_.close();
    if (!out_) throw mgp::ValueException(fmt::format("Failed writing to {}", description));
    if (!temp_path_.empty()) {
      std::error_code error;
      std::filesystem::rename(temp_path_, target_path_, error);
      if (error) throw mgp::ValueException(fmt::format("Cannot write '{}': {}", *file_, error.message()));
      temp_path_.clear();
    }
  }
  return std::move(payload_);
}

}  // namespace Export
