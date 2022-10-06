// Copyright 2022 Memgraph Ltd.
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

#include "query/db_accessor.hpp"
#include "readline.hpp"
#include "timer.hpp"

inline bool AskYesNo(const std::string &prompt) {
  while (auto line = ReadLine(prompt + " (y/n) ")) {
    if (*line == "y" || *line == "Y") return true;
    if (*line == "n" || *line == "N") return false;
  }
  return false;
}

// Dummy DbAccessor which forwards user input for various vertex counts.
class InteractiveDbAccessor {
 public:
  InteractiveDbAccessor(memgraph::query::DbAccessor *dba, int64_t vertices_count, Timer &timer)
      : dba_(dba), vertices_count_(vertices_count), timer_(timer) {}

  auto NameToLabel(const std::string &name) { return dba_->NameToLabel(name); }
  auto NameToProperty(const std::string &name) { return dba_->NameToProperty(name); }
  auto NameToEdgeType(const std::string &name) { return dba_->NameToEdgeType(name); }

  int64_t VerticesCount() { return vertices_count_; }

  int64_t VerticesCount(memgraph::storage::LabelId label_id) {
    auto label = dba_->LabelToName(label_id);
    if (label_vertex_count_.find(label) == label_vertex_count_.end()) {
      label_vertex_count_[label] = ReadVertexCount("label '" + label + "'");
    }
    return label_vertex_count_.at(label);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto key = std::make_pair(label, property);
    if (label_property_vertex_count_.find(key) == label_property_vertex_count_.end()) {
      label_property_vertex_count_[key] = ReadVertexCount("label '" + label + "' and property '" + property + "'");
    }
    return label_property_vertex_count_.at(key);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id,
                        const memgraph::storage::PropertyValue &value) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto label_prop = std::make_pair(label, property);
    if (label_property_index_.find(label_prop) == label_property_index_.end()) {
      return 0;
    }
    auto &value_vertex_count = property_value_vertex_count_[label_prop];
    if (value_vertex_count.find(value) == value_vertex_count.end()) {
      std::stringstream ss;
      ss << value;
      int64_t count = ReadVertexCount("label '" + label + "' and property '" + property + "' value '" + ss.str() + "'");
      value_vertex_count[value] = count;
    }
    return value_vertex_count.at(value);
  }

  int64_t VerticesCount(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id,
                        const std::optional<memgraph::utils::Bound<memgraph::storage::PropertyValue>> lower,
                        const std::optional<memgraph::utils::Bound<memgraph::storage::PropertyValue>> upper) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    std::stringstream range_string;
    if (lower) {
      range_string << (lower->IsInclusive() ? "[" : "(") << lower->value() << (upper ? "," : ", inf)");
    } else {
      range_string << "(-inf, ";
    }
    if (upper) {
      range_string << upper->value() << (upper->IsInclusive() ? "]" : ")");
    }
    return ReadVertexCount("label '" + label + "' and property '" + property + "' in range " + range_string.str());
  }

  bool LabelIndexExists(memgraph::storage::LabelId label) { return true; }

  bool LabelPropertyIndexExists(memgraph::storage::LabelId label_id, memgraph::storage::PropertyId property_id) {
    auto label = dba_->LabelToName(label_id);
    auto property = dba_->PropertyToName(property_id);
    auto key = std::make_pair(label, property);
    if (label_property_index_.find(key) == label_property_index_.end()) {
      bool resp = timer_.WithPause(
          [&label, &property]() { return AskYesNo("Index for ':" + label + "(" + property + ")' exists:"); });
      label_property_index_[key] = resp;
    }
    return label_property_index_.at(key);
  }

  // Save the cached vertex counts to a stream.
  void Save(std::ostream &out) {
    out << "vertex-count " << vertices_count_ << std::endl;
    out << "label-index-count " << label_vertex_count_.size() << std::endl;
    for (const auto &label_count : label_vertex_count_) {
      out << "  " << label_count.first << " " << label_count.second << std::endl;
    }
    auto save_label_prop_map = [&](const auto &name, const auto &label_prop_map) {
      out << name << " " << label_prop_map.size() << std::endl;
      for (const auto &label_prop : label_prop_map) {
        out << "  " << label_prop.first.first << " " << label_prop.first.second << " " << label_prop.second
            << std::endl;
      }
    };
    save_label_prop_map("label-property-index-exists", label_property_index_);
    save_label_prop_map("label-property-index-count", label_property_vertex_count_);
    out << "label-property-value-index-count " << property_value_vertex_count_.size() << std::endl;
    for (const auto &prop_value_count : property_value_vertex_count_) {
      out << "  " << prop_value_count.first.first << " " << prop_value_count.first.second << " "
          << prop_value_count.second.size() << std::endl;
      for (const auto &value_count : prop_value_count.second) {
        const auto &value = value_count.first;
        out << "    " << value.type() << " " << value << " " << value_count.second << std::endl;
      }
    }
  }

  // Load the cached vertex counts from a stream.
  // If loading fails, raises memgraph::utils::BasicException.
  void Load(std::istream &in) {
    auto load_named_size = [&](const auto &name) {
      int size;
      in.ignore(std::numeric_limits<std::streamsize>::max(), ' ') >> size;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load {}", name);
      }
      SPDLOG_INFO("Load {} {}", name, size);
      return size;
    };
    vertices_count_ = load_named_size("vertex-count");
    int label_vertex_size = load_named_size("label-index-count");
    for (int i = 0; i < label_vertex_size; ++i) {
      std::string label;
      int64_t count;
      in >> label >> count;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load label count");
      }
      label_vertex_count_[label] = count;
      SPDLOG_INFO("Load {} {}", label, count);
    }
    auto load_label_prop_map = [&](const auto &name, auto &label_prop_map) {
      int size = load_named_size(name);
      for (int i = 0; i < size; ++i) {
        std::string label;
        std::string property;
        in >> label >> property;
        auto &mapped = label_prop_map[std::make_pair(label, property)];
        in >> mapped;
        if (in.fail()) {
          throw memgraph::utils::BasicException("Unable to load label property");
        }
        SPDLOG_INFO("Load {} {} {}", label, property, mapped);
      }
    };
    load_label_prop_map("label-property-index-exists", label_property_index_);
    load_label_prop_map("label-property-index-count", label_property_vertex_count_);
    int label_property_value_index_size = load_named_size("label-property-value-index-count");
    for (int i = 0; i < label_property_value_index_size; ++i) {
      std::string label;
      std::string property;
      int64_t value_count;
      in >> label >> property >> value_count;
      if (in.fail()) {
        throw memgraph::utils::BasicException("Unable to load label property value");
      }
      SPDLOG_INFO("Load {} {} {}", label, property, value_count);
      for (int v = 0; v < value_count; ++v) {
        auto value = LoadPropertyValue(in);
        int64_t count;
        in >> count;
        if (in.fail()) {
          throw memgraph::utils::BasicException("Unable to load label property value");
        }
        SPDLOG_INFO("Load {} {} {}", value.type(), value, count);
        property_value_vertex_count_[std::make_pair(label, property)][value] = count;
      }
    }
  }

 private:
  typedef std::pair<std::string, std::string> LabelPropertyKey;

  memgraph::query::DbAccessor *dba_;
  int64_t vertices_count_;
  Timer &timer_;
  std::map<std::string, int64_t> label_vertex_count_;
  std::map<std::pair<std::string, std::string>, int64_t> label_property_vertex_count_;
  std::map<std::pair<std::string, std::string>, bool> label_property_index_;
  std::map<std::pair<std::string, std::string>, std::map<memgraph::storage::PropertyValue, int64_t>>
      property_value_vertex_count_;
  // TODO: Cache faked index counts by range.

  int64_t ReadVertexCount(const std::string &message) const {
    return timer_.WithPause([&message]() { return ReadInt("Vertices with " + message + ": "); });
  }

  memgraph::storage::PropertyValue LoadPropertyValue(std::istream &in) {
    std::string type;
    in >> type;
    if (type == "bool") {
      return LoadPropertyValue<bool>(in);
    } else if (type == "int") {
      return LoadPropertyValue<int64_t>(in);
    } else if (type == "double") {
      return LoadPropertyValue<double>(in);
    } else if (type == "string") {
      return LoadPropertyValue<std::string>(in);
    } else {
      throw memgraph::utils::BasicException("Unable to read type '{}'", type);
    }
  }

  template <typename T>
  memgraph::storage::PropertyValue LoadPropertyValue(std::istream &in) {
    T val;
    in >> val;
    return memgraph::storage::PropertyValue(val);
  }
};
