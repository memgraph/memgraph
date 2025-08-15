// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <filesystem>

#include <nlohmann/json.hpp>
#include "query/exceptions.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/string.hpp"

namespace r = ranges;

namespace memgraph::storage {

std::string ToLowerCasePreservingBooleanOperators(std::string_view input) {
  if (input.empty()) return {};

  std::string result;
  result.reserve(input.length());

  const auto *it = input.cbegin();
  while (it != input.cend()) {
    if (std::isspace(*it)) {
      result += *it++;
      continue;
    }

    const auto *word_start = it;
    it = r::find_if(word_start, input.cend(), [](char c) { return std::isspace(c); });

    // Extract the word
    auto word = input.substr(word_start - input.cbegin(), it - word_start);
    auto uppercase_word = utils::ToUpperCase(word);

    // Check if it's a boolean operator (case-insensitive)
    if (uppercase_word == kBooleanAnd || uppercase_word == kBooleanOr || uppercase_word == kBooleanNot) {
      // Preserve the boolean operator in uppercase
      result += uppercase_word;
    } else {
      result += utils::ToLowerCase(word);
    }
  }

  return result;
}

std::string MakeIndexPath(const std::string &base_path, std::string_view index_name) {
  return (std::filesystem::path(base_path) / index_name).string();
}

nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties,
                                   NameIdMapper *name_id_mapper) {
  // Property types that are indexed in Tantivy are Bool, Int, Double, and String.
  nlohmann::json serialized_properties = nlohmann::json::value_t::object;
  for (const auto &[prop_id, prop_value] : properties) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        serialized_properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueBool();
        break;
      case PropertyValue::Type::Int:
        serialized_properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueInt();
        break;
      case PropertyValue::Type::Double:
        serialized_properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueDouble();
        break;
      case PropertyValue::Type::String:
        serialized_properties[name_id_mapper->IdToName(prop_id.AsUint())] = prop_value.ValueString();
        break;
      case PropertyValue::Type::Null:
      case PropertyValue::Type::List:
      case PropertyValue::Type::Map:
      case PropertyValue::Type::TemporalData:
      case PropertyValue::Type::ZonedTemporalData:
      default:
        continue;
    }
  }
  return serialized_properties;
}

std::string StringifyProperties(const std::map<PropertyId, PropertyValue> &properties) {
  // Property types that are indexed in Tantivy are Bool, Int, Double, and String.
  std::vector<std::string> indexable_properties_as_string;
  for (const auto &[_, prop_value] : properties) {
    switch (prop_value.type()) {
      case PropertyValue::Type::Bool:
        indexable_properties_as_string.emplace_back(prop_value.ValueBool() ? "true" : "false");
        break;
      case PropertyValue::Type::Int:
        indexable_properties_as_string.push_back(std::to_string(prop_value.ValueInt()));
        break;
      case PropertyValue::Type::Double:
        indexable_properties_as_string.push_back(std::to_string(prop_value.ValueDouble()));
        break;
      case PropertyValue::Type::String:
        indexable_properties_as_string.push_back(prop_value.ValueString());
        break;
      // NOTE: As the following types aren't indexed in Tantivy, they don't appear in the property value string either.
      case PropertyValue::Type::Null:
      case PropertyValue::Type::List:
      case PropertyValue::Type::Map:
      case PropertyValue::Type::TemporalData:
      case PropertyValue::Type::ZonedTemporalData:
      default:
        continue;
    }
  }
  return utils::Join(indexable_properties_as_string, " ");
}

void TrackTextIndexChange(TextIndexChangeCollector &collector, std::span<TextIndexData *> indices, Vertex *vertex,
                          TextIndexOp op) {
  if (!vertex) return;
  for (auto *idx : indices) {
    auto &entry = collector[idx];
    if (op == TextIndexOp::ADD) {
      entry.to_remove_.erase(vertex);
      entry.to_add_.insert(vertex);
    } else if (op == TextIndexOp::UPDATE) {
      // On update we have to firstly remove the vertex from index and then add it back
      entry.to_remove_.insert(vertex);
      entry.to_add_.insert(vertex);
    } else {  // REMOVE
      entry.to_add_.erase(vertex);
      entry.to_remove_.insert(vertex);
    }
  }
}

mgcxx::text_search::SearchOutput SearchGivenProperties(const std::string &search_query,
                                                       mgcxx::text_search::Context &context) {
  try {
    return mgcxx::text_search::search(
        context, mgcxx::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

mgcxx::text_search::SearchOutput RegexSearch(const std::string &search_query, mgcxx::text_search::Context &context) {
  try {
    return mgcxx::text_search::regex_search(
        context, mgcxx::text_search::SearchInput{
                     .search_fields = {"all"}, .search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

mgcxx::text_search::SearchOutput SearchAllProperties(const std::string &search_query,
                                                     mgcxx::text_search::Context &context) {
  try {
    return mgcxx::text_search::search(
        context, mgcxx::text_search::SearchInput{
                     .search_fields = {"all"}, .search_query = search_query, .return_fields = {"metadata"}});
  } catch (const std::exception &e) {
    throw query::TextSearchException("Tantivy error: {}", e.what());
  }
}

}  // namespace memgraph::storage
