// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <unordered_map>

#include "helpers.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"
#include "version.hpp"

bool ValidateControlCharacter(const char *flagname, const std::string &value) {
  if (value.empty()) {
    printf("The argument '%s' cannot be empty\n", flagname);
    return false;
  }
  if (value.find('\n') != std::string::npos) {
    printf("The argument '%s' cannot contain a newline character\n", flagname);
    return false;
  }
  return true;
}

bool ValidateIdTypeOptions(const char *flagname, const std::string &value) {
  std::string upper = memgraph::utils::ToUpperCase(memgraph::utils::Trim(value));
  if (upper != "STRING" && upper != "INTEGER") {
    printf("Valid options for '%s' are: STRING/INTEGER\n", flagname);
    return false;
  }
  return true;
}

// Memgraph flags.
// NOTE: These flags must be identical as the flags in the main Memgraph binary.
// They are used to automatically load the same configuration as the main
// Memgraph binary so that the flags don't need to be specified when importing a
// CSV file on a correctly set-up Memgraph installation.
DEFINE_string(data_directory, "mg_data", "Path to directory in which to save all permanent data.");
DEFINE_bool(storage_properties_on_edges, false, "Controls whether relationships have properties.");

// CSV import flags.
DEFINE_string(array_delimiter, ";", "Delimiter between elements of array values.");
DEFINE_validator(array_delimiter, &ValidateControlCharacter);
DEFINE_string(delimiter, ",", "Delimiter between each field in the CSV.");
DEFINE_validator(delimiter, &ValidateControlCharacter);
DEFINE_string(quote, "\"", "Quotation character for data in the CSV. Cannot contain '\n'");
DEFINE_validator(quote, &ValidateControlCharacter);
DEFINE_bool(skip_duplicate_nodes, false, "Set to true to skip duplicate nodes instead of raising an error.");
DEFINE_bool(skip_bad_relationships, false,
            "Set to true to skip relationships that connect nodes that don't "
            "exist instead of raising an error.");
DEFINE_bool(ignore_empty_strings, false, "Set to true to treat empty strings as null values.");
DEFINE_bool(ignore_extra_columns, false, "Set to true to ignore columns that aren't specified in the header.");
DEFINE_bool(trim_strings, false,
            "Set to true to trim leading/trailing whitespace from all fields "
            "that are loaded from the CSV file.");
DEFINE_string(id_type, "STRING",
              "Which data type should be used to store the supplied node IDs. "
              "Possible options are: STRING/INTEGER");
DEFINE_validator(id_type, &ValidateIdTypeOptions);
// Arguments `--nodes` and `--relationships` can be input multiple times and are
// handled with custom parsing.
DEFINE_string(nodes, "",
              "Files that should be parsed for nodes. The CSV header will be loaded from "
              "the first supplied file, all other files supplied in a single flag will "
              "be treated as data files. Additional labels can be specified for the node "
              "files. The flag can be specified multiple times (useful for differently "
              "formatted node files). The format of this argument is: "
              "[<label>[:<label>]...=]<file>[,<file>][,<file>]...");
DEFINE_string(relationships, "",
              "Files that should be parsed for relationships. The CSV header will be "
              "loaded from the first supplied file, all other files supplied in a single "
              "flag will be treated as data files. The relationship type can be "
              "specified for the relationship files. The flag can be specified multiple "
              "times (useful for differently formatted relationship files). The format "
              "of this argument is: [<type>=]<file>[,<file>][,<file>]...");

std::vector<std::string> ParseRepeatedFlag(const std::string &flagname, int argc, char *argv[]) {
  std::vector<std::string> values;
  for (int i = 1; i < argc; ++i) {
    std::string flag(argv[i]);
    int matched_flag_dashes = 0;
    if (memgraph::utils::StartsWith(flag, "--" + flagname))
      matched_flag_dashes = 2;
    else if (memgraph::utils::StartsWith(flag, "-" + flagname))
      matched_flag_dashes = 1;
    // Get the value if we matched the flag.
    if (matched_flag_dashes != 0) {
      std::string value;
      auto maybe_value = flag.substr(flagname.size() + matched_flag_dashes);
      if (maybe_value.empty() && i + 1 < argc)
        value = argv[++i];
      else if (!maybe_value.empty() && maybe_value.front() == '=')
        value = maybe_value.substr(1);
      MG_ASSERT(!value.empty(), "The argument '{}' is required", flagname);
      values.push_back(value);
    }
  }
  return values;
}

// A field describing the CSV column.
struct Field {
  // Name of the field.
  std::string name;
  // Type of the values under this field.
  std::string type;
};

// A node ID from CSV format.
struct NodeId {
  std::string id;
  // Group/space of IDs. ID must be unique in a single group.
  std::string id_space;
};

bool operator==(const NodeId &a, const NodeId &b) { return a.id == b.id && a.id_space == b.id_space; }

std::ostream &operator<<(std::ostream &stream, const NodeId &node_id) {
  if (!node_id.id_space.empty()) {
    return stream << node_id.id << "(" << node_id.id_space << ")";
  } else {
    return stream << node_id.id;
  }
}

namespace std {

template <>
struct hash<NodeId> {
  size_t operator()(const NodeId &node_id) const {
    size_t id_hash = std::hash<std::string>{}(node_id.id);
    size_t id_space_hash = std::hash<std::string>{}(node_id.id_space);
    return id_hash ^ (id_space_hash << 1UL);
  }
};

}  // namespace std

// Exception used to indicate that something went wrong during data loading.
class LoadException : public memgraph::utils::BasicException {
 public:
  using memgraph::utils::BasicException::BasicException;
};

enum class CsvParserState {
  INITIAL_FIELD,
  NEXT_FIELD,
  QUOTING,
  NOT_QUOTING,
  EXPECT_DELIMITER,
};

bool SubstringStartsWith(const std::string_view str, size_t pos, const std::string_view what) {
  return memgraph::utils::StartsWith(memgraph::utils::Substr(str, pos), what);
}

/// This function reads a row from a CSV stream.
///
/// Each CSV field must be divided using the `delimiter` and each CSV field can
/// either be quoted or unquoted. When the field is quoted, the first and last
/// character in the field *must* be the quote character. If the field isn't
/// quoted, and a quote character appears in it, it is treated as a regular
/// character. If a quote character appears inside a quoted string then the
/// quote character must be doubled in order to escape it. Line feeds and
/// carriage returns are ignored in the CSV file, also, the file can't contain a
/// NULL character.
///
/// The function uses the same logic as the standard Python CSV parser. The data
/// is parsed in the same way as the following snippet:
/// ```
/// import csv
/// for row in csv.reader(stream, strict=True):
///     # process `row`
/// ```
///
/// Python uses 'excel' as the default dialect when parsing CSV files and the
/// default settings for the CSV parser are:
///  - delimiter: ','
///  - doublequote: True
///  - escapechar: None
///  - lineterminator: '\r\n'
///  - quotechar: '"'
///  - skipinitialspace: False
///
/// The above snippet can be expanded to:
/// ```
/// import csv
/// for row in csv.reader(stream, delimiter=',', doublequote=True,
///                       escapechar=None, lineterminator='\r\n',
///                       quotechar='"', skipinitialspace=False,
///                       strict=True):
///     # process `row`
/// ```
///
/// For more information about the meaning of the above values, see:
/// https://docs.python.org/3/library/csv.html#csv.Dialect
///
/// @throw LoadException
std::pair<std::vector<std::string>, uint64_t> ReadRow(std::istream &stream) {
  std::vector<std::string> row;
  std::string column;
  uint64_t lines_count = 0;

  auto state = CsvParserState::INITIAL_FIELD;

  do {
    std::string line;
    if (!std::getline(stream, line)) {
      // The whole file was processed.
      break;
    }
    ++lines_count;

    for (size_t i = 0; i < line.size(); ++i) {
      auto c = line[i];

      // Line feeds and carriage returns are ignored in CSVs.
      if (c == '\n' || c == '\r') continue;
      // Null bytes aren't allowed in CSVs.
      if (c == '\0') throw LoadException("Line contains NULL byte");

      switch (state) {
        case CsvParserState::INITIAL_FIELD:
        case CsvParserState::NEXT_FIELD: {
          if (SubstringStartsWith(line, i, FLAGS_quote)) {
            // The current field is a quoted field.
            state = CsvParserState::QUOTING;
            i += FLAGS_quote.size() - 1;
          } else if (SubstringStartsWith(line, i, FLAGS_delimiter)) {
            // The current field has an empty value.
            row.emplace_back("");
            state = CsvParserState::NEXT_FIELD;
            i += FLAGS_delimiter.size() - 1;
          } else {
            // The current field is a regular field.
            column.push_back(c);
            state = CsvParserState::NOT_QUOTING;
          }
          break;
        }
        case CsvParserState::QUOTING: {
          auto quote_now = SubstringStartsWith(line, i, FLAGS_quote);
          auto quote_next = SubstringStartsWith(line, i + FLAGS_quote.size(), FLAGS_quote);
          if (quote_now && quote_next) {
            // This is an escaped quote character.
            column += FLAGS_quote;
            i += FLAGS_quote.size() * 2 - 1;
          } else if (quote_now && !quote_next) {
            // This is the end of the quoted field.
            row.emplace_back(std::move(column));
            state = CsvParserState::EXPECT_DELIMITER;
            i += FLAGS_quote.size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::NOT_QUOTING: {
          if (SubstringStartsWith(line, i, FLAGS_delimiter)) {
            row.emplace_back(std::move(column));
            state = CsvParserState::NEXT_FIELD;
            i += FLAGS_delimiter.size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::EXPECT_DELIMITER: {
          if (SubstringStartsWith(line, i, FLAGS_delimiter)) {
            state = CsvParserState::NEXT_FIELD;
            i += FLAGS_delimiter.size() - 1;
          } else {
            throw LoadException("Expected '{}' after '{}', but got '{}'", FLAGS_delimiter, FLAGS_quote, c);
          }
          break;
        }
      }
    }
  } while (state == CsvParserState::QUOTING);

  switch (state) {
    case CsvParserState::INITIAL_FIELD: {
      break;
    }
    case CsvParserState::NEXT_FIELD: {
      row.emplace_back(std::move(column));
      break;
    }
    case CsvParserState::QUOTING: {
      throw LoadException(
          "There is no more data left to load while inside a quoted string. "
          "Did you forget to close the quote?");
      break;
    }
    case CsvParserState::NOT_QUOTING: {
      row.emplace_back(std::move(column));
      break;
    }
    case CsvParserState::EXPECT_DELIMITER: {
      break;
    }
  }

  if (FLAGS_trim_strings) {
    std::transform(std::begin(row), std::end(row), std::begin(row),
                   [](const auto &item) { return memgraph::utils::Trim(item); });
  }

  return {std::move(row), lines_count};
}

/// @throw LoadException
std::pair<std::vector<Field>, uint64_t> ReadHeader(std::istream &stream) {
  auto [row, lines_count] = ReadRow(stream);
  std::vector<Field> fields;
  fields.reserve(row.size());
  for (const auto &value : row) {
    auto name_and_type = memgraph::utils::Split(value, ":");
    if (name_and_type.size() != 1U && name_and_type.size() != 2U)
      throw LoadException(
          "Expected a name and optionally a type, got '{}'. Did you specify a "
          "correct CSV delimiter?",
          value);
    auto name = name_and_type[0];
    // When type is missing, default is string.
    std::string type("string");
    if (name_and_type.size() == 2U) type = memgraph::utils::Trim(name_and_type[1]);
    fields.push_back(Field{name, type});
  }
  return {std::move(fields), lines_count};
}

/// @throw LoadException
int64_t StringToInt(const std::string &value) {
  try {
    return memgraph::utils::ParseInt(value);
  } catch (...) {
    throw LoadException("'{}' isn't a valid integer", value);
  }
}

/// @throw LoadException
double StringToDouble(const std::string &value) {
  try {
    return memgraph::utils::ParseDouble(value);
  } catch (...) {
    throw LoadException("'{}' isn't a valid floating-point value", value);
  }
}

/// @throw LoadException
memgraph::storage::PropertyValue StringToValue(const std::string &str, const std::string &type) {
  if (FLAGS_ignore_empty_strings && str.empty()) return {};
  auto convert = [](const auto &str, const auto &type) {
    if (type == "integer" || type == "int" || type == "long" || type == "byte" || type == "short") {
      return memgraph::storage::PropertyValue(StringToInt(str));
    } else if (type == "float" || type == "double") {
      return memgraph::storage::PropertyValue(StringToDouble(str));
    } else if (type == "boolean" || type == "bool") {
      if (memgraph::utils::ToLowerCase(str) == "true") {
        return memgraph::storage::PropertyValue(true);
      } else {
        return memgraph::storage::PropertyValue(false);
      }
    } else if (type == "char" || type == "string") {
      return memgraph::storage::PropertyValue(str);
    }
    throw LoadException("Unexpected type: {}", type);
  };
  // Type *not* ending with '[]', signifies regular value.
  if (!memgraph::utils::EndsWith(type, "[]")) return convert(str, type);
  // Otherwise, we have an array type.
  auto elem_type = type.substr(0, type.size() - 2);
  auto elems = memgraph::utils::Split(str, FLAGS_array_delimiter);
  std::vector<memgraph::storage::PropertyValue> array;
  array.reserve(elems.size());
  for (const auto &elem : elems) {
    array.push_back(convert(elem, elem_type));
  }
  return memgraph::storage::PropertyValue(std::move(array));
}

/// @throw LoadException
std::string GetIdSpace(const std::string &type) {
  // The format of this field is as follows:
  // [START_|END_]ID[(<id_space>)]
  std::regex format(R"(^(START_|END_)?ID(\(([^\(\)]+)\))?$)", std::regex::extended);
  std::smatch res;
  if (!std::regex_match(type, res, format))
    throw LoadException(
        "Expected the ID field to look like '[START_|END_]ID[(<id_space>)]', "
        "but got '{}' instead",
        type);
  MG_ASSERT(res.size() == 4, "Invalid regex match result!");
  return res[3];
}

/// @throw LoadException
void ProcessNodeRow(memgraph::storage::Storage *store, const std::vector<std::string> &row,
                    const std::vector<Field> &fields, const std::vector<std::string> &additional_labels,
                    std::unordered_map<NodeId, memgraph::storage::Gid> *node_id_map) {
  std::optional<NodeId> id;
  auto acc = store->Access();
  auto node = acc->CreateVertex();
  for (size_t i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    const auto &value = row[i];
    if (memgraph::utils::StartsWith(field.type, "ID")) {
      if (id) throw LoadException("Only one node ID must be specified");
      if (FLAGS_id_type == "INTEGER") {
        // Call `StringToInt` to verify that the ID is a valid integer.
        StringToInt(value);
      }
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map->find(node_id);
      if (it != node_id_map->end()) {
        if (FLAGS_skip_duplicate_nodes) {
          spdlog::warn(memgraph::utils::MessageWithLink("Skipping duplicate node with ID '{}'.", node_id,
                                                        "https://memgr.ph/csv-import-tool"));
          return;
        } else {
          throw LoadException("Node with ID '{}' already exists", node_id);
        }
      }
      node_id_map->emplace(node_id, node.Gid());
      if (!field.name.empty()) {
        memgraph::storage::PropertyValue pv_id;
        if (FLAGS_id_type == "INTEGER") {
          pv_id = memgraph::storage::PropertyValue(StringToInt(node_id.id));
        } else {
          pv_id = memgraph::storage::PropertyValue(node_id.id);
        }
        auto old_node_property = node.SetProperty(acc->NameToProperty(field.name), pv_id);
        if (!old_node_property.HasValue()) throw LoadException("Couldn't add property '{}' to the node", field.name);
        if (!old_node_property->IsNull()) throw LoadException("The property '{}' already exists", field.name);
      }
      id = node_id;
    } else if (field.type == "LABEL") {
      for (const auto &label : memgraph::utils::Split(value, FLAGS_array_delimiter)) {
        auto node_label = node.AddLabel(acc->NameToLabel(label));
        if (!node_label.HasValue()) throw LoadException("Couldn't add label '{}' to the node", label);
        if (!*node_label) throw LoadException("The label '{}' already exists", label);
      }
    } else if (field.type != "IGNORE") {
      auto old_node_property = node.SetProperty(acc->NameToProperty(field.name), StringToValue(value, field.type));
      if (!old_node_property.HasValue()) throw LoadException("Couldn't add property '{}' to the node", field.name);
      if (!old_node_property->IsNull()) throw LoadException("The property '{}' already exists", field.name);
    }
  }
  for (const auto &label : additional_labels) {
    auto node_label = node.AddLabel(acc->NameToLabel(label));
    if (!node_label.HasValue()) throw LoadException("Couldn't add label '{}' to the node", label);
    if (!*node_label) throw LoadException("The label '{}' already exists", label);
  }
  if (acc->Commit().HasError()) throw LoadException("Couldn't store the node");
}

void ProcessNodes(memgraph::storage::Storage *store, const std::string &nodes_path,
                  std::optional<std::vector<Field>> *header,
                  std::unordered_map<NodeId, memgraph::storage::Gid> *node_id_map,
                  const std::vector<std::string> &additional_labels) {
  std::ifstream nodes_file(nodes_path);
  MG_ASSERT(nodes_file, "Unable to open '{}'", nodes_path);
  uint64_t row_number = 1;
  try {
    if (!*header) {
      auto [fields, header_lines] = ReadHeader(nodes_file);
      row_number += header_lines;
      header->emplace(std::move(fields));
    }
    while (true) {
      auto [row, lines_count] = ReadRow(nodes_file);
      if (lines_count == 0) break;
      if ((!FLAGS_ignore_extra_columns && row.size() != (*header)->size()) ||
          (FLAGS_ignore_extra_columns && row.size() < (*header)->size()))
        throw LoadException(
            "Expected as many values as there are header fields (found {}, "
            "expected {})",
            row.size(), (*header)->size());
      if (row.size() > (*header)->size()) {
        row.resize((*header)->size());
      }
      ProcessNodeRow(store, row, **header, additional_labels, node_id_map);
      row_number += lines_count;
    }
  } catch (const LoadException &e) {
    LOG_FATAL("Couldn't process row {} of '{}' because of: {}", row_number, nodes_path, e.what());
  }
}

/// @throw LoadException
void ProcessRelationshipsRow(memgraph::storage::Storage *store, const std::vector<Field> &fields,
                             const std::vector<std::string> &row, std::optional<std::string> relationship_type,
                             const std::unordered_map<NodeId, memgraph::storage::Gid> &node_id_map) {
  std::optional<memgraph::storage::Gid> start_id;
  std::optional<memgraph::storage::Gid> end_id;
  std::map<std::string, memgraph::storage::PropertyValue> properties;
  for (size_t i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    const auto &value = row[i];
    if (memgraph::utils::StartsWith(field.type, "START_ID")) {
      if (start_id) throw LoadException("Only one node ID must be specified");
      if (FLAGS_id_type == "INTEGER") {
        // Call `StringToInt` to verify that the START_ID is a valid integer.
        StringToInt(value);
      }
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map.find(node_id);
      if (it == node_id_map.end()) {
        if (FLAGS_skip_bad_relationships) {
          spdlog::warn(memgraph::utils::MessageWithLink("Skipping bad relationship with START_ID '{}'.", node_id,
                                                        "https://memgr.ph/csv-import-tool"));
          return;
        } else {
          throw LoadException("Node with ID '{}' does not exist", node_id);
        }
      }
      start_id = it->second;
    } else if (memgraph::utils::StartsWith(field.type, "END_ID")) {
      if (end_id) throw LoadException("Only one node ID must be specified");
      if (FLAGS_id_type == "INTEGER") {
        // Call `StringToInt` to verify that the END_ID is a valid integer.
        StringToInt(value);
      }
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map.find(node_id);
      if (it == node_id_map.end()) {
        if (FLAGS_skip_bad_relationships) {
          spdlog::warn(memgraph::utils::MessageWithLink("Skipping bad relationship with END_ID '{}'.", node_id,
                                                        "https://memgr.ph/csv-import-tool"));
          return;
        } else {
          throw LoadException("Node with ID '{}' does not exist", node_id);
        }
      }
      end_id = it->second;
    } else if (field.type == "TYPE") {
      if (relationship_type) throw LoadException("Only one relationship TYPE must be specified");
      relationship_type = value;
    } else if (field.type != "IGNORE") {
      auto [it, inserted] = properties.emplace(field.name, StringToValue(value, field.type));
      if (!inserted) throw LoadException("The property '{}' already exists", field.name);
    }
  }
  if (!start_id) throw LoadException("START_ID must be set");
  if (!end_id) throw LoadException("END_ID must be set");
  if (!relationship_type) throw LoadException("Relationship TYPE must be set");

  auto acc = store->Access();
  auto from_node = acc->FindVertex(*start_id, memgraph::storage::View::NEW);
  if (!from_node) throw LoadException("From node must be in the storage");
  auto to_node = acc->FindVertex(*end_id, memgraph::storage::View::NEW);
  if (!to_node) throw LoadException("To node must be in the storage");

  auto relationship = acc->CreateEdge(&from_node.value(), &to_node.value(), acc->NameToEdgeType(*relationship_type));
  if (!relationship.HasValue()) throw LoadException("Couldn't create the relationship");

  for (const auto &property : properties) {
    auto ret = relationship.GetValue().SetProperty(acc->NameToProperty(property.first), property.second);
    if (!ret.HasValue()) {
      if (ret.GetError() != memgraph::storage::Error::PROPERTIES_DISABLED) {
        throw LoadException("Couldn't add property '{}' to the relationship", property.first);
      } else {
        throw LoadException(
            "Couldn't add property '{}' to the relationship because properties "
            "on edges are disabled",
            property.first);
      }
    }
  }

  if (acc->Commit().HasError()) throw LoadException("Couldn't store the relationship");
}

void ProcessRelationships(memgraph::storage::Storage *store, const std::string &relationships_path,
                          const std::optional<std::string> &relationship_type,
                          std::optional<std::vector<Field>> *header,
                          const std::unordered_map<NodeId, memgraph::storage::Gid> &node_id_map) {
  std::ifstream relationships_file(relationships_path);
  MG_ASSERT(relationships_file, "Unable to open '{}'", relationships_path);
  uint64_t row_number = 1;
  try {
    if (!*header) {
      auto [fields, header_lines] = ReadHeader(relationships_file);
      row_number += header_lines;
      header->emplace(std::move(fields));
    }
    while (true) {
      auto [row, lines_count] = ReadRow(relationships_file);
      if (lines_count == 0) break;
      if ((!FLAGS_ignore_extra_columns && row.size() != (*header)->size()) ||
          (FLAGS_ignore_extra_columns && row.size() < (*header)->size()))
        throw LoadException(
            "Expected as many values as there are header fields (found {}, "
            "expected {})",
            row.size(), (*header)->size());
      if (row.size() > (*header)->size()) {
        row.resize((*header)->size());
      }
      ProcessRelationshipsRow(store, **header, row, relationship_type, node_id_map);
      row_number += lines_count;
    }
  } catch (const LoadException &e) {
    LOG_FATAL("Couldn't process row {} of '{}' because of: {}", row_number, relationships_path, e.what());
  }
}

struct NodesArgument {
  // List of all files that have should be processed for nodes.
  std::vector<std::string> nodes;
  // List of all additional labels that should be added to the nodes.
  std::vector<std::string> additional_labels;
};

NodesArgument ParseNodesArgument(const std::string &value) {
  // The format of this argument is as follows:
  // [<label>[:<label>]...=]<file>[,<file>][,<file>]...

  std::vector<std::string> nodes;
  std::vector<std::string> additional_labels;

  size_t pos_nodes = 0;
  auto pos_equal = value.find('=');
  if (pos_equal != std::string::npos) {
    // We have additional labels.
    additional_labels = memgraph::utils::Split(value.substr(0, pos_equal), ":");
    pos_nodes = pos_equal + 1;
  }

  nodes = memgraph::utils::Split(value.substr(pos_nodes), ",");

  return {std::move(nodes), std::move(additional_labels)};
}

struct RelationshipsArgument {
  // List of all files that have should be processed for relationships.
  std::vector<std::string> relationships;
  // Optional type of the relationships.
  std::optional<std::string> type;
};

RelationshipsArgument ParseRelationshipsArgument(const std::string &value) {
  // The format of this argument is as follows:
  // [<type>=]<file>[,<file>][,<file>]...

  std::vector<std::string> relationships;
  std::optional<std::string> type;

  size_t pos_relationships = 0;
  auto pos_equal = value.find('=');
  if (pos_equal != std::string::npos) {
    // The type has been specified.
    type = value.substr(0, pos_equal);
    pos_relationships = pos_equal + 1;
  }

  relationships = memgraph::utils::Split(value.substr(pos_relationships), ",");

  return {std::move(relationships), std::move(type)};
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage("Create a Memgraph recovery snapshot file from CSV.");
  gflags::SetVersionString(version_string);

  auto nodes = ParseRepeatedFlag("nodes", argc, argv);
  auto relationships = ParseRepeatedFlag("relationships", argc, argv);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("mg_import_csv");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  MG_ASSERT(!nodes.empty(), "The --nodes flag is required!");

  {
    std::string upper = memgraph::utils::ToUpperCase(memgraph::utils::Trim(FLAGS_id_type));
    FLAGS_id_type = upper;
  }

  std::unordered_map<NodeId, memgraph::storage::Gid> node_id_map;
  auto store = std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{

      .items = {.properties_on_edges = FLAGS_storage_properties_on_edges},
      .durability = {.storage_directory = FLAGS_data_directory,
                     .recover_on_startup = false,
                     .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED,
                     .snapshot_on_exit = true},
  });

  memgraph::utils::Timer load_timer;

  // Process all nodes files.
  for (const auto &value : nodes) {
    auto [files, additional_labels] = ParseNodesArgument(value);
    std::optional<std::vector<Field>> header;
    for (const auto &nodes_file : files) {
      spdlog::info("Loading {}", nodes_file);
      ProcessNodes(store.get(), nodes_file, &header, &node_id_map, additional_labels);
    }
  }

  // Process all relationships files.
  for (const auto &value : relationships) {
    auto [files, type] = ParseRelationshipsArgument(value);
    std::optional<std::vector<Field>> header;
    for (const auto &relationships_file : files) {
      spdlog::info("Loading {}", relationships_file);
      ProcessRelationships(store.get(), relationships_file, type, &header, node_id_map);
    }
  }

  double load_sec = load_timer.Elapsed().count();
  spdlog::info("Loaded all data in {:.3f}s", load_sec);

  // The snapshot will be created in the storage destructor.

  return 0;
}
