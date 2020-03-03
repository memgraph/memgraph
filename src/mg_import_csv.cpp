#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "helpers.hpp"
#include "storage/v2/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"
#include "version.hpp"

bool ValidateNotNewline(const char *flagname, const std::string &value) {
  auto has_no_newline = value.find('\n') == std::string::npos;
  if (!has_no_newline) {
    printf("The argument '%s' cannot contain newline character\n", flagname);
  }
  return has_no_newline;
}

bool ValidateNoWhitespace(const char *flagname, const std::string &value) {
  auto trimmed = utils::Trim(value);
  if (trimmed.empty() && !value.empty()) {
    printf("The argument '%s' cannot be only whitespace\n", flagname);
    return false;
  } else if (!trimmed.empty()) {
    for (auto c : trimmed) {
      if (std::isspace(c)) {
        printf("The argument '%s' cannot contain whitespace\n", flagname);
        return false;
      }
    }
  }
  return true;
}

// Memgraph flags.
// NOTE: These flags must be identical as the flags in the main Memgraph binary.
// They are used to automatically load the same configuration as the main
// Memgraph binary so that the flags don't need to be specified when importing a
// CSV file on a correctly set-up Memgraph installation.
DEFINE_string(data_directory, "mg_data",
              "Path to directory in which to save all permanent data.");
DEFINE_bool(storage_properties_on_edges, false,
            "Controls whether relationships have properties.");

// CSV import flags.
DEFINE_string(array_delimiter, ";",
              "Delimiter between elements of array values.");
DEFINE_string(delimiter, ",", "Delimiter between each field in the CSV.");
DEFINE_string(quote, "\"",
              "Quotation character for data in the CSV. Cannot contain '\n'");
DEFINE_validator(quote, &ValidateNotNewline);
DEFINE_bool(skip_duplicate_nodes, false,
            "Set to true to skip duplicate nodes instead of raising an error.");
// Arguments `--nodes` and `--relationships` can be input multiple times and are
// handled with custom parsing.
DEFINE_string(nodes, "", "CSV file containing graph nodes (vertices).");
DEFINE_string(node_label, "",
              "Specify additional label for nodes. To add multiple labels, "
              "repeat the flag multiple times.");
DEFINE_validator(node_label, &ValidateNoWhitespace);
DEFINE_string(relationships, "",
              "CSV file containing graph relationships (edges).");
DEFINE_string(relationship_type, "",
              "Overwrite the relationship type from csv with the given value.");
DEFINE_validator(relationship_type, &ValidateNoWhitespace);

std::vector<std::string> ParseRepeatedFlag(const std::string &flagname,
                                           int argc, char *argv[]) {
  std::vector<std::string> values;
  for (int i = 1; i < argc; ++i) {
    std::string flag(argv[i]);
    int matched_flag_dashes = 0;
    if (utils::StartsWith(flag, "--" + flagname))
      matched_flag_dashes = 2;
    else if (utils::StartsWith(flag, "-" + flagname))
      matched_flag_dashes = 1;
    // Get the value if we matched the flag.
    if (matched_flag_dashes != 0) {
      std::string value;
      auto maybe_value = flag.substr(flagname.size() + matched_flag_dashes);
      if (maybe_value.empty() && i + 1 < argc)
        value = argv[++i];
      else if (!maybe_value.empty() && maybe_value.front() == '=')
        value = maybe_value.substr(1);
      CHECK(!value.empty()) << "The argument '" << flagname << "' is required";
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

bool operator==(const NodeId &a, const NodeId &b) {
  return a.id == b.id && a.id_space == b.id_space;
}

std::ostream &operator<<(std::ostream &stream, const NodeId &node_id) {
  return stream << node_id.id << "(" << node_id.id_space << ")";
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
class LoadException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/// @throw LoadException
std::pair<std::vector<std::string>, uint64_t> ReadRow(std::istream &stream) {
  std::vector<std::string> row;
  bool quoting = false;
  std::vector<char> column;
  std::string line;
  uint64_t lines_count = 0;

  auto check_quote = [&line](int curr_idx) {
    return curr_idx + FLAGS_quote.size() <= line.size() &&
           line.compare(curr_idx, FLAGS_quote.size(), FLAGS_quote) == 0;
  };

  do {
    if (!std::getline(stream, line)) {
      if (quoting) {
        throw LoadException(
            "There is no more data left to load while inside a quoted string. "
            "Did you forget to close the quote?");
      } else {
        // The whole row was processed.
        break;
      }
    }
    ++lines_count;
    for (size_t i = 0; i < line.size(); ++i) {
      auto c = line[i];
      if (quoting) {
        if (check_quote(i)) {
          quoting = false;
          i += FLAGS_quote.size() - 1;
        } else {
          column.push_back(c);
        }
      } else if (check_quote(i)) {
        // Hopefully, escaping isn't needed
        quoting = true;
        i += FLAGS_quote.size() - 1;
      } else if (c == FLAGS_delimiter.front()) {
        row.emplace_back(column.begin(), column.end());
        column.clear();
        // Handle special case when delimiter is the last
        // character in line. This means that another
        // empty column needs to be added.
        if (i == line.size() - 1) {
          row.emplace_back("");
        }
      } else {
        column.push_back(c);
      }
    }
  } while (quoting);

  if (!column.empty()) row.emplace_back(column.begin(), column.end());
  return {std::move(row), lines_count};
}

/// @throw LoadException
std::pair<std::vector<Field>, uint64_t> ReadHeader(std::istream &stream) {
  auto [row, lines_count] = ReadRow(stream);
  std::vector<Field> fields;
  fields.reserve(row.size());
  for (const auto &value : row) {
    auto name_and_type = utils::Split(value, ":");
    if (name_and_type.size() != 1U && name_and_type.size() != 2U)
      throw LoadException(
          "Expected a name and optionally a type, got '{}'. Did you specify a "
          "correct CSV delimiter?",
          value);
    auto name = name_and_type[0];
    // When type is missing, default is string.
    std::string type("string");
    if (name_and_type.size() == 2U) type = utils::Trim(name_and_type[1]);
    fields.push_back(Field{name, type});
  }
  return {std::move(fields), lines_count};
}

/// @throw LoadException
storage::PropertyValue StringToValue(const std::string &str,
                                     const std::string &type) {
  // Empty string signifies Null.
  if (str.empty()) return storage::PropertyValue();
  auto convert = [](const auto &str, const auto &type) {
    if (type == "int" || type == "long" || type == "byte" || type == "short") {
      std::istringstream ss(str);
      int64_t val;
      ss >> val;
      return storage::PropertyValue(val);
    } else if (type == "float" || type == "double") {
      return storage::PropertyValue(utils::ParseDouble(str));
    } else if (type == "boolean") {
      if (utils::ToLowerCase(str) == "true") {
        return storage::PropertyValue(true);
      } else {
        return storage::PropertyValue(false);
      }
    } else if (type == "char" || type == "string") {
      return storage::PropertyValue(str);
    }
    throw LoadException("Unexpected type: {}", type);
  };
  // Type *not* ending with '[]', signifies regular value.
  if (!utils::EndsWith(type, "[]")) return convert(str, type);
  // Otherwise, we have an array type.
  auto elem_type = type.substr(0, type.size() - 2);
  auto elems = utils::Split(str, FLAGS_array_delimiter);
  std::vector<storage::PropertyValue> array;
  array.reserve(elems.size());
  for (const auto &elem : elems) {
    array.push_back(convert(std::string(utils::Trim(elem)), elem_type));
  }
  return storage::PropertyValue(std::move(array));
}

/// @throw LoadException
std::string GetIdSpace(const std::string &type) {
  // The format of this field is as follows:
  // [START_|END_]ID[(<id_space>)]
  std::regex format(R"(^(START_|END_)?ID(\(([^\(\)]+)\))?$)",
                    std::regex::extended);
  std::smatch res;
  if (!std::regex_match(type, res, format))
    throw LoadException(
        "Expected the ID field to look like '[START_|END_]ID[(<id_space>)]', "
        "but got '{}' instead",
        type);
  CHECK(res.size() == 4) << "Invalid regex match result!";
  return res[3];
}

/// @throw LoadException
void ProcessNodeRow(storage::Storage *store, const std::vector<Field> &fields,
                    const std::vector<std::string> &row,
                    const std::vector<std::string> &additional_labels,
                    std::unordered_map<NodeId, storage::Gid> *node_id_map) {
  std::optional<NodeId> id;
  auto acc = store->Access();
  auto node = acc.CreateVertex();
  for (size_t i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    std::string value(utils::Trim(row[i]));
    if (utils::StartsWith(field.type, "ID")) {
      if (id) throw LoadException("Only one node ID must be specified");
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map->find(node_id);
      if (it != node_id_map->end()) {
        if (FLAGS_skip_duplicate_nodes) {
          LOG(WARNING) << "Skipping duplicate node with id '" << node_id << "'";
          return;
        } else {
          throw LoadException("Node with id '{}' already exists", node_id);
        }
      }
      node_id_map->emplace(node_id, node.Gid());
      auto node_property = node.SetProperty(acc.NameToProperty("id"),
                                            storage::PropertyValue(node_id.id));
      if (!node_property.HasValue())
        throw LoadException("Couldn't add property 'id' to the node");
      if (!*node_property)
        throw LoadException("The property 'id' already exists");
      id = node_id;
    } else if (field.type == "LABEL") {
      for (const auto &label : utils::Split(value, FLAGS_array_delimiter)) {
        auto node_label = node.AddLabel(acc.NameToLabel(utils::Trim(label)));
        if (!node_label.HasValue())
          throw LoadException("Couldn't add label '{}' to the node",
                              utils::Trim(label));
        if (!*node_label)
          throw LoadException("The label '{}' already exists",
                              utils::Trim(label));
      }
    } else if (field.type != "IGNORE") {
      auto node_property = node.SetProperty(acc.NameToProperty(field.name),
                                            StringToValue(value, field.type));
      if (!node_property.HasValue())
        throw LoadException("Couldn't add property '{}' to the node",
                            field.name);
      if (!*node_property)
        throw LoadException("The property '{}' already exists", field.name);
    }
  }
  for (const auto &label : additional_labels) {
    auto node_label = node.AddLabel(acc.NameToLabel(utils::Trim(label)));
    if (!node_label.HasValue())
      throw LoadException("Couldn't add label '{}' to the node",
                          utils::Trim(label));
    if (!*node_label)
      throw LoadException("The label '{}' already exists", utils::Trim(label));
  }
  if (!id) throw LoadException("Node ID must be specified");
  if (acc.Commit().HasError()) throw LoadException("Couldn't store the node");
}

void ProcessNodes(storage::Storage *store, const std::string &nodes_path,
                  std::unordered_map<NodeId, storage::Gid> *node_id_map,
                  const std::vector<std::string> &additional_labels) {
  std::ifstream nodes_file(nodes_path);
  CHECK(nodes_file) << "Unable to open '" << nodes_path << "'";
  uint64_t row_number = 1;
  try {
    auto [fields, header_lines] = ReadHeader(nodes_file);
    row_number += header_lines;
    while (true) {
      auto [row, lines_count] = ReadRow(nodes_file);
      if (lines_count == 0) break;
      if (row.size() != fields.size())
        throw LoadException(
            "Expected as many values as there are header fields (found {}, "
            "expected {})",
            row.size(), fields.size());
      ProcessNodeRow(store, fields, row, additional_labels, node_id_map);
      row_number += lines_count;
    }
  } catch (const LoadException &e) {
    LOG(FATAL) << "Couldn't process row " << row_number << " of '" << nodes_path
               << "' because of: " << e.what();
  }
}

/// @throw LoadException
void ProcessRelationshipsRow(
    storage::Storage *store, const std::vector<Field> &fields,
    const std::vector<std::string> &row,
    const std::unordered_map<NodeId, storage::Gid> &node_id_map) {
  std::optional<storage::Gid> start_id;
  std::optional<storage::Gid> end_id;
  std::optional<std::string> relationship_type;
  std::map<std::string, storage::PropertyValue> properties;
  for (size_t i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    std::string value(utils::Trim(row[i]));
    if (utils::StartsWith(field.type, "START_ID")) {
      if (start_id) throw LoadException("Only one node ID must be specified");
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map.find(node_id);
      if (it == node_id_map.end())
        throw LoadException("Node with id '{}' does not exist", node_id);
      start_id = it->second;
    } else if (utils::StartsWith(field.type, "END_ID")) {
      if (end_id) throw LoadException("Only one node ID must be specified");
      NodeId node_id{value, GetIdSpace(field.type)};
      auto it = node_id_map.find(node_id);
      if (it == node_id_map.end())
        throw LoadException("Node with id '{}' does not exist", node_id);
      end_id = it->second;
    } else if (field.type == "TYPE") {
      if (relationship_type)
        throw LoadException("Only one relationship TYPE must be specified");
      relationship_type = value;
    } else if (field.type != "IGNORE") {
      properties[field.name] = StringToValue(value, field.type);
    }
  }
  auto rel_type = utils::Trim(FLAGS_relationship_type);
  if (!rel_type.empty()) {
    relationship_type = rel_type;
  }
  if (!start_id) throw LoadException("START_ID must be set");
  if (!end_id) throw LoadException("END_ID must be set");
  if (!relationship_type) throw LoadException("Relationship TYPE must be set");

  auto acc = store->Access();
  auto from_node = acc.FindVertex(*start_id, storage::View::NEW);
  if (!from_node) throw LoadException("From node must be in the storage");
  auto to_node = acc.FindVertex(*end_id, storage::View::NEW);
  if (!to_node) throw LoadException("To node must be in the storage");

  auto relationship = acc.CreateEdge(&*from_node, &*to_node,
                                     acc.NameToEdgeType(*relationship_type));
  if (!relationship.HasValue())
    throw LoadException("Couldn't create the relationship");

  if (acc.Commit().HasError())
    throw LoadException("Couldn't store the relationship");
}

void ProcessRelationships(
    storage::Storage *store, const std::string &relationships_path,
    const std::unordered_map<NodeId, storage::Gid> &node_id_map) {
  std::ifstream relationships_file(relationships_path);
  CHECK(relationships_file) << "Unable to open '" << relationships_path << "'";
  uint64_t row_number = 1;
  try {
    auto [fields, header_lines] = ReadHeader(relationships_file);
    row_number += header_lines;
    while (true) {
      auto [row, lines_count] = ReadRow(relationships_file);
      if (lines_count == 0) break;
      if (row.size() != fields.size())
        throw LoadException(
            "Expected as many values as there are header fields (found {}, "
            "expected {})",
            row.size(), fields.size());
      ProcessRelationshipsRow(store, fields, row, node_id_map);
      row_number += lines_count;
    }
  } catch (const LoadException &e) {
    LOG(FATAL) << "Couldn't process row " << row_number << " of '"
               << relationships_path << "' because of: " << e.what();
  }
}

static const char *usage =
    "[OPTION]... [--out=SNAPSHOT_FILE] [--nodes=CSV_FILE]... "
    "[--relationships=CSV_FILE]...\n"
    "Create a Memgraph recovery snapshot file from CSV.\n";

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage(usage);
  gflags::SetVersionString(version_string);

  auto nodes = ParseRepeatedFlag("nodes", argc, argv);
  auto additional_labels = ParseRepeatedFlag("node-label", argc, argv);
  auto relationships = ParseRepeatedFlag("relationships", argc, argv);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("mg_import_csv");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  CHECK(!nodes.empty()) << "The --nodes flag is required!";

  // Verify that the user that started the Memgraph process is the same user
  // that is the owner of the data directory.
  VerifyDataDirectoryOwnerAndProcessUser(FLAGS_data_directory);

  {
    auto all_inputs = nodes;
    all_inputs.insert(all_inputs.end(), relationships.begin(),
                      relationships.end());
    LOG(INFO) << "Loading " << utils::Join(all_inputs, ", ");
  }

  std::unordered_map<NodeId, storage::Gid> node_id_map;
  storage::Storage store{
      {.durability =
           {.storage_directory = FLAGS_data_directory,
            .recover_on_startup = false,
            .snapshot_wal_mode =
                storage::Config::Durability::SnapshotWalMode::DISABLED,
            .snapshot_on_exit = true},
       .items = {
           .properties_on_edges = FLAGS_storage_properties_on_edges,
       }}};

  utils::Timer load_timer;

  // Process all nodes files.
  for (const auto &nodes_file : nodes) {
    ProcessNodes(&store, nodes_file, &node_id_map, additional_labels);
  }

  // Process all relationships files.
  for (const auto &relationships_file : relationships) {
    ProcessRelationships(&store, relationships_file, node_id_map);
  }

  double load_sec = load_timer.Elapsed().count();
  LOG(INFO) << "Loaded all data in " << fmt::format("{:.3f}", load_sec) << " s";

  // The snapshot will be created in the storage destructor.

  return 0;
}
