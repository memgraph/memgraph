#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cppitertools/chain.hpp>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "config.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/single_node/paths.hpp"
#include "durability/single_node/snapshooter.hpp"
#include "durability/single_node/version.hpp"
#include "utils/cast.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

// Snapshot layout is described in durability/version.hpp
static_assert(durability::kVersion == 11,
              "Wrong snapshot version, please update!");

bool ValidateNotEmpty(const char *flagname, const std::string &value) {
  if (utils::Trim(value).empty()) {
    printf("The argument '%s' is required\n", flagname);
    return false;
  }
  return true;
}

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

DEFINE_string(out, "",
              "Destination for the created snapshot file. Without it, snapshot "
              "is written inside the expected snapshots directory of Memgraph "
              "installation.");
DEFINE_bool(overwrite, false, "Overwrite the output file if it exists");
DEFINE_string(array_delimiter, ";",
              "Delimiter between elements of array values, default is ';'");
DEFINE_string(csv_delimiter, ",",
              "Delimiter between each field in the CSV, default is ','");
DEFINE_string(quote, "\"",
              "Quotation character, default is '\"'. Cannot contain '\n'");
DEFINE_validator(quote, &ValidateNotNewline);
DEFINE_bool(skip_duplicate_nodes, false,
            "Skip duplicate nodes or raise an error (default)");
// Arguments `--nodes` and `--relationships` can be input multiple times and are
// handled with custom parsing.
DEFINE_string(nodes, "", "CSV file containing graph nodes (vertices)");
DEFINE_validator(nodes, &ValidateNotEmpty);
DEFINE_string(node_label, "",
              "Specify additional label for nodes. To add multiple labels, "
              "repeat the flag multiple times");
DEFINE_validator(node_label, &ValidateNoWhitespace);
DEFINE_string(relationships, "",
              "CSV file containing graph relationships (edges)");
DEFINE_string(relationship_type, "",
              "Overwrite the relationship type from csv with the given value");
DEFINE_validator(relationship_type, &ValidateNoWhitespace);

auto ParseRepeatedFlag(const std::string &flagname, int argc, char *argv[]) {
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

auto &operator<<(std::ostream &stream, const NodeId &node_id) {
  return stream << fmt::format("{}({})", node_id.id, node_id.id_space);
}

namespace std {

template <>
struct hash<NodeId> {
  size_t operator()(const NodeId &node_id) const {
    size_t id_hash = std::hash<std::string>{}(node_id.id);
    size_t id_space_hash = std::hash<std::string>{}(node_id.id_space);
    return id_hash ^ (id_space_hash << 1);
  }
};

}  // namespace std

class MemgraphNodeIdMap {
 public:
  std::optional<int64_t> Get(const NodeId &node_id) const {
    auto found_it = node_id_to_mg_.find(node_id);
    if (found_it == node_id_to_mg_.end()) return std::nullopt;
    return found_it->second;
  }

  uint64_t Insert(const NodeId &node_id) {
    auto gid = generator_.Next();
    node_id_to_mg_[node_id] = gid;
    return gid;
  }

 private:
  gid::Generator generator_;
  std::unordered_map<NodeId, int64_t> node_id_to_mg_;
};

std::vector<std::string> ReadRow(std::istream &stream) {
  std::vector<std::string> row;
  bool quoting = false;
  std::vector<char> column;
  std::string line;

  auto check_quote = [&line](int curr_idx) {
    return curr_idx + FLAGS_quote.size() <= line.size() &&
           line.compare(curr_idx, FLAGS_quote.size(), FLAGS_quote) == 0;
  };

  do {
    std::getline(stream, line);
    auto line_size = line.size();
    for (auto i = 0; i < line_size; ++i) {
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
      } else if (c == FLAGS_csv_delimiter.front()) {
        row.emplace_back(column.begin(), column.end());
        column.clear();
        // Handle special case when delimiter is the last
        // character in line. This means that another
        // empty column needs to be added.
        if (i == line_size - 1) {
          row.emplace_back("");
        }
      } else {
        column.push_back(c);
      }
    }
  } while (quoting);

  if (!column.empty()) row.emplace_back(column.begin(), column.end());
  return row;
}

std::vector<Field> ReadHeader(std::istream &stream) {
  auto row = ReadRow(stream);
  std::vector<Field> fields;
  fields.reserve(row.size());
  for (const auto &value : row) {
    auto name_and_type = utils::Split(value, ":");
    CHECK(name_and_type.size() == 1U || name_and_type.size() == 2U)
        << fmt::format(
               "\nExpected a name and optionally a type, got '{}'.\nDid you "
               "specify a correct CSV delimiter?",
               value);
    auto name = name_and_type[0];
    // When type is missing, default is string.
    std::string type("string");
    if (name_and_type.size() == 2U)
      type = utils::ToLowerCase(utils::Trim(name_and_type[1]));
    fields.push_back(Field{name, type});
  }
  return fields;
}

communication::bolt::Value StringToValue(const std::string &str,
                                         const std::string &type) {
  // Empty string signifies Null.
  if (str.empty()) return communication::bolt::Value();
  auto convert = [](const auto &str,
                    const auto &type) -> communication::bolt::Value {
    if (type == "int" || type == "long" || type == "byte" || type == "short") {
      std::istringstream ss(str);
      int64_t val;
      ss >> val;
      return val;
    } else if (type == "float" || type == "double") {
      return utils::ParseDouble(str);
    } else if (type == "boolean") {
      return utils::ToLowerCase(str) == "true" ? true : false;
    } else if (type == "char" || type == "string") {
      return str;
    }
    LOG(FATAL) << "Unexpected type: " << type;
    return communication::bolt::Value();
  };
  // Type *not* ending with '[]', signifies regular value.
  if (!utils::EndsWith(type, "[]")) return convert(str, type);
  // Otherwise, we have an array type.
  auto elem_type = type.substr(0, type.size() - 2);
  auto elems = utils::Split(str, FLAGS_array_delimiter);
  std::vector<communication::bolt::Value> array;
  array.reserve(elems.size());
  for (const auto &elem : elems) {
    array.push_back(convert(std::string(utils::Trim(elem)), elem_type));
  }
  return array;
}

std::string GetIdSpace(const std::string &type) {
  auto start = type.find("(");
  if (start == std::string::npos) return "";
  return type.substr(start + 1, type.size() - 1);
}

void WriteNodeRow(
    communication::bolt::BaseEncoder<HashedFileWriter> *encoder,
    const std::vector<Field> &fields, const std::vector<std::string> &row,
    const std::vector<std::string> &additional_labels,
    MemgraphNodeIdMap &node_id_map) {
  std::optional<gid::Gid> id;
  std::vector<std::string> labels;
  std::map<std::string, communication::bolt::Value> properties;
  for (int i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    std::string value(utils::Trim(row[i]));
    if (utils::StartsWith(field.type, "id")) {
      CHECK(!id) << "Only one node ID must be specified";
      NodeId node_id{value, GetIdSpace(field.type)};
      if (node_id_map.Get(node_id)) {
        if (FLAGS_skip_duplicate_nodes) {
          LOG(WARNING) << fmt::format("Skipping duplicate node with id '{}'",
                                      node_id);
          return;
        } else {
          LOG(FATAL) << fmt::format("Node with id '{}' already exists",
                                    node_id);
        }
      }
      id = node_id_map.Insert(node_id);
      properties["id"] = node_id.id;
    } else if (field.type == "label") {
      for (const auto &label : utils::Split(value, FLAGS_array_delimiter)) {
        labels.emplace_back(utils::Trim(label));
      }
    } else if (field.type != "ignore") {
      properties[field.name] = StringToValue(value, field.type);
    }
  }
  labels.insert(labels.end(), additional_labels.begin(),
                additional_labels.end());
  CHECK(id) << "Node ID must be specified";
  encoder->WriteVertex(
      {communication::bolt::Id::FromUint(*id), labels, properties});
}

auto PassNodes(
    communication::bolt::BaseEncoder<HashedFileWriter> *encoder,
    const std::string &nodes_path, MemgraphNodeIdMap &node_id_map,
    const std::vector<std::string> &additional_labels) {
  int64_t node_count = 0;
  std::ifstream nodes_file(nodes_path);
  CHECK(nodes_file) << fmt::format("Unable to open '{}'", nodes_path);
  auto fields = ReadHeader(nodes_file);
  auto row = ReadRow(nodes_file);
  while (!row.empty()) {
    CHECK_EQ(row.size(), fields.size())
        << "Expected as many values as there are header fields";
    WriteNodeRow(encoder, fields, row, additional_labels, node_id_map);
    // Increase count and move to next row.
    node_count += 1;
    row = ReadRow(nodes_file);
  }
  return node_count;
}

void WriteRelationshipsRow(
    communication::bolt::BaseEncoder<HashedFileWriter> *encoder,
    const std::vector<Field> &fields, const std::vector<std::string> &row,
    const MemgraphNodeIdMap &node_id_map, gid::Gid relationship_id) {
  std::optional<int64_t> start_id;
  std::optional<int64_t> end_id;
  std::optional<std::string> relationship_type;
  std::map<std::string, communication::bolt::Value> properties;
  for (int i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    std::string value(utils::Trim(row[i]));
    if (utils::StartsWith(field.type, "start_id")) {
      CHECK(!start_id) << "Only one node ID must be specified";
      NodeId node_id{value, GetIdSpace(field.type)};
      start_id = node_id_map.Get(node_id);
      if (!start_id)
        LOG(FATAL) << fmt::format("Node with id '{}' does not exist", node_id);
    } else if (utils::StartsWith(field.type, "end_id")) {
      CHECK(!end_id) << "Only one node ID must be specified";
      NodeId node_id{value, GetIdSpace(field.type)};
      end_id = node_id_map.Get(node_id);
      if (!end_id)
        LOG(FATAL) << fmt::format("Node with id '{}' does not exist", node_id);
    } else if (field.type == "type") {
      CHECK(!relationship_type)
          << "Only one relationship TYPE must be specified";
      relationship_type = value;
    } else if (field.type != "ignore") {
      properties[field.name] = StringToValue(value, field.type);
    }
  }
  auto rel_type = utils::Trim(FLAGS_relationship_type);
  if (!rel_type.empty()) {
    relationship_type = FLAGS_relationship_type;
  }
  CHECK(start_id) << "START_ID must be set";
  CHECK(end_id) << "END_ID must be set";
  CHECK(relationship_type) << "Relationship TYPE must be set";

  auto bolt_id = communication::bolt::Id::FromUint(relationship_id);
  auto bolt_start_id = communication::bolt::Id::FromUint(*start_id);
  auto bolt_end_id = communication::bolt::Id::FromUint(*end_id);
  encoder->WriteEdge(
      {bolt_id, bolt_start_id, bolt_end_id, *relationship_type, properties});
}

int PassRelationships(
    communication::bolt::BaseEncoder<HashedFileWriter> *encoder,
    const std::string &relationships_path, const MemgraphNodeIdMap &node_id_map,
    gid::Generator &relationship_id_generator) {
  std::ifstream relationships_file(relationships_path);
  CHECK(relationships_file)
      << fmt::format("Unable to open '{}'", relationships_path);
  auto fields = ReadHeader(relationships_file);
  auto row = ReadRow(relationships_file);
  int64_t relationships = 0;
  while (!row.empty()) {
    CHECK_EQ(row.size(), fields.size())
        << "Expected as many values as there are header fields";
    WriteRelationshipsRow(encoder, fields, row, node_id_map,
                          relationship_id_generator.Next());
    ++relationships;
    row = ReadRow(relationships_file);
  }
  return relationships;
}

void Convert(const std::vector<std::string> &nodes,
             const std::vector<std::string> &additional_labels,
             const std::vector<std::string> &relationships,
             const std::string &output_path) {
  try {
    HashedFileWriter buffer(output_path);
    communication::bolt::BaseEncoder<HashedFileWriter> encoder(buffer);
    int64_t node_count = 0;
    int64_t edge_count = 0;
    gid::Generator relationship_id_generator;
    MemgraphNodeIdMap node_id_map;
    // Snapshot file has the following contents in order:
    //   1) Magic number.
    //   2) Transaction ID of the snapshooter. When generated set to 0.
    //   3) Transactional snapshot of the snapshoter. When the snapshot is
    //   generated it's an empty list.
    //   4) List of label+property index.
    //   5) All nodes, sequentially, but not encoded as a list.
    //   6) All relationships, sequentially, but not encoded as a list.
    //   7) Summary with node count, relationship count and hash digest.
    encoder.WriteRAW(durability::kSnapshotMagic.data(),
                     durability::kSnapshotMagic.size());
    encoder.WriteValue(durability::kVersion);

    encoder.WriteInt(0);    // Id of transaction that is snapshooting.
    encoder.WriteList({});  // Transactional snapshot.
    encoder.WriteList({});  // Label + property indexes.
    encoder.WriteList({});  // Unique constraints
    // PassNodes streams vertices to the encoder.
    for (const auto &nodes_file : nodes) {
      node_count +=
          PassNodes(&encoder, nodes_file, node_id_map, additional_labels);
    }
    // PassEdges streams edges to the encoder.
    for (const auto &relationships_file : relationships) {
      edge_count += PassRelationships(&encoder, relationships_file, node_id_map,
                                      relationship_id_generator);
    }
    buffer.WriteValue(node_count);
    buffer.WriteValue(edge_count);
    buffer.WriteValue(buffer.hash());
  } catch (const std::ios_base::failure &) {
    // Only HashedFileWriter sets the underlying fstream to throw.
    LOG(FATAL) << fmt::format("Unable to write to '{}'", output_path);
  }
}

static const char *usage =
    "[OPTION]... [--out=SNAPSHOT_FILE] [--nodes=CSV_FILE]... "
    "[--relationships=CSV_FILE]...\n"
    "Create a Memgraph recovery snapshot file from CSV.\n";

// Used only to get the value from memgraph's configuration files.
DECLARE_string(durability_directory);

std::string GetOutputPath() {
  // If we have the 'out' flag, use that.
  if (!utils::Trim(FLAGS_out).empty()) return FLAGS_out;
  // Without the 'out', fall back to reading the memgraph configuration for
  // durability_directory. Hopefully, memgraph configuration doesn't contain
  // other flags which are defined in this file.
  LoadConfig();
  // Without durability_directory, we have to require 'out' flag.
  std::string durability_dir(utils::Trim(FLAGS_durability_directory));
  if (durability_dir.empty())
    LOG(FATAL) << "Unable to determine snapshot output location. Please, "
                  "provide the 'out' flag";
  try {
    auto snapshot_dir = durability_dir + "/snapshots";
    if (!std::filesystem::exists(snapshot_dir) &&
        !std::filesystem::create_directories(snapshot_dir)) {
      LOG(FATAL) << fmt::format("Cannot create snapshot directory '{}'",
                                snapshot_dir);
    }
  } catch (const std::filesystem::filesystem_error &error) {
    LOG(FATAL) << error.what();
  }
  // TODO: Remove this stupid hack which deletes WAL files just to make snapshot
  // recovery work. Newest snapshot without accompanying WAL files should be
  // detected in memgraph and correctly recovered (or error reported).
  try {
    auto wal_dir = durability_dir + "/wal";
    if (std::filesystem::exists(wal_dir)) {
      for ([[gnu::unused]] const auto &wal_file :
           std::filesystem::directory_iterator(wal_dir)) {
        if (!FLAGS_overwrite) {
          LOG(FATAL) << "Durability directory isn't empty. Pass --overwrite to "
                        "remove the old recovery data";
        }
        break;
      }
      LOG(WARNING) << "Removing old recovery data!";
      std::filesystem::remove_all(wal_dir);
    }
  } catch (const std::filesystem::filesystem_error &error) {
    LOG(FATAL) << error.what();
  }
  return std::string(
      durability::MakeSnapshotPath(durability_dir, 0));
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage(usage);
  auto nodes = ParseRepeatedFlag("nodes", argc, argv);
  auto additional_labels = ParseRepeatedFlag("node-label", argc, argv);
  auto relationships = ParseRepeatedFlag("relationships", argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  std::string output_path(GetOutputPath());
  if (std::filesystem::exists(output_path) && !FLAGS_overwrite) {
    LOG(FATAL) << fmt::format(
        "File exists: '{}'. Pass --overwrite if you want to overwrite.",
        output_path);
  }
  auto iter_all_inputs = iter::chain(nodes, relationships);
  std::vector<std::string> all_inputs(iter_all_inputs.begin(),
                                      iter_all_inputs.end());
  LOG(INFO) << fmt::format("Converting {} to '{}'",
                           utils::Join(all_inputs, ", "), output_path);
  utils::Timer conversion_timer;
  Convert(nodes, additional_labels, relationships, output_path);
  double conversion_sec = conversion_timer.Elapsed().count();
  LOG(INFO) << fmt::format("Created '{}' in {:.2f} seconds", output_path,
                           conversion_sec);
  return 0;
}
