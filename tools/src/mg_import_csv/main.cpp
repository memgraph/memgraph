#include <cstdio>
#include <experimental/filesystem>
#include <experimental/optional>
#include <fstream>
#include <unordered_map>

#include "cppitertools/chain.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "config.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/snapshooter.hpp"
#include "durability/version.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

bool ValidateNotEmpty(const char *flagname, const std::string &value) {
  if (utils::Trim(value).empty()) {
    printf("The argument '%s' is required\n", flagname);
    return false;
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
DEFINE_bool(skip_duplicate_nodes, false,
            "Skip duplicate nodes or raise an error (default)");
// Arguments `--nodes` and `--relationships` can be input multiple times and are
// handled with custom parsing.
DEFINE_string(nodes, "", "CSV file containing graph nodes (vertices)");
DEFINE_validator(nodes, &ValidateNotEmpty);
DEFINE_string(relationships, "",
              "CSV file containing graph relationships (edges)");

auto ParseRepeatedFlag(const std::string &flagname, int argc, char *argv[]) {
  std::vector<std::string> values;
  for (int i = 1; i < argc; ++i) {
    std::string flag(argv[i]);
    if ((flag == "--" + flagname || flag == "-" + flagname) && i + 1 < argc)
      values.push_back(argv[++i]);
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
  std::experimental::optional<int64_t> Get(const NodeId &node_id) const {
    auto found_it = node_id_to_mg_.find(node_id);
    if (found_it == node_id_to_mg_.end()) return std::experimental::nullopt;
    return found_it->second;
  }

  int64_t Insert(const NodeId &node_id) {
    int64_t id = mg_id_++;
    node_id_to_mg_[node_id] = id;
    return id;
  }

 private:
  int64_t mg_id_ = 0;
  std::unordered_map<NodeId, int64_t> node_id_to_mg_;
};

std::vector<std::string> ReadRow(std::istream &stream) {
  std::vector<std::string> row;
  char quoting = 0;
  std::vector<char> column;
  char c;
  while (!stream.get(c).eof()) {
    if (!stream) LOG(FATAL) << "Unable to read CSV row";
    if (quoting) {
      if (c == quoting)
        quoting = 0;
      else
        column.push_back(c);
    } else if (c == '"') {
      // Hopefully, escaping isn't needed.
      quoting = c;
    } else if (c == FLAGS_csv_delimiter.front()) {
      row.emplace_back(column.begin(), column.end());
      column.clear();
    } else if (c == '\n') {
      row.emplace_back(column.begin(), column.end());
      return row;
    } else {
      column.push_back(c);
    }
  }
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
        << "Expected a name and optionally a type";
    auto name = name_and_type[0];
    // When type is missing, default is string.
    std::string type("string");
    if (name_and_type.size() == 2U)
      type = utils::ToLowerCase(utils::Trim(name_and_type[1]));
    fields.push_back(Field{name, type});
  }
  return fields;
}

query::TypedValue StringToTypedValue(const std::string &str,
                                     const std::string &type) {
  // Empty string signifies Null.
  if (str.empty()) return query::TypedValue::Null;
  auto convert = [](const auto &str, const auto &type) -> query::TypedValue {
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
    return query::TypedValue::Null;
  };
  // Type *not* ending with '[]', signifies regular value.
  if (!utils::EndsWith(type, "[]")) return convert(str, type);
  // Otherwise, we have an array type.
  auto elem_type = type.substr(0, type.size() - 2);
  auto elems = utils::Split(str, FLAGS_array_delimiter);
  std::vector<query::TypedValue> array;
  array.reserve(elems.size());
  for (const auto &elem : elems) {
    array.push_back(convert(utils::Trim(elem), elem_type));
  }
  return array;
}

std::string GetIdSpace(const std::string &type) {
  auto start = type.find("(");
  if (start == std::string::npos) return "";
  return type.substr(start + 1, type.size() - 1);
}

void WriteNodeRow(const std::vector<Field> &fields,
                  const std::vector<std::string> &row,
                  MemgraphNodeIdMap &node_id_map,
                  communication::bolt::BaseEncoder<HashedFileWriter> &encoder) {
  std::experimental::optional<int64_t> id;
  std::vector<query::TypedValue> labels;
  std::map<std::string, query::TypedValue> properties;
  for (int i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    auto value = utils::Trim(row[i]);
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
      properties[field.name] = StringToTypedValue(value, field.type);
    }
  }
  CHECK(id) << "Node ID must be specified";
  // write node
  encoder.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                   3);
  encoder.WriteRAW(underlying_cast(communication::bolt::Signature::Node));
  encoder.WriteInt(*id);
  encoder.WriteList(labels);
  encoder.WriteMap(properties);
}

auto ConvertNodes(const std::string &nodes_path, MemgraphNodeIdMap &node_id_map,
                  communication::bolt::BaseEncoder<HashedFileWriter> &encoder) {
  int64_t node_count = 0;
  std::ifstream nodes_file(nodes_path);
  CHECK(nodes_file) << fmt::format("Unable to open '{}'", nodes_path);
  auto fields = ReadHeader(nodes_file);
  auto row = ReadRow(nodes_file);
  while (!row.empty()) {
    CHECK_EQ(row.size(), fields.size())
        << "Expected as many values as there are header fields";
    WriteNodeRow(fields, row, node_id_map, encoder);
    // Increase count and move to next row.
    node_count += 1;
    row = ReadRow(nodes_file);
  }
  return node_count;
}

void WriteRelationshipsRow(
    const std::vector<Field> &fields, const std::vector<std::string> &row,
    const MemgraphNodeIdMap &node_id_map, int64_t relationship_id,
    communication::bolt::BaseEncoder<HashedFileWriter> &encoder) {
  std::experimental::optional<int64_t> start_id;
  std::experimental::optional<int64_t> end_id;
  std::experimental::optional<std::string> relationship_type;
  std::map<std::string, query::TypedValue> properties;
  for (int i = 0; i < row.size(); ++i) {
    const auto &field = fields[i];
    auto value = utils::Trim(row[i]);
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
      properties[field.name] = StringToTypedValue(value, field.type);
    }
  }
  CHECK(start_id) << "START_ID must be set";
  CHECK(end_id) << "END_ID must be set";
  CHECK(relationship_type) << "Relationship TYPE must be set";
  // write relationship
  encoder.WriteRAW(underlying_cast(communication::bolt::Marker::TinyStruct) +
                   5);
  encoder.WriteRAW(
      underlying_cast(communication::bolt::Signature::Relationship));
  encoder.WriteInt(relationship_id);
  encoder.WriteInt(*start_id);
  encoder.WriteInt(*end_id);
  encoder.WriteString(*relationship_type);
  encoder.WriteMap(properties);
}

void ConvertRelationships(
    const std::string &relationships_path, const MemgraphNodeIdMap &node_id_map,
    communication::bolt::BaseEncoder<HashedFileWriter> &encoder,
    int64_t &next_relationship_id) {
  std::ifstream relationships_file(relationships_path);
  CHECK(relationships_file)
      << fmt::format("Unable to open '{}'", relationships_path);
  auto fields = ReadHeader(relationships_file);
  auto row = ReadRow(relationships_file);
  while (!row.empty()) {
    CHECK_EQ(row.size(), fields.size())
        << "Expected as many values as there are header fields";
    WriteRelationshipsRow(fields, row, node_id_map, next_relationship_id++,
                          encoder);
    row = ReadRow(relationships_file);
  }
}

void Convert(const std::vector<std::string> &nodes,
             const std::vector<std::string> &relationships,
             const std::string &output_path) {
  try {
    HashedFileWriter buffer(output_path);
    communication::bolt::BaseEncoder<HashedFileWriter> encoder(buffer);
    int64_t node_count = 0;
    int64_t next_relationship_id = 0;
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
    encoder.WriteRAW(durability::kMagicNumber.data(),
                     durability::kMagicNumber.size());
    encoder.WriteTypedValue(durability::kVersion);
    encoder.WriteInt(0);    // Id of transaction that is snapshooting.
    encoder.WriteList({});  // Transactional snapshot.
    encoder.WriteList({});  // Label + property indexes.
    for (const auto &nodes_file : nodes) {
      node_count += ConvertNodes(nodes_file, node_id_map, encoder);
    }
    for (const auto &relationships_file : relationships) {
      ConvertRelationships(relationships_file, node_id_map, encoder,
                           next_relationship_id);
    }
    buffer.WriteValue(node_count);
    buffer.WriteValue(next_relationship_id);
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
DEFINE_HIDDEN_string(snapshot_directory, "", "Snapshot directory");

std::string GetOutputPath() {
  // If we have the 'out' flag, use that.
  if (!utils::Trim(FLAGS_out).empty()) return FLAGS_out;
  // Without the 'out', fall back to reading the memgraph configuration for
  // snapshot_directory. Hopefully, memgraph configuration doesn't contain other
  // flags which are defined in this file.
  LoadConfig();
  // Without snapshot_directory, we have to require 'out' flag.
  if (utils::Trim(FLAGS_snapshot_directory).empty())
    LOG(FATAL) << "Unable to determine snapshot output location. Please, "
                  "provide the 'out' flag";
  // TODO: Remove 'default' when Dbms is purged.
  std::string snapshot_dir = FLAGS_snapshot_directory + "/default";
  try {
    if (!std::experimental::filesystem::exists(snapshot_dir) &&
        !std::experimental::filesystem::create_directories(snapshot_dir)) {
      LOG(FATAL) << fmt::format("Cannot create snapshot directory '{}'",
                                snapshot_dir);
    }
  } catch (const std::experimental::filesystem::filesystem_error &error) {
    LOG(FATAL) << error.what();
  }
  return std::string(durability::MakeSnapshotPath(snapshot_dir));
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage(usage);
  auto nodes = ParseRepeatedFlag("nodes", argc, argv);
  auto relationships = ParseRepeatedFlag("relationships", argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  std::string output_path(GetOutputPath());
  if (std::experimental::filesystem::exists(output_path) && !FLAGS_overwrite) {
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
  Convert(nodes, relationships, output_path);
  double conversion_sec = conversion_timer.Elapsed().count();
  LOG(INFO) << fmt::format("Created '{}' in {:.2f} seconds", output_path,
                           conversion_sec);
  return 0;
}
