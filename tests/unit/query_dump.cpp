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

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <usearch/index_plugins.hpp>
#include <vector>

#include "communication/result_stream_faker.hpp"
#include "dbms/constants.hpp"
#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "flags/experimental.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/dump.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "query/trigger_context.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"
#include "timezone_handler.hpp"
#include "utils/temporal.hpp"

namespace ms = memgraph::storage;
namespace r = ranges;
namespace rv = r::views;

const char *kPropertyId = "property_id";

const char *kCreateInternalIndex = "CREATE INDEX ON :__mg_vertex__(__mg_id__);";
const char *kDropInternalIndex = "DROP INDEX ON :__mg_vertex__(__mg_id__);";
const char *kRemoveInternalLabelProperty = "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;";

// A helper struct that contains info about database that is used to compare
// two databases (to check if their states are the same). It is assumed that
// each vertex and each edge have unique integer property under key
// `kPropertyId`.
struct DatabaseState {
  struct Vertex {
    int64_t id;
    std::set<std::string, std::less<>> labels;
    memgraph::storage::PropertyValue::map_t props;
  };

  struct Edge {
    int64_t from, to;
    std::string edge_type;
    memgraph::storage::PropertyValue::map_t props;
  };

  struct LabelItem {
    std::string label;
  };

  struct OrderedLabelPropertiesItem {
    std::string label;
    std::vector<std::string> properties;
  };

  struct TextItem {
    std::string index_name;
    std::string label;
    std::vector<std::string> properties;
  };

  struct LabelPropertiesItem {
    std::string label;
    std::set<std::string, std::less<>> properties;
  };

  struct LabelPropertyItem {
    std::string label;
    std::string property;
  };

  struct PointItem {
    std::string label;
    std::string property;
  };

  struct LabelPropertyType {
    std::string label;
    std::string property;
    memgraph::storage::TypeConstraintKind type;
  };

  std::set<Vertex> vertices;
  std::set<Edge> edges;
  std::set<LabelItem> label_indices;
  std::set<OrderedLabelPropertiesItem> label_property_indices;
  std::set<TextItem> text_indices;
  std::set<PointItem> point_indices;
  std::set<LabelPropertyItem> existence_constraints;
  std::set<LabelPropertiesItem> unique_constraints;
  std::set<LabelPropertyType> type_constraints;
};

bool operator<(const DatabaseState::Vertex &first, const DatabaseState::Vertex &second) {
  if (first.id != second.id) return first.id < second.id;
  if (first.labels != second.labels) return first.labels < second.labels;
  return first.props < second.props;
}

bool operator<(const DatabaseState::Edge &first, const DatabaseState::Edge &second) {
  if (first.from != second.from) return first.from < second.from;
  if (first.to != second.to) return first.to < second.to;
  if (first.edge_type != second.edge_type) return first.edge_type < second.edge_type;
  return first.props < second.props;
}

bool operator<(const DatabaseState::LabelItem &first, const DatabaseState::LabelItem &second) {
  return first.label < second.label;
}

bool operator<(const DatabaseState::OrderedLabelPropertiesItem &first,
               const DatabaseState::OrderedLabelPropertiesItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.properties < second.properties;
}

bool operator<(const DatabaseState::LabelPropertyItem &first, const DatabaseState::LabelPropertyItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.property < second.property;
}

bool operator<(const DatabaseState::TextItem &first, const DatabaseState::TextItem &second) {
  return first.index_name < second.index_name && first.label < second.label;
}

bool operator<(const DatabaseState::PointItem &first, const DatabaseState::PointItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.property < second.property;
}

bool operator<(const DatabaseState::LabelPropertiesItem &first, const DatabaseState::LabelPropertiesItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.properties < second.properties;
}
bool operator<(const DatabaseState::LabelPropertyType &first, const DatabaseState::LabelPropertyType &second) {
  if (first.label != second.label) return first.label < second.label;
  if (first.property != second.property) return first.property < second.property;
  return first.type < second.type;
}

bool operator==(const DatabaseState::Vertex &first, const DatabaseState::Vertex &second) {
  return first.id == second.id && first.labels == second.labels && first.props == second.props;
}

bool operator==(const DatabaseState::Edge &first, const DatabaseState::Edge &second) {
  return first.from == second.from && first.to == second.to && first.edge_type == second.edge_type &&
         first.props == second.props;
}

bool operator==(const DatabaseState::LabelItem &first, const DatabaseState::LabelItem &second) {
  return first.label == second.label;
}

bool operator==(const DatabaseState::OrderedLabelPropertiesItem &first,
                const DatabaseState::OrderedLabelPropertiesItem &second) {
  return first.label == second.label && first.properties == second.properties;
}

bool operator==(const DatabaseState::LabelPropertyItem &first, const DatabaseState::LabelPropertyItem &second) {
  return first.label == second.label && first.property == second.property;
}

bool operator==(const DatabaseState::TextItem &first, const DatabaseState::TextItem &second) {
  return first.index_name == second.index_name && first.label == second.label;
}

bool operator==(const DatabaseState::PointItem &first, const DatabaseState::PointItem &second) {
  return first.label == second.label && first.property == second.property;
}

bool operator==(const DatabaseState::LabelPropertiesItem &first, const DatabaseState::LabelPropertiesItem &second) {
  return first.label == second.label && first.properties == second.properties;
}

bool operator==(const DatabaseState::LabelPropertyType &first, const DatabaseState::LabelPropertyType &second) {
  return first.label == second.label && first.property == second.property && first.type == second.type;
}

bool operator==(const DatabaseState &first, const DatabaseState &second) {
  return first.vertices == second.vertices && first.edges == second.edges &&
         first.label_indices == second.label_indices && first.label_property_indices == second.label_property_indices &&
         first.existence_constraints == second.existence_constraints &&
         first.unique_constraints == second.unique_constraints && first.type_constraints == second.type_constraints;
}

DatabaseState GetState(memgraph::storage::Storage *db) {
  // Capture all vertices
  std::map<memgraph::storage::Gid, int64_t> gid_mapping;
  std::set<DatabaseState::Vertex> vertices;
  auto dba = db->Access();
  auto property_id = dba->NameToProperty(kPropertyId);
  for (const auto &vertex : dba->Vertices(memgraph::storage::View::NEW)) {
    std::set<std::string, std::less<>> labels;
    auto maybe_labels = vertex.Labels(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_labels.HasValue());
    for (const auto &label : *maybe_labels) {
      labels.insert(dba->LabelToName(label));
    }
    memgraph::storage::PropertyValue::map_t props;
    auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_properties.HasValue());
    for (const auto &kv : *maybe_properties) {
      props.emplace(kv.first, kv.second);
    }
    MG_ASSERT(props.count(property_id) == 1);
    const auto id = props[property_id].ValueInt();
    gid_mapping[vertex.Gid()] = id;
    vertices.insert({id, labels, props});
  }

  // Capture all edges
  std::set<DatabaseState::Edge> edges;
  for (const auto &vertex : dba->Vertices(memgraph::storage::View::NEW)) {
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_edges.HasValue());
    for (const auto &edge : maybe_edges->edges) {
      const auto &edge_type_name = dba->EdgeTypeToName(edge.EdgeType());
      memgraph::storage::PropertyValue::map_t props;
      auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
      MG_ASSERT(maybe_properties.HasValue());
      for (const auto &kv : *maybe_properties) {
        props.emplace(kv.first, kv.second);
      }
      const auto from = gid_mapping[edge.FromVertex().Gid()];
      const auto to = gid_mapping[edge.ToVertex().Gid()];
      edges.insert({from, to, edge_type_name, props});
    }
  }

  // Capture all indices
  std::set<DatabaseState::LabelItem> label_indices;
  std::set<DatabaseState::OrderedLabelPropertiesItem> label_properties_indices;
  std::set<DatabaseState::TextItem> text_indices;
  std::set<DatabaseState::PointItem> point_indices;
  // TODO: where are the edge types indices?

  {
    auto info = dba->ListAllIndices();
    for (const auto &item : info.label) {
      label_indices.insert({dba->LabelToName(item)});
    }
    for (const auto &[label, properties] : info.label_properties) {
      using namespace std::string_literals;
      auto properties_as_strings =
          properties | rv::transform([&](auto &&path) { return ToString(path, dba.get()); }) | r::to_vector;
      label_properties_indices.insert({dba->LabelToName(label), std::move(properties_as_strings)});
    }
    for (const auto &[name, label, properties] : info.text_indices) {
      auto prop_names =
          properties | rv::transform([&](auto prop_id) { return dba->PropertyToName(prop_id); }) | r::to_vector;
      text_indices.insert({name, dba->LabelToName(label), std::move(prop_names)});
    }
    for (const auto &item : info.point_label_property) {
      point_indices.insert({dba->LabelToName(item.first), dba->PropertyToName(item.second)});
    }
  }

  // Capture all constraints
  std::set<DatabaseState::LabelPropertyItem> existence_constraints;
  std::set<DatabaseState::LabelPropertiesItem> unique_constraints;
  std::set<DatabaseState::LabelPropertyType> type_constraints;
  {
    auto info = dba->ListAllConstraints();
    for (const auto &item : info.existence) {
      existence_constraints.insert({dba->LabelToName(item.first), dba->PropertyToName(item.second)});
    }
    for (const auto &item : info.unique) {
      std::set<std::string, std::less<>> properties;
      for (const auto &property : item.second) {
        properties.insert(dba->PropertyToName(property));
      }
      unique_constraints.insert({dba->LabelToName(item.first), std::move(properties)});
    }
    for (const auto &[label, property, type] : info.type) {
      type_constraints.insert({dba->LabelToName(label), dba->PropertyToName(property), type});
    }
  }

  return {vertices,        edges,         label_indices,         label_properties_indices,
          text_indices,    point_indices, existence_constraints, unique_constraints,
          type_constraints};
}

auto Execute(memgraph::query::InterpreterContext *context, memgraph::dbms::DatabaseAccess db,
             const std::string &query) {
  memgraph::query::Interpreter interpreter(context, db);
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  interpreter.SetUser(auth_checker.GenQueryUser(std::nullopt, {}));
  ResultStreamFaker stream(db->storage());

  auto [header, _1, qid, _2] = interpreter.Prepare(query, memgraph::query::no_params_fn, {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);

  return stream;
}

memgraph::storage::VertexAccessor CreateVertex(memgraph::storage::Storage::Accessor *dba,
                                               const std::vector<std::string> &labels,
                                               const memgraph::storage::PropertyValue::map_t &props,
                                               bool add_property_id = true) {
  MG_ASSERT(dba);
  auto vertex = dba->CreateVertex();
  for (const auto &label_name : labels) {
    MG_ASSERT(vertex.AddLabel(dba->NameToLabel(label_name)).HasValue());
  }
  for (const auto &kv : props) {
    MG_ASSERT(vertex.SetProperty(kv.first, kv.second).HasValue());
  }
  if (add_property_id) {
    MG_ASSERT(
        vertex.SetProperty(dba->NameToProperty(kPropertyId), memgraph::storage::PropertyValue(vertex.Gid().AsInt()))
            .HasValue());
  }
  return vertex;
}

memgraph::storage::EdgeAccessor CreateEdge(memgraph::storage::Storage::Accessor *dba,
                                           memgraph::storage::VertexAccessor *from,
                                           memgraph::storage::VertexAccessor *to, const std::string &edge_type_name,
                                           const memgraph::storage::PropertyValue::map_t &props,
                                           bool add_property_id = true) {
  MG_ASSERT(dba);
  auto edge = dba->CreateEdge(from, to, dba->NameToEdgeType(edge_type_name));
  MG_ASSERT(edge.HasValue());
  auto edgeAcc = std::move(edge.GetValue());
  for (const auto &kv : props) {
    MG_ASSERT(edgeAcc.SetProperty(kv.first, kv.second).HasValue());
  }
  if (add_property_id) {
    MG_ASSERT(
        edgeAcc.SetProperty(dba->NameToProperty(kPropertyId), memgraph::storage::PropertyValue(edgeAcc.Gid().AsInt()))
            .HasValue());
  }
  return edgeAcc;
}

template <class... TArgs>
void VerifyQueries(const std::vector<std::vector<memgraph::communication::bolt::Value>> &results, TArgs &&...args) {
  std::vector<std::string> expected{std::forward<TArgs>(args)...};
  std::vector<std::string> got;
  got.reserve(results.size());
  for (const auto &result : results) {
    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(result[0].IsString());
    got.push_back(result[0].ValueString());
  }
  std::sort(got.begin(), got.end());
  std::sort(expected.begin(), expected.end());
  ASSERT_EQ(got, expected);
}

template <typename StorageType>
class DumpTest : public ::testing::Test {
 public:
  const std::string testSuite = "query_dump";
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_query_dump_class"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
          config.salient.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
        }
        return config;
      }()  // iile
  };

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext context{memgraph::query::InterpreterConfig{}, nullptr, repl_state, system_state
#ifdef MG_ENTERPRISE
                                              ,
                                              std::nullopt
#endif
  };

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(DumpTest, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, EmptyGraph) {
  ResultStreamFaker stream(this->db->storage());
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  {
    auto acc = this->db->Access();
    memgraph::query::DbAccessor dba(acc.get());
    memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
  }
  ASSERT_EQ(stream.GetResults().size(), 0);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, SingleVertex) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithSingleLabel) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label1"}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1` {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithMultipleLabels) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithSingleProperty) {
  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("prop");
    CreateVertex(dba.get(), {}, {{prop_id, memgraph::storage::PropertyValue(42)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0, `prop`: 42});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultipleVertices) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    CreateVertex(dba.get(), {}, {}, false);
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});", "CREATE (:__mg_vertex__ {__mg_id__: 2});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

void test_PropertyValue(auto *test) {
  {
    auto dba = test->db->Access();
    auto null_value = memgraph::storage::PropertyValue();
    auto int_value = memgraph::storage::PropertyValue(13);
    auto bool_value = memgraph::storage::PropertyValue(true);
    auto double_value = memgraph::storage::PropertyValue(-1.2);
    auto str_value = memgraph::storage::PropertyValue("hello 'world'");
    auto map_key_1 = dba->NameToProperty("prop 1");
    auto map_key_2 = dba->NameToProperty("prop`2`");
    auto map_value = memgraph::storage::PropertyValue({{map_key_1, int_value}, {map_key_2, bool_value}});
    auto dt = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::Date, memgraph::utils::Date({1994, 12, 7}).MicrosecondsSinceEpoch()));
    auto lt = memgraph::storage::PropertyValue(
        memgraph::storage::TemporalData(memgraph::storage::TemporalType::LocalTime,
                                        memgraph::utils::LocalTime({14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()));
    auto ldt = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::LocalDateTime,
        memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).SysMicrosecondsSinceEpoch()));
    auto dur = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::Duration, memgraph::utils::Duration({3, 4, 5, 6, 10, 11}).microseconds));
    auto zdt = memgraph::storage::PropertyValue(memgraph::storage::ZonedTemporalData(
        memgraph::storage::ZonedTemporalType::ZonedDateTime,
        memgraph::utils::AsSysTime(
            memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()),
        memgraph::utils::Timezone("America/Los_Angeles")));
    auto list_value = memgraph::storage::PropertyValue({map_value, null_value, double_value, dt, lt, ldt, dur, zdt});
    auto prop1_id = dba->NameToProperty("p1");
    auto prop2_id = dba->NameToProperty("p2");
    CreateVertex(dba.get(), {}, {{prop1_id, list_value}, {prop2_id, str_value}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(test->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = test->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, test->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0, `p1`: [{`prop 1`: 13, "
                  "`prop``2```: true}, Null, -1.2, DATE(\"1994-12-07\"), "
                  "LOCALTIME(\"14:10:44.099099\"), LOCALDATETIME(\"1994-12-07T14:10:44.099099\"), "
                  "DURATION(\"P3DT4H5M6.010011S\"), DATETIME(\"1994-12-07T06:10:44.099099-08:00[America/Los_Angeles]\")"
                  "], `p2`: \"hello \\'world\\'\"});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, PropertyValue) { test_PropertyValue(this); }

TYPED_TEST(DumpTest, PropertyValueTZ) {
  HandleTimezone htz;
  htz.Set("Europe/Rome");
  test_PropertyValue(this);
}

TYPED_TEST(DumpTest, PropertyValueTZ2) {
  HandleTimezone htz;
  htz.Set("America/Los_Angeles");
  test_PropertyValue(this);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, SingleEdge) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultipleEdges) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    auto w = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {}, false);
    CreateEdge(dba.get(), &v, &u, "EdgeType 2", {}, false);
    CreateEdge(dba.get(), &v, &w, "EdgeType `!\"", {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});", "CREATE (:__mg_vertex__ {__mg_id__: 2});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
                  "v.__mg_id__ = 0 CREATE (u)-[:`EdgeType 2`]->(v);",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
                  "v.__mg_id__ = 2 CREATE (u)-[:`EdgeType ``!\"`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, EdgeWithProperties) {
  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("prop");
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {{prop_id, memgraph::storage::PropertyValue(13)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType` {`prop`: 13}]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, IndicesKeys) {
  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("p");
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{prop_id, memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(this->db->storage()->NameToLabel("Label1"), {this->db->storage()->NameToProperty("prop")})
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(this->db->storage()->NameToLabel("Label 2"), {this->db->storage()->NameToProperty("prop `")})
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :`Label1`(`prop`);", "CREATE INDEX ON :`Label 2`(`prop ```);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, CompositeIndicesKeys) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Composite indices not implemented for disk storage";
  }

  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("p");
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{prop_id, memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(this->db->storage()->NameToLabel("Label1"),
                          {this->db->storage()->NameToProperty("prop_a"), this->db->storage()->NameToProperty("prop_b"),
                           this->db->storage()->NameToProperty("prop_c")})
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :`Label1`(`prop_a`, `prop_b`, `prop_c`);", kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: 1});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, CompositeNestedIndicesKeys) {
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    GTEST_SKIP() << "Composite/nested indices not implemented for disk storage";
  }

  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("p");
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{prop_id, memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateIndex(this->db->storage()->NameToLabel("Label1"),
                                   {ms::PropertyPath{this->db->storage()->NameToProperty("prop_a"),
                                                     this->db->storage()->NameToProperty("prop_b")},
                                    ms::PropertyPath{this->db->storage()->NameToProperty("prop_a"),
                                                     this->db->storage()->NameToProperty("prop_c")}})
                     .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :`Label1`(`prop_a`.`prop_b`, `prop_a`.`prop_c`);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, EdgeIndicesKeys) {
  if (this->config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP() << "Edge index not implemented for on-disk storage mode";
  }

  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(this->db->storage()->NameToEdgeType("EdgeType")).HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(this->db->storage()->NameToEdgeType("EdgeType"), this->db->storage()->NameToProperty("prop"))
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateGlobalEdgeIndex(this->db->storage()->NameToProperty("prop")).HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE EDGE INDEX ON :`EdgeType`;", "CREATE EDGE INDEX ON :`EdgeType`(`prop`);",
                  "CREATE GLOBAL EDGE INDEX ON :(`prop`);", kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0});", "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, PointIndices) {
  if (this->config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP() << "Point index not implemented for ondisk";
  }

  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("p");
    auto point = memgraph::storage::Point2d{memgraph::storage::CoordinateReferenceSystem::Cartesian_2d, 1., 1.};
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{prop_id, memgraph::storage::PropertyValue(point)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreatePointIndex(this->db->storage()->NameToLabel("Label1"), this->db->storage()->NameToProperty("prop"))
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreatePointIndex(this->db->storage()->NameToLabel("Label 2"),
                                        this->db->storage()->NameToProperty("prop `"))
                     .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE POINT INDEX ON :`Label1`(`prop`);",
                  "CREATE POINT INDEX ON :`Label 2`(`prop ```);", kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: POINT({ x:1, y:1, srid: 7203 })});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, VectorIndices) {
  static constexpr std::string_view test_index1 = "test_index1";
  static constexpr std::string_view test_index2 = "test_index2";
  static constexpr unum::usearch::metric_kind_t metric = unum::usearch::metric_kind_t::l2sq_k;
  static constexpr uint16_t dimension = 2;
  static constexpr std::size_t capacity = 10;
  static constexpr uint16_t resize_coefficient = 2;
  static constexpr unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::f32_k;

  if (this->config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP() << "Vector index not implemented for ondisk";
  }

  {
    auto dba = this->db->Access();
    auto vector_property_id = dba->NameToProperty("vector_property");
    memgraph::storage::PropertyValue property_value(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(1.0)});
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{vector_property_id, property_value}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    const auto spec = memgraph::storage::VectorIndexSpec{test_index1.data(),
                                                         this->db->storage()->NameToLabel("Label1"),
                                                         this->db->storage()->NameToProperty("vector_property"),
                                                         metric,
                                                         dimension,
                                                         resize_coefficient,
                                                         capacity,
                                                         scalar_kind};
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateVectorIndex(spec).HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    const auto spec = memgraph::storage::VectorIndexSpec{test_index2.data(),
                                                         this->db->storage()->NameToLabel("Label 2"),
                                                         this->db->storage()->NameToProperty("prop `"),
                                                         metric,
                                                         dimension,
                                                         resize_coefficient,
                                                         capacity,
                                                         scalar_kind};
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateVectorIndex(spec).HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(
        stream.GetResults(),
        R"(CREATE VECTOR INDEX `test_index1` ON :`Label1`(`vector_property`) WITH CONFIG { "dimension": 2, "metric": "l2sq", "capacity": 10, "resize_coefficient": 2, "scalar_kind": "f32" };)",
        R"(CREATE VECTOR INDEX `test_index2` ON :`Label 2`(`prop ```) WITH CONFIG { "dimension": 2, "metric": "l2sq", "capacity": 10, "resize_coefficient": 2, "scalar_kind": "f32" };)",
        kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `vector_property`: [1, 1]});",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, VectorEdgeIndices) {
  static constexpr std::string_view test_index1 = "test_index1";
  static constexpr unum::usearch::metric_kind_t metric = unum::usearch::metric_kind_t::l2sq_k;
  static constexpr uint16_t dimension = 2;
  static constexpr std::size_t capacity = 10;
  static constexpr uint16_t resize_coefficient = 2;
  static constexpr unum::usearch::scalar_kind_t scalar_kind = unum::usearch::scalar_kind_t::f32_k;

  if (this->config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP() << "Vector index not implemented for ondisk";
  }

  {
    auto dba = this->db->Access();
    auto vector_property_id = dba->NameToProperty("vector_property");
    memgraph::storage::PropertyValue property_value(std::vector<memgraph::storage::PropertyValue>{
        memgraph::storage::PropertyValue(1.0), memgraph::storage::PropertyValue(1.0)});
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {{vector_property_id, property_value}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    const auto spec = memgraph::storage::VectorEdgeIndexSpec{test_index1.data(),
                                                             this->db->storage()->NameToEdgeType("EdgeType"),
                                                             this->db->storage()->NameToProperty("vector_property"),
                                                             metric,
                                                             dimension,
                                                             resize_coefficient,
                                                             capacity,
                                                             scalar_kind};
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateVectorEdgeIndex(spec).HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(
        stream.GetResults(),
        R"(CREATE VECTOR EDGE INDEX `test_index1` ON :`EdgeType`(`vector_property`) WITH CONFIG { "dimension": 2, "metric": "l2sq", "capacity": 10, "resize_coefficient": 2, "scalar_kind": "f32" };)",
        kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});", "CREATE (:__mg_vertex__ {__mg_id__: 1});",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
        "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType` {`vector_property`: [1, 1]}]->(v);",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExistenceConstraints) {
  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("prop");
    CreateVertex(dba.get(), {"L`abel 1"}, {{prop_id, memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("L`abel 1"),
                                                     this->db->storage()->NameToProperty("prop"));
    ASSERT_FALSE(res.HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE CONSTRAINT ON (u:`L``abel 1`) ASSERT EXISTS (u.`prop`);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`L``abel 1` {__mg_id__: 0, `prop`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, UniqueConstraints) {
  {
    auto dba = this->db->Access();
    auto prop_id = dba->NameToProperty("prop");
    auto prop2_id = dba->NameToProperty("prop2");
    CreateVertex(dba.get(), {"Label"},
                 {{prop_id, memgraph::storage::PropertyValue(1)}, {prop2_id, memgraph::storage::PropertyValue(2)}},
                 false);
    CreateVertex(dba.get(), {"Label"},
                 {{prop_id, memgraph::storage::PropertyValue(2)}, {prop2_id, memgraph::storage::PropertyValue(2)}},
                 false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateUniqueConstraint(
        this->db->storage()->NameToLabel("Label"),
        {this->db->storage()->NameToProperty("prop"), this->db->storage()->NameToProperty("prop2")});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(),
                  "CREATE CONSTRAINT ON (u:`Label`) ASSERT u.`prop`, u.`prop2` "
                  "IS UNIQUE;",
                  kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label` {__mg_id__: 0, `prop`: 1, "
                  "`prop2`: 2});",
                  "CREATE (:__mg_vertex__:`Label` {__mg_id__: 1, `prop`: 2, "
                  "`prop2`: 2});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  {
    auto dba = this->db->Access();
    auto map_key_1 = dba->NameToProperty("nested1");
    auto map_key_2 = dba->NameToProperty("nested2");
    auto prop1_id = dba->NameToProperty("prop1");
    auto prop2_id = dba->NameToProperty("prop2");
    memgraph::storage::PropertyValue::map_t prop1 = {{map_key_1, memgraph::storage::PropertyValue(1337)},
                                                     {map_key_2, memgraph::storage::PropertyValue(3.14)}};

    CreateVertex(
        dba.get(), {"Label1", "Label2"},
        {{prop1_id, memgraph::storage::PropertyValue(prop1)}, {prop2_id, memgraph::storage::PropertyValue("$'\t'")}});

    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  memgraph::storage::Config config{};
  config.durability.storage_directory = this->data_directory / "s1";
  config.disk.main_storage_directory = config.durability.storage_directory / "disk";
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    config.disk = disk_test_utils::GenerateOnDiskConfig("query-dump-s1").disk;
    config.force_on_disk = true;
  }
  auto clean_up_s1 = memgraph::utils::OnScopeExit{[&] {
    if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs("query-dump-s1");
    }
    std::filesystem::remove_all(config.durability.storage_directory);
  }};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state(
      ReplicationStateRootPath(config));

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk(config, repl_state);
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt) << "Failed to access db";
  auto &db_acc = *db_acc_opt;
  ASSERT_TRUE(db_acc->GetStorageMode() == (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL))
      << "Wrong storage mode!";

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context(memgraph::query::InterpreterConfig{}, nullptr, repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt
#endif
  );

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    const auto &results = stream.GetResults();
    ASSERT_GE(results.size(), 1);
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      Execute(&interpreter_context, db_acc, item[0].ValueString());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, CheckStateSimpleGraph) {
  memgraph::flags::SetExperimental(memgraph::flags::Experiments::TEXT_SEARCH);
  {
    auto dba = this->db->Access();
    auto name_id = dba->NameToProperty("name");
    auto id_id = dba->NameToProperty("id");
    auto u = CreateVertex(dba.get(), {"Person"}, {{name_id, memgraph::storage::PropertyValue("Ivan")}});
    auto v = CreateVertex(dba.get(), {"Person"}, {{name_id, memgraph::storage::PropertyValue("Josko")}});
    auto w = CreateVertex(
        dba.get(), {"Person"},
        {{name_id, memgraph::storage::PropertyValue("Bosko")}, {id_id, memgraph::storage::PropertyValue(0)}});
    auto z = CreateVertex(
        dba.get(), {"Person"},
        {{name_id, memgraph::storage::PropertyValue("Buha")}, {id_id, memgraph::storage::PropertyValue(1)}});
    auto zdt = memgraph::storage::ZonedTemporalData(
        memgraph::storage::ZonedTemporalType::ZonedDateTime,
        memgraph::utils::AsSysTime(
            memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).SysMicrosecondsSinceEpoch()),
        memgraph::utils::Timezone("America/Los_Angeles"));

    auto how_long_id = dba->NameToProperty("how_long");
    auto how_id = dba->NameToProperty("how");
    auto time_id = dba->NameToProperty("time");
    CreateEdge(dba.get(), &u, &v, "Knows", {});
    CreateEdge(dba.get(), &v, &w, "Knows", {{how_long_id, memgraph::storage::PropertyValue(5)}});
    CreateEdge(dba.get(), &w, &u, "Knows", {{how_id, memgraph::storage::PropertyValue("distant past")}});
    CreateEdge(dba.get(), &v, &u, "Knows", {});
    CreateEdge(dba.get(), &v, &u, "Likes", {});
    CreateEdge(dba.get(), &z, &u, "Knows", {});
    CreateEdge(dba.get(), &w, &z, "Knows", {{how_id, memgraph::storage::PropertyValue("school")}});
    CreateEdge(dba.get(), &w, &z, "Likes", {{how_id, memgraph::storage::PropertyValue("1234567890")}});
    CreateEdge(dba.get(), &w, &z, "Date",
               {{time_id, memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                              memgraph::storage::TemporalType::Date,
                              memgraph::utils::Date({1994, 12, 7}).MicrosecondsSinceEpoch()))}});
    CreateEdge(dba.get(), &w, &z, "LocalTime",
               {{time_id, memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                              memgraph::storage::TemporalType::LocalTime,
                              memgraph::utils::LocalTime({14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()))}});
    CreateEdge(
        dba.get(), &w, &z, "LocalDateTime",
        {{time_id,
          memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
              memgraph::storage::TemporalType::LocalDateTime,
              memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).SysMicrosecondsSinceEpoch()))}});
    CreateEdge(dba.get(), &w, &z, "Duration",
               {{time_id, memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                              memgraph::storage::TemporalType::Duration,
                              memgraph::utils::Duration({3, 4, 5, 6, 10, 11}).microseconds))}});
    CreateEdge(dba.get(), &w, &z, "NegativeDuration",
               {{time_id, memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                              memgraph::storage::TemporalType::Duration,
                              memgraph::utils::Duration({-3, -4, -5, -6, -10, -11}).microseconds))}});
    CreateEdge(dba.get(), &w, &z, "ZonedDateTime", {{time_id, memgraph::storage::PropertyValue(zdt)}});
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto ret = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("Person"),
                                                     this->db->storage()->NameToProperty("name"));
    ASSERT_FALSE(ret.HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto ret = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("Person"),
                                                  {this->db->storage()->NameToProperty("name")});
    ASSERT_TRUE(ret.HasValue());
    ASSERT_EQ(ret.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc->CreateIndex(this->db->storage()->NameToLabel("Person"), {this->db->storage()->NameToProperty("id")})
            .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateIndex(this->db->storage()->NameToLabel("Person"),
                                   {this->db->storage()->NameToProperty("unexisting_property")})
                     .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateTextIndex("text_index", this->db->storage()->NameToLabel("Person"),
                                       std::array{this->db->storage()->NameToProperty("name")})
                     .HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  const auto &db_initial_state = GetState(this->db->storage());
  memgraph::storage::Config config{};
  config.durability.storage_directory = this->data_directory / "s2";
  config.disk.main_storage_directory = config.durability.storage_directory / "disk";
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    config.disk = disk_test_utils::GenerateOnDiskConfig("query-dump-s2").disk;
    config.force_on_disk = true;
  }
  auto clean_up_s2 = memgraph::utils::OnScopeExit{[&] {
    if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs("query-dump-s2");
    }
    std::filesystem::remove_all(config.durability.storage_directory);
  }};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt) << "Failed to access db";
  auto &db_acc = *db_acc_opt;
  ASSERT_TRUE(db_acc->GetStorageMode() == (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL))
      << "Wrong storage mode!";

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context(memgraph::query::InterpreterConfig{}, nullptr, repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt
#endif
  );
  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    const auto &results = stream.GetResults();
    // Indices and constraints are 4 queries and there must be at least one more
    // query for the data.
    ASSERT_GE(results.size(), 5);
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      spdlog::debug("Query: {}", item[0].ValueString());
      Execute(&interpreter_context, db_acc, item[0].ValueString());
    }
  }
  ASSERT_EQ(GetState(this->db->storage()), db_initial_state);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExecuteDumpDatabase) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  {
    auto stream = Execute(&this->context, this->db, "DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    EXPECT_EQ(header[0], "QUERY");
    EXPECT_EQ(results.size(), 4U);
    for (const auto &item : results) {
      EXPECT_EQ(item.size(), 1);
      EXPECT_TRUE(item[0].IsString());
    }
    EXPECT_EQ(results[0][0].ValueString(), "CREATE (:__mg_vertex__ {__mg_id__: 0});");
    EXPECT_EQ(results[1][0].ValueString(), "CREATE INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[2][0].ValueString(), "DROP INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[3][0].ValueString(), "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;");
  }
}

class StatefulInterpreter {
 public:
  explicit StatefulInterpreter(memgraph::query::InterpreterContext *context, memgraph::dbms::DatabaseAccess db)
      : context_(context), interpreter_(context_, db) {
    memgraph::query::AllowEverythingAuthChecker auth_checker;
    interpreter_.SetUser(auth_checker.GenQueryUser(std::nullopt, {}));
  }

  auto Execute(const std::string &query) {
    ResultStreamFaker stream(interpreter_.current_db_.db_acc_->get()->storage());

    auto [header, _1, qid, _2] = interpreter_.Prepare(query, memgraph::query::no_params_fn, {});
    stream.Header(header);
    auto summary = interpreter_.PullAll(&stream);
    stream.Summary(summary);

    return stream;
  }

 private:
  memgraph::query::InterpreterContext *context_;
  memgraph::query::Interpreter interpreter_;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExecuteDumpDatabaseInMulticommandTransaction) {
  StatefulInterpreter interpreter(&this->context, this->db);

  // Begin the transaction before the vertex is created.
  interpreter.Execute("BEGIN");

  // Verify that nothing is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    ASSERT_EQ(header[0], "QUERY");
    ASSERT_EQ(results.size(), 0U);
  }

  // Create the vertex.
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  // Verify that nothing is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    ASSERT_EQ(header[0], "QUERY");
    ASSERT_EQ(results.size(), 0U);
  }

  // Rollback the transaction.
  interpreter.Execute("ROLLBACK");

  // Start a new transaction, this transaction should see the vertex.
  interpreter.Execute("BEGIN");

  // Verify that the vertex is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    EXPECT_EQ(header[0], "QUERY");
    EXPECT_EQ(results.size(), 4U);
    for (const auto &item : results) {
      EXPECT_EQ(item.size(), 1);
      EXPECT_TRUE(item[0].IsString());
    }
    EXPECT_EQ(results[0][0].ValueString(), "CREATE (:__mg_vertex__ {__mg_id__: 0});");
    EXPECT_EQ(results[1][0].ValueString(), "CREATE INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[2][0].ValueString(), "DROP INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[3][0].ValueString(), "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;");
  }

  // Rollback the transaction.
  interpreter.Execute("ROLLBACK");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultiplePartialPulls) {
  {
    // Create indices
    {
      auto unique_acc = this->db->UniqueAccess();
      ASSERT_FALSE(
          unique_acc
              ->CreateIndex(this->db->storage()->NameToLabel("PERSON"), {this->db->storage()->NameToProperty("name")})
              .HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      ASSERT_FALSE(unique_acc
                       ->CreateIndex(this->db->storage()->NameToLabel("PERSON"),
                                     {this->db->storage()->NameToProperty("surname")})
                       .HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }

    // Create existence constraints
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                       this->db->storage()->NameToProperty("name"));
      ASSERT_FALSE(res.HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                       this->db->storage()->NameToProperty("surname"));
      ASSERT_FALSE(res.HasError());
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }

    // Create unique constraints
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                    {this->db->storage()->NameToProperty("name")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                    {this->db->storage()->NameToProperty("surname")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
      ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
    }

    auto dba = this->db->Access();
    auto name_id = dba->NameToProperty("name");
    auto surname_id = dba->NameToProperty("surname");
    auto p1 = CreateVertex(dba.get(), {"PERSON"},
                           {{name_id, memgraph::storage::PropertyValue("Person1")},
                            {surname_id, memgraph::storage::PropertyValue("Unique1")}},
                           false);
    auto p2 = CreateVertex(dba.get(), {"PERSON"},
                           {{name_id, memgraph::storage::PropertyValue("Person2")},
                            {surname_id, memgraph::storage::PropertyValue("Unique2")}},
                           false);
    auto p3 = CreateVertex(dba.get(), {"PERSON"},
                           {{name_id, memgraph::storage::PropertyValue("Person3")},
                            {surname_id, memgraph::storage::PropertyValue("Unique3")}},
                           false);
    auto p4 = CreateVertex(dba.get(), {"PERSON"},
                           {{name_id, memgraph::storage::PropertyValue("Person4")},
                            {surname_id, memgraph::storage::PropertyValue("Unique4")}},
                           false);
    auto p5 = CreateVertex(dba.get(), {"PERSON"},
                           {{name_id, memgraph::storage::PropertyValue("Person5")},
                            {surname_id, memgraph::storage::PropertyValue("Unique5")}},
                           false);
    CreateEdge(dba.get(), &p1, &p2, "REL", {}, false);
    CreateEdge(dba.get(), &p1, &p3, "REL", {}, false);
    CreateEdge(dba.get(), &p4, &p5, "REL", {}, false);
    CreateEdge(dba.get(), &p2, &p5, "REL", {}, false);
    ASSERT_FALSE(dba->PrepareForCommitPhase().HasError());
  }

  ResultStreamFaker stream(this->db->storage());
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  auto acc = this->db->Access();
  memgraph::query::DbAccessor dba(acc.get());

  memgraph::query::PullPlanDump pullPlan{&dba, this->db};

  auto offset_index = 0U;
  auto check_next = [&](const std::string &expected_row) mutable {
    pullPlan.Pull(&query_stream, 1);
    const auto &results{stream.GetResults()};
    ASSERT_EQ(results.size(), offset_index + 1);
    VerifyQueries({results.begin() + offset_index, results.end()}, expected_row);
    ++offset_index;
  };

  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 0, `name`: "Person1", `surname`: "Unique1"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 1, `name`: "Person2", `surname`: "Unique2"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 2, `name`: "Person3", `surname`: "Unique3"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 3, `name`: "Person4", `surname`: "Unique4"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 4, `name`: "Person5", `surname`: "Unique5"});)r");
  check_next(kCreateInternalIndex);

  pullPlan.Pull(&query_stream, 4);
  const auto edge_results = stream.GetResults();
  /// NOTE: For disk storage, the order of returned edges isn't guaranteed so we check them together and we guarantee
  /// the order by sorting.
  VerifyQueries({edge_results.end() - 4, edge_results.end()},
                "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND v.__mg_id__ = 1 CREATE "
                "(u)-[:`REL`]->(v);",
                "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND v.__mg_id__ = 2 CREATE "
                "(u)-[:`REL`]->(v);",
                "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 4 CREATE "
                "(u)-[:`REL`]->(v);",
                "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 3 AND v.__mg_id__ = 4 CREATE "
                "(u)-[:`REL`]->(v);");
  offset_index += 4;

  check_next(kDropInternalIndex);
  check_next(kRemoveInternalLabelProperty);
  check_next("CREATE INDEX ON :`PERSON`(`name`);");
  check_next("CREATE INDEX ON :`PERSON`(`surname`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT EXISTS (u.`name`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT EXISTS (u.`surname`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`name` IS UNIQUE;");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`surname` IS UNIQUE;");
}

TYPED_TEST(DumpTest, DumpDatabaseWithTriggers) {
  auto acc = this->db->storage()->Access();
  memgraph::query::DbAccessor dba(acc.get());
  memgraph::utils::SkipList<memgraph::query::QueryCacheEntry> ast_cache;
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  memgraph::query::InterpreterConfig::Query query_config;
  memgraph::storage::ExternalPropertyValue::map_t props;

  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_vcreate";
    const std::string trigger_statement = "UNWIND createdVertices AS newNodes SET newNodes.created = timestamp()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::VERTEX_CREATE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::AFTER_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_vupdate";
    const std::string trigger_statement = "CREATE (:DummyUpdate)";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::VERTEX_UPDATE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::BEFORE_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_vdelete";
    const std::string trigger_statement = "CREATE (:DummyDelete)";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::VERTEX_DELETE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::AFTER_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_ecreate";
    const std::string trigger_statement = "CREATE ()-[:DummyCreate]->()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::EDGE_CREATE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::BEFORE_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_eupdate";
    const std::string trigger_statement = "CREATE ()-[:DummyUpdate]->()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::EDGE_UPDATE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::BEFORE_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_edelete";
    const std::string trigger_statement = "CREATE ()-[:DummyDelete]->()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::EDGE_DELETE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::AFTER_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_any";
    const std::string trigger_statement = "CREATE ()-[:Any]->()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::ANY;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::BEFORE_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "trigger_on_any_after";
    const std::string trigger_statement = "CREATE ()-[:Any]->()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::ANY;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::AFTER_COMMIT;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, auth_checker.GenQueryUser(std::nullopt, {}),
                              memgraph::dbms::kDefaultDB);
  }
  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    { memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db); }
    VerifyQueries(
        stream.GetResults(),
        "CREATE TRIGGER trigger_on_vcreate ON () CREATE AFTER COMMIT EXECUTE UNWIND createdVertices AS newNodes "
        "SET newNodes.created = timestamp();",
        "CREATE TRIGGER trigger_on_vupdate ON () UPDATE BEFORE COMMIT EXECUTE CREATE (:DummyUpdate);",
        "CREATE TRIGGER trigger_on_vdelete ON () DELETE AFTER COMMIT EXECUTE CREATE (:DummyDelete);",
        "CREATE TRIGGER trigger_on_ecreate ON --> CREATE BEFORE COMMIT EXECUTE CREATE ()-[:DummyCreate]->();",
        "CREATE TRIGGER trigger_on_eupdate ON --> UPDATE BEFORE COMMIT EXECUTE CREATE ()-[:DummyUpdate]->();",
        "CREATE TRIGGER trigger_on_edelete ON --> DELETE AFTER COMMIT EXECUTE CREATE ()-[:DummyDelete]->();",
        "CREATE TRIGGER trigger_on_any BEFORE COMMIT EXECUTE CREATE ()-[:Any]->();",
        "CREATE TRIGGER trigger_on_any_after AFTER COMMIT EXECUTE CREATE ()-[:Any]->();");
  }
}

TYPED_TEST(DumpTest, DumpTypeConstraints) {
  if (this->config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    GTEST_SKIP() << "Type constraints are not implemented for on-disk";
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                     this->db->storage()->NameToProperty("name"));
    ASSERT_FALSE(res.HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                  {this->db->storage()->NameToProperty("name")});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateTypeConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                this->db->storage()->NameToProperty("name"),
                                                memgraph::storage::TypeConstraintKind::INTEGER);
    ASSERT_FALSE(res.HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateTypeConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                this->db->storage()->NameToProperty("surname"),
                                                memgraph::storage::TypeConstraintKind::STRING);
    ASSERT_FALSE(res.HasError());
    ASSERT_FALSE(unique_acc->PrepareForCommitPhase().HasError());
  }

  ResultStreamFaker stream(this->db->storage());
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  {
    auto acc = this->db->Access();
    memgraph::query::DbAccessor dba(acc.get());
    memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
  }
  VerifyQueries(stream.GetResults(), "CREATE CONSTRAINT ON (u:`PERSON`) ASSERT EXISTS (u.`name`);",
                "CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`name` IS UNIQUE;",
                "CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`name` IS TYPED INTEGER;",
                "CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`surname` IS TYPED STRING;");
}
