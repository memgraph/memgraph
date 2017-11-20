#include <cstdio>
#include <experimental/filesystem>
#include <experimental/optional>
#include <functional>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "durability/version.hpp"
#include "utils/string.hpp"

DECLARE_int32(wal_flush_interval_millis);
DECLARE_int32(wal_rotate_ops_count);

namespace fs = std::experimental::filesystem;

// Helper class for performing random CRUD ops on a database.
class DbGenerator {
  static constexpr int kLabelCount = 3;
  static constexpr int kPropertyCount = 4;
  static constexpr int kEdgeTypeCount = 2;

  auto Label(int i) { return dba_.Label("label" + std::to_string(i)); }
  auto Property(int i) { return dba_.Property("property" + std::to_string(i)); }
  auto EdgeType(int i) {
    return dba_.EdgeType("edge_type" + std::to_string(i));
  }

 public:
  DbGenerator(GraphDbAccessor &dba) : dba_(dba) {}

  void BuildIndex(int seq_number) {
    dba_.BuildIndex(Label(seq_number % kLabelCount),
                    Property(seq_number % kPropertyCount));
  }

  EdgeAccessor RandomEdge(bool remove_from_ids = false) {
    return *dba_.FindEdge(RandomElement(edge_ids_, remove_from_ids), true);
  }

  VertexAccessor RandomVertex(bool remove_from_ids = false) {
    return *dba_.FindVertex(RandomElement(vertex_ids_, remove_from_ids), true);
  }

  VertexAccessor InsertVertex() {
    auto vertex = dba_.InsertVertex();
    vertex_ids_.emplace_back(vertex.id());
    return vertex;
  }

  void DetachRemoveVertex() {
    auto vertex = RandomVertex(true);
    dba_.RemoveVertex(vertex);
  }

  EdgeAccessor InsertEdge() {
    auto from = RandomVertex();
    auto to = RandomVertex();
    auto edge = dba_.InsertEdge(from, to, EdgeType(RandomInt(kEdgeTypeCount)));
    edge_ids_.emplace_back(edge.id());
    return edge;
  }

  void RemoveEdge() {
    auto edge = RandomEdge(true);
    dba_.RemoveEdge(edge);
  }

  void SetVertexProperty() {
    auto vertex = RandomVertex();
    vertex.PropsSet(Property(RandomInt(kPropertyCount)), RandomValue());
  }

  void EraseVertexProperty() {
    auto v = RandomVertex();
    for (int i = 0; i < kPropertyCount; i++) v.PropsErase(Property(i));
  }

  void ClearVertexProperties() { RandomVertex().PropsClear(); }

  void SetEdgeProperty() {
    auto edge = RandomEdge();
    edge.PropsSet(Property(RandomInt(kPropertyCount)), RandomValue());
  }

  void EraseEdgeProperty() {
    auto e = RandomEdge();
    for (int i = 0; i < kPropertyCount; i++) e.PropsErase(Property(i));
  }

  void ClearEdgeProperties() { RandomEdge().PropsClear(); }

  void AddLabel() {
    auto vertex = RandomVertex();
    vertex.add_label(Label(RandomInt(kLabelCount)));
  }

  void ClearLabels() {
    auto vertex = RandomVertex();
    auto labels = vertex.labels();
    for (auto label : labels) vertex.remove_label(label);
  }

 private:
  GraphDbAccessor &dba_;
  std::vector<int64_t> vertex_ids_;
  std::vector<int64_t> edge_ids_;

  std::mt19937 gen_{std::random_device{}()};
  std::uniform_real_distribution<> rand_{0.0, 1.0};

  int64_t RandomElement(std::vector<int64_t> &collection, bool remove = false) {
    DCHECK(!collection.empty()) << "Random element from empty collection";
    int64_t id = RandomInt(collection.size());
    int64_t r_val = collection[id];
    if (remove) {
      collection[id] = collection.back();
      collection.resize(collection.size() - 1);
    }

    return r_val;
  }

  int64_t RandomInt(int64_t upper_bound) { return rand_(gen_) * upper_bound; }

  PropertyValue RandomValue() {
    switch (RandomInt(3)) {
      case 0:
        return rand_(gen_);  // Float
      case 1:
        return RandomInt(1000);
      case 2:
        return rand_(gen_) < 0.5;
      default:
        LOG(FATAL) << "Unsupported random value";
    }
  }
};

/** Returns true if the given databases have the same contents (indices,
 * vertices and edges). */
void CompareDbs(GraphDb &a, GraphDb &b) {
  GraphDbAccessor dba_a(a);
  GraphDbAccessor dba_b(b);

  {
    auto index_a = dba_a.IndexInfo();
    auto index_b = dba_b.IndexInfo();
    EXPECT_TRUE(
        index_a.size() == index_b.size() &&
        std::is_permutation(index_a.begin(), index_a.end(), index_b.begin()))
        << "Indexes not equal [" << utils::Join(index_a, ", ") << "] != ["
        << utils::Join(index_b, ", ");
  }

  auto is_permutation_props = [&dba_a, &dba_b](const auto &p1, const auto p2) {
    return p1.size() == p2.size() &&
           std::is_permutation(
               p1.begin(), p1.end(), p2.begin(),
               [&dba_a, &dba_b](const auto &p1, const auto &p2) {
                 return dba_a.PropertyName(p1.first) ==
                            dba_b.PropertyName(p2.first) &&
                        query::TypedValue::BoolEqual{}(p1.second, p2.second);
               });
  };

  {
    int vertices_a_count = 0;
    for (auto v_a : dba_a.Vertices(false)) {
      vertices_a_count++;
      auto v_b = dba_b.FindVertex(v_a.id(), false);
      ASSERT_TRUE(v_b) << "Vertex not found, id: " << v_a.id();
      ASSERT_EQ(v_a.labels().size(), v_b->labels().size());
      EXPECT_TRUE(std::is_permutation(
          v_a.labels().begin(), v_a.labels().end(), v_b->labels().begin(),
          [&dba_a, &dba_b](const auto &la, const auto &lb) {
            return dba_a.LabelName(la) == dba_b.LabelName(lb);
          }));
      EXPECT_TRUE(is_permutation_props(v_a.Properties(), v_b->Properties()));
    }
    auto vertices_b = dba_b.Vertices(false);
    EXPECT_EQ(std::distance(vertices_b.begin(), vertices_b.end()),
              vertices_a_count);
  }
  {
    int edges_a_count = 0;
    for (auto e_a : dba_a.Edges(false)) {
      edges_a_count++;
      auto e_b = dba_b.FindEdge(e_a.id(), false);
      ASSERT_TRUE(e_b);
      ASSERT_TRUE(e_b) << "Edge not found, id: " << e_a.id();
      EXPECT_EQ(dba_a.EdgeTypeName(e_a.EdgeType()),
                dba_b.EdgeTypeName(e_b->EdgeType()));
      EXPECT_EQ(e_a.from().id(), e_b->from().id());
      EXPECT_EQ(e_a.to().id(), e_b->to().id());
      EXPECT_TRUE(is_permutation_props(e_a.Properties(), e_b->Properties()));
    }
    auto edges_b = dba_b.Edges(false);
    EXPECT_EQ(std::distance(edges_b.begin(), edges_b.end()), edges_a_count);
  }
}

const fs::path kDurabilityDir =
    fs::temp_directory_path() / "MG_test_unit_durability";
const fs::path kSnapshotDir = kDurabilityDir / durability::kSnapshotDir;
const fs::path kWalDir = kDurabilityDir / durability::kWalDir;

void CleanDurability() {
  if (fs::exists(kDurabilityDir)) fs::remove_all(kDurabilityDir);
}

std::vector<fs::path> DirFiles(fs::path dir) {
  std::vector<fs::path> files;
  if (fs::exists(dir))
    for (auto &file : fs::directory_iterator(dir)) files.push_back(file.path());
  return files;
}

auto DbConfig() {
  GraphDb::Config config;
  config.durability_enabled = false;
  config.durability_directory = kDurabilityDir;
  config.snapshot_on_exit = false;
  config.db_recover_on_startup = false;

  return config;
}

void MakeSnapshot(GraphDb &db, int snapshot_max_retained = -1) {
  GraphDbAccessor dba(db);
  ASSERT_TRUE(
      durability::MakeSnapshot(dba, kDurabilityDir, snapshot_max_retained));
  dba.Commit();
}

fs::path GetLastFile(fs::path dir) {
  std::vector<fs::path> files = DirFiles(dir);
  CHECK(static_cast<int>(files.size()) > 0) << "No files in folder.";
  return *std::max_element(files.begin(), files.end());
}

void MakeDb(GraphDbAccessor &dba, int scale, std::vector<int> indices = {}) {
  DbGenerator generator{dba};
  for (int i = 0; i < scale; i++) generator.InsertVertex();
  for (int i = 0; i < scale * 2; i++) generator.InsertEdge();
  // Give the WAL some time to flush, we're pumping ops fast here.
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  for (int i = 0; i < scale * 3; i++) {
    generator.SetVertexProperty();
    generator.SetEdgeProperty();
    generator.AddLabel();
    if (i % 500 == 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
  for (int i = 0; i < scale / 2; i++) {
    generator.ClearLabels();
    generator.EraseEdgeProperty();
    generator.EraseVertexProperty();
    generator.ClearEdgeProperties();
    generator.ClearVertexProperties();
    if (i % 500 == 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
  for (auto index : indices) generator.BuildIndex(index);
}

void MakeDb(GraphDb &db, int scale, std::vector<int> indices = {}) {
  GraphDbAccessor dba{db};
  MakeDb(dba, scale, indices);
  dba.Commit();
}

class Durability : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_wal_rotate_ops_count = 1000;
    CleanDurability();
  }

  void TearDown() override { CleanDurability(); }
};

TEST_F(Durability, WalEncoding) {
  {
    auto config = DbConfig();
    config.durability_enabled = true;
    GraphDb db{config};
    GraphDbAccessor dba(db);
    auto v0 = dba.InsertVertex();
    ASSERT_EQ(v0.id(), 0);
    v0.add_label(dba.Label("l0"));
    v0.PropsSet(dba.Property("p0"), 42);
    auto v1 = dba.InsertVertex();
    ASSERT_EQ(v1.id(), 1);
    auto e0 = dba.InsertEdge(v0, v1, dba.EdgeType("et0"));
    ASSERT_EQ(e0.id(), 0);
    e0.PropsSet(dba.Property("p0"), std::vector<PropertyValue>{1, 2, 3});
    dba.BuildIndex(dba.Label("l1"), dba.Property("p1"));
    dba.Commit();
  }

  // Sleep to ensure the WAL gets flushed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  HashedFileReader reader;
  ASSERT_EQ(DirFiles(kWalDir).size(), 1);
  ASSERT_TRUE(reader.Open(GetLastFile(kWalDir)));
  communication::bolt::Decoder<HashedFileReader> decoder{reader};
  std::vector<durability::WriteAheadLog::Op> ops;
  while (true) {
    auto op = durability::WriteAheadLog::Op::Decode(reader, decoder);
    if (op) {
      ops.emplace_back(*op);
    } else {
      break;
    }
  }
  reader.Close();
  ASSERT_EQ(ops.size(), 13);

  using Type = durability::WriteAheadLog::Op::Type;
  EXPECT_EQ(ops[0].type_, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(ops[0].transaction_id_, 1);
  EXPECT_EQ(ops[1].type_, Type::CREATE_VERTEX);
  EXPECT_EQ(ops[1].transaction_id_, 1);
  EXPECT_EQ(ops[1].vertex_id_, 0);
  EXPECT_EQ(ops[2].type_, Type::ADD_LABEL);
  EXPECT_EQ(ops[2].transaction_id_, 1);
  EXPECT_EQ(ops[2].label_, "l0");
  EXPECT_EQ(ops[3].type_, Type::SET_PROPERTY_VERTEX);
  EXPECT_EQ(ops[3].transaction_id_, 1);
  EXPECT_EQ(ops[3].vertex_id_, 0);
  EXPECT_EQ(ops[3].property_, "p0");
  EXPECT_EQ(ops[3].value_.type(), PropertyValue::Type::Int);
  EXPECT_EQ(ops[3].value_.Value<int64_t>(), 42);
  EXPECT_EQ(ops[4].type_, Type::CREATE_VERTEX);
  EXPECT_EQ(ops[4].transaction_id_, 1);
  EXPECT_EQ(ops[4].vertex_id_, 1);
  EXPECT_EQ(ops[5].type_, Type::CREATE_EDGE);
  EXPECT_EQ(ops[5].transaction_id_, 1);
  EXPECT_EQ(ops[5].edge_id_, 0);
  EXPECT_EQ(ops[5].vertex_from_id_, 0);
  EXPECT_EQ(ops[5].vertex_to_id_, 1);
  EXPECT_EQ(ops[5].edge_type_, "et0");
  EXPECT_EQ(ops[6].type_, Type::SET_PROPERTY_EDGE);
  EXPECT_EQ(ops[6].transaction_id_, 1);
  EXPECT_EQ(ops[6].edge_id_, 0);
  EXPECT_EQ(ops[6].property_, "p0");
  EXPECT_EQ(ops[6].value_.type(), PropertyValue::Type::List);
  // The next four ops are the BuildIndex internal transactions.
  EXPECT_EQ(ops[7].type_, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(ops[8].type_, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(ops[9].type_, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(ops[10].type_, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(ops[11].type_, Type::BUILD_INDEX);
  EXPECT_EQ(ops[11].label_, "l1");
  EXPECT_EQ(ops[11].property_, "p1");
  EXPECT_EQ(ops[12].type_, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(ops[12].transaction_id_, 1);
}

TEST_F(Durability, SnapshotEncoding) {
  {
    GraphDb db{DbConfig()};
    GraphDbAccessor dba(db);
    auto v0 = dba.InsertVertex();
    ASSERT_EQ(v0.id(), 0);
    v0.add_label(dba.Label("l0"));
    v0.PropsSet(dba.Property("p0"), 42);
    auto v1 = dba.InsertVertex();
    ASSERT_EQ(v1.id(), 1);
    v1.add_label(dba.Label("l0"));
    v1.add_label(dba.Label("l1"));
    auto v2 = dba.InsertVertex();
    ASSERT_EQ(v2.id(), 2);
    v2.PropsSet(dba.Property("p0"), true);
    v2.PropsSet(dba.Property("p1"), "Johnny");
    auto e0 = dba.InsertEdge(v0, v1, dba.EdgeType("et0"));
    ASSERT_EQ(e0.id(), 0);
    e0.PropsSet(dba.Property("p0"), std::vector<PropertyValue>{1, 2, 3});
    auto e1 = dba.InsertEdge(v2, v1, dba.EdgeType("et1"));
    ASSERT_EQ(e1.id(), 1);
    dba.BuildIndex(dba.Label("l1"), dba.Property("p1"));
    dba.Commit();
    MakeSnapshot(db);
  }

  auto snapshot = GetLastFile(kSnapshotDir);
  HashedFileReader buffer;
  communication::bolt::Decoder<HashedFileReader> decoder(buffer);

  int64_t vertex_count, edge_count;
  uint64_t hash;
  ASSERT_TRUE(buffer.Open(snapshot));
  ASSERT_TRUE(
      durability::ReadSnapshotSummary(buffer, vertex_count, edge_count, hash));
  ASSERT_EQ(vertex_count, 3);
  ASSERT_EQ(edge_count, 2);

  auto magic_number = durability::kMagicNumber;
  buffer.Read(magic_number.data(), magic_number.size());
  ASSERT_EQ(magic_number, durability::kMagicNumber);

  communication::bolt::DecodedValue dv;
  decoder.ReadValue(&dv);
  ASSERT_EQ(dv.ValueInt(), durability::kVersion);
  // Transaction ID.
  decoder.ReadValue(&dv);
  ASSERT_TRUE(dv.IsInt());
  // Transactional snapshot.
  decoder.ReadValue(&dv);
  ASSERT_TRUE(dv.IsList());
  // Label property indices.
  decoder.ReadValue(&dv);
  ASSERT_EQ(dv.ValueList().size(), 2);
  EXPECT_EQ(dv.ValueList()[0].ValueString(), "l1");
  EXPECT_EQ(dv.ValueList()[1].ValueString(), "p1");

  std::map<int64_t, communication::bolt::DecodedVertex> decoded_vertices;
  std::map<int64_t, communication::bolt::DecodedEdge> decoded_edges;

  // Decode vertices.
  for (int i = 0; i < vertex_count; ++i) {
    decoder.ReadValue(&dv);
    ASSERT_EQ(dv.type(), communication::bolt::DecodedValue::Type::Vertex);
    auto &vertex = dv.ValueVertex();
    decoded_vertices.emplace(vertex.id, vertex);
  }
  ASSERT_EQ(decoded_vertices.size(), 3);
  ASSERT_EQ(decoded_vertices[0].labels.size(), 1);
  EXPECT_EQ(decoded_vertices[0].labels[0], "l0");
  ASSERT_EQ(decoded_vertices[0].properties.size(), 1);
  EXPECT_EQ(decoded_vertices[0].properties["p0"].ValueInt(), 42);
  EXPECT_EQ(decoded_vertices[1].labels.size(), 2);
  EXPECT_EQ(decoded_vertices[1].properties.size(), 0);
  EXPECT_EQ(decoded_vertices[2].labels.size(), 0);
  EXPECT_EQ(decoded_vertices[2].properties.size(), 2);

  // Decode edges.
  for (int i = 0; i < edge_count; ++i) {
    decoder.ReadValue(&dv);
    ASSERT_EQ(dv.type(), communication::bolt::DecodedValue::Type::Edge);
    auto &edge = dv.ValueEdge();
    decoded_edges.emplace(edge.id, edge);
  }
  ASSERT_EQ(decoded_edges.size(), 2);
  ASSERT_EQ(decoded_edges[0].from, 0);
  ASSERT_EQ(decoded_edges[0].to, 1);
  ASSERT_EQ(decoded_edges[0].type, "et0");
  ASSERT_EQ(decoded_edges[0].properties.size(), 1);
  ASSERT_EQ(decoded_edges[1].from, 2);
  ASSERT_EQ(decoded_edges[1].to, 1);
  ASSERT_EQ(decoded_edges[1].type, "et1");
  ASSERT_EQ(decoded_edges[1].properties.size(), 0);

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  buffer.ReadType(vertex_count);
  buffer.ReadType(edge_count);
  buffer.Close();

  EXPECT_EQ(buffer.hash(), hash);
}

TEST_F(Durability, SnapshotRecovery) {
  GraphDb db{DbConfig()};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeDb(db, 300, {3, 4});
  MakeSnapshot(db);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    GraphDb recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, WalRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  GraphDb db{config};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeDb(db, 300, {3, 4});

  // Sleep to ensure the WAL gets flushed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(DirFiles(kSnapshotDir).size(), 0);
  EXPECT_GT(DirFiles(kWalDir).size(), 1);

  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    GraphDb recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, SnapshotAndWalRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  GraphDb db{config};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeSnapshot(db);
  MakeDb(db, 300, {3, 4});
  MakeDb(db, 300);
  MakeDb(db, 300, {5});

  // Sleep to ensure the WAL gets flushed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(DirFiles(kSnapshotDir).size(), 1);
  EXPECT_GT(DirFiles(kWalDir).size(), 1);

  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    GraphDb recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, SnapshotAndWalRecoveryAfterComplexTxSituation) {
  auto config = DbConfig();
  config.durability_enabled = true;
  GraphDb db{config};

  // The first transaction modifies and commits.
  GraphDbAccessor dba_1{db};
  MakeDb(dba_1, 100);
  dba_1.Commit();

  // The second transaction will commit after snapshot.
  GraphDbAccessor dba_2{db};
  MakeDb(dba_2, 100);

  // The third transaction modifies and commits.
  GraphDbAccessor dba_3{db};
  MakeDb(dba_3, 100);
  dba_3.Commit();

  MakeSnapshot(db);  // Snapshooter takes the fourth transaction.
  dba_2.Commit();

  // The fifth transaction starts and commits after snapshot.
  GraphDbAccessor dba_5{db};
  MakeDb(dba_5, 100);
  dba_5.Commit();

  // The sixth transaction will not commit at all.
  GraphDbAccessor dba_6{db};
  MakeDb(dba_6, 100);

  auto VisibleVertexCount = [](GraphDb &db) {
    GraphDbAccessor dba{db};
    auto vertices = dba.Vertices(false);
    return std::distance(vertices.begin(), vertices.end());
  };
  ASSERT_EQ(VisibleVertexCount(db), 400);

  // Sleep to ensure the WAL gets flushed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(DirFiles(kSnapshotDir).size(), 1);
  EXPECT_GT(DirFiles(kWalDir).size(), 1);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    GraphDb recovered{recovered_config};
    ASSERT_EQ(VisibleVertexCount(recovered), 400);
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, NoWalDuringRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  GraphDb db{config};
  MakeDb(db, 300, {0, 1, 2});

  // Sleep to ensure the WAL gets flushed.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto wal_files_before = DirFiles(kWalDir);
  ASSERT_GT(wal_files_before.size(), 3);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    GraphDb recovered{recovered_config};
    CompareDbs(db, recovered);
    auto wal_files_after = DirFiles(kWalDir);
    EXPECT_EQ(wal_files_after.size(), wal_files_before.size());
  }
}

TEST_F(Durability, SnapshotRetention) {
  GraphDb db{DbConfig()};
  for (auto &pair : {std::pair<int, int>{5, 10}, {5, 3}, {7, -1}}) {
    CleanDurability();
    int count, retain;
    std::tie(count, retain) = pair;

    // Track the added snapshots to ensure the correct ones are pruned.
    std::unordered_set<std::string> snapshots;
    for (int i = 0; i < count; ++i) {
      MakeSnapshot(db, retain);
      auto latest = GetLastFile(kSnapshotDir);
      snapshots.emplace(GetLastFile(kSnapshotDir));
      // Ensures that the latest snapshot was not in the snapshots collection
      // before. Thus ensures that it wasn't pruned.
      EXPECT_EQ(snapshots.size(), i + 1);
    }

    EXPECT_EQ(DirFiles(kSnapshotDir).size(),
              std::min(count, retain < 0 ? count : retain));
  };
}

TEST_F(Durability, SnapshotOnExit) {
  {
    auto config = DbConfig();
    config.snapshot_on_exit = true;
    GraphDb graph_db{config};
  }
  EXPECT_EQ(DirFiles(kSnapshotDir).size(), 1);
}
