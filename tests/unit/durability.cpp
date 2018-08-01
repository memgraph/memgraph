#include <experimental/filesystem>
#include <experimental/optional>
#include <functional>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "database/distributed_graph_db.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/paths.hpp"
#include "durability/recovery.hpp"
#include "durability/snapshooter.hpp"
#include "durability/snapshot_decoder.hpp"
#include "durability/version.hpp"
#include "utils/string.hpp"

DECLARE_int32(wal_flush_interval_millis);
DECLARE_int32(wal_rotate_deltas_count);

DECLARE_string(durability_directory);

using namespace std::literals::chrono_literals;

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
  DbGenerator(database::GraphDbAccessor &dba) : dba_(dba) {}

  void BuildIndex(int seq_number) {
    dba_.BuildIndex(Label(seq_number % kLabelCount),
                    Property(seq_number % kPropertyCount));
  }

  EdgeAccessor RandomEdge(bool remove_from_ids = false) {
    return dba_.FindEdge(RandomElement(edge_ids_, remove_from_ids), true);
  }

  VertexAccessor RandomVertex(bool remove_from_ids = false) {
    return dba_.FindVertex(RandomElement(vertex_ids_, remove_from_ids), true);
  }

  VertexAccessor InsertVertex() {
    auto vertex = dba_.InsertVertex();
    vertex_ids_.emplace_back(vertex.gid());
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
    edge_ids_.emplace_back(edge.gid());
    return edge;
  }

  EdgeAccessor InsertCycleEdge() {
    auto vertex = RandomVertex();
    auto edge =
        dba_.InsertEdge(vertex, vertex, EdgeType(RandomInt(kEdgeTypeCount)));
    edge_ids_.emplace_back(edge.gid());
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
  database::GraphDbAccessor &dba_;
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

/** Checks if the given databases have the same contents (indices,
 * vertices and edges). */
void CompareDbs(database::GraphDb &a, database::GraphDb &b) {
  auto dba_a = a.Access();
  auto dba_b = b.Access();

  {
    auto index_a = dba_a->IndexInfo();
    auto index_b = dba_b->IndexInfo();
    EXPECT_TRUE(
        index_a.size() == index_b.size() &&
        std::is_permutation(index_a.begin(), index_a.end(), index_b.begin()))
        << "Indexes not equal [" << utils::Join(index_a, ", ") << "] != ["
        << utils::Join(index_b, ", ");
  }

  auto is_permutation_props = [&dba_a, &dba_b](const auto &p1_id,
                                               const auto &p2_id) {
    std::vector<std::pair<std::string, query::TypedValue>> p1;
    std::vector<std::pair<std::string, query::TypedValue>> p2;

    for (auto x : p1_id) p1.push_back({dba_a->PropertyName(x.first), x.second});
    for (auto x : p2_id) p2.push_back({dba_b->PropertyName(x.first), x.second});

    // Don't use a binary predicate which depends on different value getters
    // semantics for two containers because is_permutation might call the
    // predicate with both arguments on the same container
    return p1.size() == p2.size() &&
           std::is_permutation(p1.begin(), p1.end(), p2.begin(),
                               [](const auto &p1, const auto &p2) {
                                 return p1.first == p2.first &&
                                        query::TypedValue::BoolEqual{}(
                                            p1.second, p2.second);
                               });
  };

  {
    int vertices_a_count = 0;
    for (auto v_a : dba_a->Vertices(false)) {
      vertices_a_count++;
      auto v_b = dba_b->FindVertexOptional(v_a.gid(), false);
      ASSERT_TRUE(v_b) << "Vertex not found, id: " << v_a.gid();
      ASSERT_EQ(v_a.labels().size(), v_b->labels().size());
      std::vector<std::string> v_a_labels;
      std::vector<std::string> v_b_labels;
      for (auto x : v_a.labels()) v_a_labels.push_back(dba_a->LabelName(x));
      for (auto x : v_b->labels()) v_b_labels.push_back(dba_b->LabelName(x));
      EXPECT_TRUE(std::is_permutation(v_a_labels.begin(), v_a_labels.end(),
                                      v_b_labels.begin()));
      EXPECT_TRUE(is_permutation_props(v_a.Properties(), v_b->Properties()));
    }
    auto vertices_b = dba_b->Vertices(false);
    EXPECT_EQ(std::distance(vertices_b.begin(), vertices_b.end()),
              vertices_a_count);
  }
  {
    int edges_a_count = 0;
    for (auto e_a : dba_a->Edges(false)) {
      edges_a_count++;
      auto e_b = dba_b->FindEdgeOptional(e_a.gid(), false);
      ASSERT_TRUE(e_b);
      ASSERT_TRUE(e_b) << "Edge not found, id: " << e_a.gid();
      EXPECT_EQ(dba_a->EdgeTypeName(e_a.EdgeType()),
                dba_b->EdgeTypeName(e_b->EdgeType()));
      EXPECT_EQ(e_a.from().gid(), e_b->from().gid());
      EXPECT_EQ(e_a.to().gid(), e_b->to().gid());
      EXPECT_TRUE(is_permutation_props(e_a.Properties(), e_b->Properties()));
    }
    auto edges_b = dba_b->Edges(false);
    EXPECT_EQ(std::distance(edges_b.begin(), edges_b.end()), edges_a_count);
  }
}

std::vector<fs::path> DirFiles(fs::path dir) {
  std::vector<fs::path> files;
  if (fs::exists(dir))
    for (auto &file : fs::directory_iterator(dir)) files.push_back(file.path());
  return files;
}

fs::path GetLastFile(fs::path dir) {
  std::vector<fs::path> files = DirFiles(dir);
  CHECK(static_cast<int>(files.size()) > 0) << "No files in folder.";
  return *std::max_element(files.begin(), files.end());
}

void MakeDb(durability::WriteAheadLog &wal, database::GraphDbAccessor &dba,
            int scale, std::vector<int> indices = {}) {
  DbGenerator generator{dba};
  for (int i = 0; i < scale; i++) generator.InsertVertex();
  for (int i = 0; i < scale * 2; i++) generator.InsertEdge();
  for (int i = 0; i < scale / 2; i++) generator.InsertCycleEdge();
  wal.Flush();

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

void MakeDb(database::GraphDb &db, int scale, std::vector<int> indices = {}) {
  auto dba = db.Access();
  MakeDb(db.wal(), *dba, scale, indices);
  dba->Commit();
}

class Durability : public ::testing::Test {
 protected:
  fs::path tmp_dir_ = fs::temp_directory_path() / "MG_test_unit_durability";
  fs::path durability_dir_;
  fs::path snapshot_dir_;
  fs::path wal_dir_;

  void CleanDurability() {
    if (fs::exists(tmp_dir_)) fs::remove_all(tmp_dir_);
  }

  auto DbConfig() {
    database::Config config;
    config.durability_enabled = false;
    config.durability_directory = durability_dir_;
    config.snapshot_on_exit = false;
    config.db_recover_on_startup = false;

    return config;
  }

  void MakeSnapshot(int worker_id, database::GraphDb &db,
                    int snapshot_max_retained = -1) {
    auto dba = db.Access();
    ASSERT_TRUE(durability::MakeSnapshot(db, *dba, worker_id, durability_dir_,
                                         snapshot_max_retained));
  }

  void SetUp() override {
    durability_dir_ = tmp_dir_ / utils::RandomString(24);
    snapshot_dir_ = durability_dir_ / durability::kSnapshotDir;
    wal_dir_ = durability_dir_ / durability::kWalDir;
    FLAGS_wal_rotate_deltas_count = 1000;
    FLAGS_durability_directory = "MG_test_unit_durability";
    CleanDurability();
  }

  void TearDown() override { CleanDurability(); }
};

// Tests wal encoder to encode correctly non-CRUD deltas, and that all deltas
// are written in the correct order
TEST_F(Durability, WalEncoding) {
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  {
    auto config = DbConfig();
    config.durability_enabled = true;
    database::SingleNode db{config};
    auto dba = db.Access();
    auto v0 = dba->InsertVertex();
    ASSERT_EQ(v0.gid(), gid0);
    v0.add_label(dba->Label("l0"));
    v0.PropsSet(dba->Property("p0"), 42);
    auto v1 = dba->InsertVertex();
    ASSERT_EQ(v1.gid(), gid1);
    auto e0 = dba->InsertEdge(v0, v1, dba->EdgeType("et0"));
    ASSERT_EQ(e0.gid(), gid0);
    e0.PropsSet(dba->Property("p0"), std::vector<PropertyValue>{1, 2, 3});
    dba->BuildIndex(dba->Label("l1"), dba->Property("p1"));
    dba->Commit();

    db.wal().Flush();
  }

  HashedFileReader reader;
  ASSERT_EQ(DirFiles(wal_dir_).size(), 1);
  ASSERT_TRUE(reader.Open(GetLastFile(wal_dir_)));
  communication::bolt::Decoder<HashedFileReader> decoder{reader};
  std::vector<database::StateDelta> deltas;
  while (true) {
    auto delta = database::StateDelta::Decode(reader, decoder);
    if (delta) {
      deltas.emplace_back(*delta);
    } else {
      break;
    }
  }
  reader.Close();
  ASSERT_EQ(deltas.size(), 11);

  using Type = enum database::StateDelta::Type;
  EXPECT_EQ(deltas[0].type, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(deltas[0].transaction_id, 1);
  EXPECT_EQ(deltas[1].type, Type::CREATE_VERTEX);
  EXPECT_EQ(deltas[1].transaction_id, 1);
  EXPECT_EQ(deltas[2].type, Type::ADD_LABEL);
  EXPECT_EQ(deltas[2].transaction_id, 1);
  EXPECT_EQ(deltas[3].type, Type::SET_PROPERTY_VERTEX);
  EXPECT_EQ(deltas[3].transaction_id, 1);
  EXPECT_EQ(deltas[4].type, Type::CREATE_VERTEX);
  EXPECT_EQ(deltas[4].transaction_id, 1);
  EXPECT_EQ(deltas[5].type, Type::CREATE_EDGE);
  EXPECT_EQ(deltas[5].transaction_id, 1);
  EXPECT_EQ(deltas[6].type, Type::SET_PROPERTY_EDGE);
  EXPECT_EQ(deltas[6].transaction_id, 1);
  // The next two deltas are the BuildIndex internal transactions.
  EXPECT_EQ(deltas[7].type, Type::TRANSACTION_BEGIN);
  EXPECT_EQ(deltas[8].type, Type::BUILD_INDEX);
  EXPECT_EQ(deltas[8].label_name, "l1");
  EXPECT_EQ(deltas[8].property_name, "p1");
  EXPECT_EQ(deltas[9].type, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(deltas[10].type, Type::TRANSACTION_COMMIT);
  EXPECT_EQ(deltas[10].transaction_id, 1);
}

TEST_F(Durability, SnapshotEncoding) {
  gid::Generator generator(0);
  auto gid0 = generator.Next();
  auto gid1 = generator.Next();
  auto gid2 = generator.Next();
  {
    database::SingleNode db{DbConfig()};
    auto dba = db.Access();
    auto v0 = dba->InsertVertex();
    ASSERT_EQ(v0.gid(), gid0);
    v0.add_label(dba->Label("l0"));
    v0.PropsSet(dba->Property("p0"), 42);
    auto v1 = dba->InsertVertex();
    ASSERT_EQ(v1.gid(), gid1);
    v1.add_label(dba->Label("l0"));
    v1.add_label(dba->Label("l1"));
    auto v2 = dba->InsertVertex();
    ASSERT_EQ(v2.gid(), gid2);
    v2.PropsSet(dba->Property("p0"), true);
    v2.PropsSet(dba->Property("p1"), "Johnny");
    auto e0 = dba->InsertEdge(v0, v1, dba->EdgeType("et0"));
    ASSERT_EQ(e0.gid(), gid0);
    e0.PropsSet(dba->Property("p0"), std::vector<PropertyValue>{1, 2, 3});
    auto e1 = dba->InsertEdge(v2, v1, dba->EdgeType("et1"));
    ASSERT_EQ(e1.gid(), gid1);
    dba->BuildIndex(dba->Label("l1"), dba->Property("p1"));
    dba->Commit();
    MakeSnapshot(0, db);
  }

  auto snapshot = GetLastFile(snapshot_dir_);
  HashedFileReader buffer;
  durability::SnapshotDecoder<HashedFileReader> decoder(buffer);

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

  communication::bolt::Value dv;
  decoder.ReadValue(&dv);
  ASSERT_EQ(dv.ValueInt(), durability::kVersion);
  // Worker id
  decoder.ReadValue(&dv);
  ASSERT_EQ(dv.ValueInt(), 0);
  // Number of generated vertex ids.
  decoder.ReadValue(&dv);
  ASSERT_TRUE(dv.IsInt());
  // Number of generated edge ids.
  decoder.ReadValue(&dv);
  ASSERT_TRUE(dv.IsInt());
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

  std::map<gid::Gid, durability::SnapshotVertex> decoded_vertices;

  // Decode vertices.
  for (int i = 0; i < vertex_count; ++i) {
    auto vertex = decoder.ReadSnapshotVertex();
    ASSERT_NE(vertex, std::experimental::nullopt);
    decoded_vertices.emplace(vertex->gid, *vertex);
  }
  ASSERT_EQ(decoded_vertices.size(), 3);
  ASSERT_EQ(decoded_vertices[gid0].labels.size(), 1);
  EXPECT_EQ(decoded_vertices[gid0].labels[0], "l0");
  ASSERT_EQ(decoded_vertices[gid0].properties.size(), 1);
  EXPECT_EQ(decoded_vertices[gid0].properties["p0"].ValueInt(), 42);
  EXPECT_EQ(decoded_vertices[gid1].labels.size(), 2);
  EXPECT_EQ(decoded_vertices[gid1].properties.size(), 0);
  EXPECT_EQ(decoded_vertices[gid2].labels.size(), 0);
  EXPECT_EQ(decoded_vertices[gid2].properties.size(), 2);

  std::map<gid::Gid, communication::bolt::Edge> decoded_edges;

  // Decode edges.
  for (int i = 0; i < edge_count; ++i) {
    decoder.ReadValue(&dv);
    ASSERT_EQ(dv.type(), communication::bolt::Value::Type::Edge);
    auto &edge = dv.ValueEdge();
    decoded_edges.emplace(edge.id.AsUint(), edge);
    // Read cypher_id.
    decoder.ReadValue(&dv);
    ASSERT_EQ(dv.type(), communication::bolt::Value::Type::Int);
  }
  EXPECT_EQ(decoded_edges.size(), 2);
  EXPECT_EQ(decoded_edges[gid0].from.AsUint(), gid0);
  EXPECT_EQ(decoded_edges[gid0].to.AsUint(), gid1);
  EXPECT_EQ(decoded_edges[gid0].type, "et0");
  EXPECT_EQ(decoded_edges[gid0].properties.size(), 1);
  EXPECT_EQ(decoded_edges[gid1].from.AsUint(), gid2);
  EXPECT_EQ(decoded_edges[gid1].to.AsUint(), gid1);
  EXPECT_EQ(decoded_edges[gid1].type, "et1");
  EXPECT_EQ(decoded_edges[gid1].properties.size(), 0);

  // Vertex and edge counts are included in the hash. Re-read them to update
  // the hash.
  buffer.ReadType(vertex_count);
  buffer.ReadType(edge_count);
  buffer.Close();

  EXPECT_EQ(buffer.hash(), hash);
}

TEST_F(Durability, SnapshotRecovery) {
  database::SingleNode db{DbConfig()};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeDb(db, 300, {3, 4});
  MakeSnapshot(0, db);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, SnapshotNoVerticesIdRecovery) {
  database::SingleNode db{DbConfig()};
  MakeDb(db, 10);

  // Erase all vertices, this should cause snapshot to not have any more
  // vertices which should make it not change any id after snapshot recovery,
  // but we still have to make sure that the id for generators is recovered
  {
    auto dba = db.Access();
    for (auto vertex : dba->Vertices(false)) dba->RemoveVertex(vertex);
    dba->Commit();
  }

  MakeSnapshot(0, db);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    EXPECT_EQ(db.storage().VertexGenerator().LocalCount(),
              recovered.storage().VertexGenerator().LocalCount());
    EXPECT_EQ(db.storage().EdgeGenerator().LocalCount(),
              recovered.storage().EdgeGenerator().LocalCount());
  }
}

TEST_F(Durability, SnapshotAndWalIdRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  database::SingleNode db{config};
  MakeDb(db, 300);
  MakeSnapshot(0, db);
  MakeDb(db, 300);
  db.wal().Flush();
  ASSERT_EQ(DirFiles(snapshot_dir_).size(), 1);
  EXPECT_GT(DirFiles(wal_dir_).size(), 1);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    EXPECT_EQ(db.storage().VertexGenerator().LocalCount(),
              recovered.storage().VertexGenerator().LocalCount());
    EXPECT_EQ(db.storage().EdgeGenerator().LocalCount(),
              recovered.storage().EdgeGenerator().LocalCount());
  }
}

TEST_F(Durability, OnlyWalIdRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  database::SingleNode db{config};
  MakeDb(db, 300);
  db.wal().Flush();
  ASSERT_EQ(DirFiles(snapshot_dir_).size(), 0);
  EXPECT_GT(DirFiles(wal_dir_).size(), 1);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    EXPECT_EQ(db.storage().VertexGenerator().LocalCount(),
              recovered.storage().VertexGenerator().LocalCount());
    EXPECT_EQ(db.storage().EdgeGenerator().LocalCount(),
              recovered.storage().EdgeGenerator().LocalCount());
  }
}

TEST_F(Durability, WalRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  database::SingleNode db{config};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeDb(db, 300, {3, 4});

  db.wal().Flush();
  ASSERT_EQ(DirFiles(snapshot_dir_).size(), 0);
  EXPECT_GT(DirFiles(wal_dir_).size(), 1);

  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, SnapshotAndWalRecovery) {
  auto config = DbConfig();
  config.durability_enabled = true;
  database::SingleNode db{config};
  MakeDb(db, 300, {0, 1, 2});
  MakeDb(db, 300);
  MakeSnapshot(0, db);
  MakeDb(db, 300, {3, 4});
  MakeDb(db, 300);
  MakeDb(db, 300, {5});

  db.wal().Flush();
  ASSERT_EQ(DirFiles(snapshot_dir_).size(), 1);
  EXPECT_GT(DirFiles(wal_dir_).size(), 1);

  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, SnapshotAndWalRecoveryAfterComplexTxSituation) {
  auto config = DbConfig();
  config.durability_enabled = true;
  database::SingleNode db{config};

  // The first transaction modifies and commits.
  auto dba_1 = db.Access();
  MakeDb(db.wal(), *dba_1, 100);
  dba_1->Commit();

  // The second transaction will commit after snapshot.
  auto dba_2 = db.Access();
  MakeDb(db.wal(), *dba_2, 100);

  // The third transaction modifies and commits.
  auto dba_3 = db.Access();
  MakeDb(db.wal(), *dba_3, 100);
  dba_3->Commit();

  MakeSnapshot(0, db);  // Snapshooter takes the fourth transaction.
  dba_2->Commit();

  // The fifth transaction starts and commits after snapshot.
  auto dba_5 = db.Access();
  MakeDb(db.wal(), *dba_5, 100);
  dba_5->Commit();

  // The sixth transaction will not commit at all.
  auto dba_6 = db.Access();
  MakeDb(db.wal(), *dba_6, 100);

  auto VisibleVertexCount = [](database::GraphDb &db) {
    auto dba = db.Access();
    auto vertices = dba->Vertices(false);
    return std::distance(vertices.begin(), vertices.end());
  };
  ASSERT_EQ(VisibleVertexCount(db), 400);

  db.wal().Flush();

  ASSERT_EQ(DirFiles(snapshot_dir_).size(), 1);
  EXPECT_GT(DirFiles(wal_dir_).size(), 1);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    ASSERT_EQ(VisibleVertexCount(recovered), 400);
    CompareDbs(db, recovered);
  }
}

TEST_F(Durability, NoWalDuringRecovery) {
  size_t wal_files_before;
  {
    auto config = DbConfig();
    config.durability_enabled = true;
    database::SingleNode db{config};
    MakeDb(db, 300, {0, 1, 2});

    db.wal().Flush();
  }
  wal_files_before = DirFiles(wal_dir_).size();
  ASSERT_GT(wal_files_before, 3);
  {
    auto recovered_config = DbConfig();
    recovered_config.db_recover_on_startup = true;
    database::SingleNode recovered{recovered_config};
    EXPECT_EQ(DirFiles(wal_dir_).size(), wal_files_before);
  }
}

TEST_F(Durability, SnapshotRetention) {
  database::SingleNode db{DbConfig()};
  for (auto &pair : {std::pair<int, int>{5, 10}, {5, 3}, {7, -1}}) {
    CleanDurability();
    int count, retain;
    std::tie(count, retain) = pair;

    // Track the added snapshots to ensure the correct ones are pruned.
    std::unordered_set<std::string> snapshots;
    for (int i = 0; i < count; ++i) {
      MakeSnapshot(0, db, retain);
      auto latest = GetLastFile(snapshot_dir_);
      snapshots.emplace(GetLastFile(snapshot_dir_));
      // Ensures that the latest snapshot was not in the snapshots collection
      // before. Thus ensures that it wasn't pruned.
      EXPECT_EQ(snapshots.size(), i + 1);
    }

    EXPECT_EQ(DirFiles(snapshot_dir_).size(),
              std::min(count, retain < 0 ? count : retain));
  };
}

TEST_F(Durability, WalRetention) {
  FLAGS_wal_rotate_deltas_count = 100;
  {
    auto config = DbConfig();
    config.durability_enabled = true;
    database::SingleNode db{config};
    MakeDb(db, 100);
    MakeSnapshot(0, db);
    MakeDb(db, 100);
    EXPECT_EQ(DirFiles(snapshot_dir_).size(), 1);
    db.wal().Flush();
    // 1 current WAL file, plus retained ones
    EXPECT_GT(DirFiles(wal_dir_).size(), 1);
    MakeSnapshot(0, db);
    db.wal().Flush();
  }

  // only 1 current WAL file
  EXPECT_EQ(DirFiles(snapshot_dir_).size(), 2);
  // There can only be one extra wal file (two total) because that file could
  // have been written after the snapshot WAL cleanup
  EXPECT_LE(DirFiles(wal_dir_).size(), 2);
}

TEST_F(Durability, SnapshotOnExit) {
  {
    auto config = DbConfig();
    config.snapshot_on_exit = true;
    database::SingleNode graph_db{config};
  }
  EXPECT_EQ(DirFiles(snapshot_dir_).size(), 1);
}

TEST_F(Durability, WorkerIdRecovery) {
  auto config = DbConfig();
  config.worker_id = 5;
  config.recovering_cluster_size = 1;
  database::Master db{config};
  MakeDb(db, 100);
  MakeSnapshot(db.WorkerId(), db);
  EXPECT_EQ(DirFiles(snapshot_dir_).size(), 1);

  // WorkerIds are equal and recovery should be sucessful
  {
    auto config = DbConfig();
    config.worker_id = 5;
    config.db_recover_on_startup = true;
    config.recovering_cluster_size = 1;
    database::Master recovered{config};
    EXPECT_EQ(recovered.WorkerId(), config.worker_id);
    CompareDbs(db, recovered);
    auto dba = recovered.Access();
    EXPECT_NE(dba->VerticesCount(), 0);
    EXPECT_NE(dba->EdgesCount(), 0);
  }

  // WorkerIds are not equal and recovery should fail
  {
    auto config = DbConfig();
    config.worker_id = 10;
    config.db_recover_on_startup = true;
    config.recovering_cluster_size = 1;
    database::Master recovered{config};
    EXPECT_NE(recovered.WorkerId(), db.WorkerId());
    auto dba = recovered.Access();
    EXPECT_EQ(dba->VerticesCount(), 0);
    EXPECT_EQ(dba->EdgesCount(), 0);
  }
}

TEST_F(Durability, SequentialRecovery) {
  const int kNumWorkers = 6;
  const int kNumVertices = 1000;

  auto random_int = [](int upper_exclusive) {
    static thread_local std::mt19937 pseudo_rand_gen{std::random_device{}()};
    static thread_local std::uniform_int_distribution<int> rand_dist;
    return rand_dist(pseudo_rand_gen) % upper_exclusive;
  };

  auto init_db = [](database::GraphDb &db) {
    auto dba = db.Access();
    for (int i = 0; i < kNumVertices; ++i) dba->InsertVertex(i);
    dba->Commit();
  };

  auto run_updates = [&random_int](database::GraphDb &db,
                                   std::atomic<bool> &keep_running) {
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumWorkers; ++i) {
      threads.emplace_back([&random_int, &db, &keep_running]() {
        while (keep_running) {
          auto dba = db.Access();
          auto v = dba->FindVertex(random_int(kNumVertices), false);
          try {
            v.PropsSet(dba->Property("prop"), random_int(100));
          } catch (utils::LockTimeoutException &) {
          } catch (mvcc::SerializationError &) {
          }
          dba->InsertVertex();
          dba->Commit();
        }
      });
    }
    return threads;
  };

  auto make_updates = [&run_updates, this](database::SingleNode &db,
                                           bool snapshot_during,
                                           bool snapshot_after) {
    std::atomic<bool> keep_running{true};
    auto update_theads = run_updates(db, keep_running);
    std::this_thread::sleep_for(25ms);
    if (snapshot_during) {
      MakeSnapshot(0, db);
    }
    std::this_thread::sleep_for(25ms);
    keep_running = false;
    for (auto &t : update_theads) t.join();
    if (snapshot_after) {
      MakeSnapshot(0, db);
    }

    db.wal().Flush();
  };

  const std::vector<std::pair<bool, bool>> combinations{{0, 0}, {1, 0}, {0, 1}};
  for (auto &combo : combinations) {
    CleanDurability();
    auto config = DbConfig();
    config.durability_enabled = true;
    database::SingleNode db{config};
    init_db(db);
    make_updates(db, combo.first, combo.second);

    {
      auto recovered_config = DbConfig();
      recovered_config.db_recover_on_startup = true;
      recovered_config.durability_enabled = true;
      database::SingleNode recovered{recovered_config};
      CompareDbs(db, recovered);
      {
        for (auto &combo2 : combinations) {
          make_updates(recovered, combo2.first, combo2.second);
          database::SingleNode recovered_2{recovered_config};
          CompareDbs(recovered, recovered_2);
        }
      }
    }
  }
}
