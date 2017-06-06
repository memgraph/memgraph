#include "durability/recovery.hpp"
#include <cstdio>
#include <experimental/filesystem>
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "dbms/dbms.hpp"
#include "durability/file_reader_buffer.hpp"
#include "gtest/gtest.h"
#include "utils/assert.hpp"

namespace fs = std::experimental::filesystem;

const fs::path SNAPSHOTS_RECOVERY_DEFAULT_DB_DIR = std::tmpnam(nullptr);

std::vector<fs::path> GetFilesFromDir(
    const std::string &snapshots_default_db_dir) {
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshots_default_db_dir))
    files.push_back(file.path());
  return files;
}

void CleanDbDir() {
  if (!fs::exists(SNAPSHOTS_RECOVERY_DEFAULT_DB_DIR)) return;
  std::vector<fs::path> files =
      GetFilesFromDir(SNAPSHOTS_RECOVERY_DEFAULT_DB_DIR);
  for (auto file : files) {
    fs::remove(file);
  }
}

class RecoveryTest : public ::testing::Test {
 protected:
  void TearDown() override {
    CleanDbDir();
    CONFIG(config::SNAPSHOT_CYCLE_SEC) = snapshot_cycle_sec_setup_;
  }

  void SetUp() override {
    CleanDbDir();
    snapshot_cycle_sec_setup_ = CONFIG(config::SNAPSHOT_CYCLE_SEC);
    CONFIG(config::SNAPSHOT_CYCLE_SEC) = "-1";
  }
  std::string snapshot_cycle_sec_setup_;
  const int max_retained_snapshots_ = 10;
};

void CreateSmallGraph(Dbms &dbms) {
  auto dba = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba->insert_vertex();
  auto va2 = dba->insert_vertex();
  dba->insert_edge(va1, va2, dba->edge_type("likes"));
  auto va3 = dba->insert_vertex();
  dba->insert_edge(va3, va2, dba->edge_type("hates"));
  dba->commit();
}

void CreateBigGraph(Dbms &dbms) {
  // creates graph with one inner vertex connected with other 999 outer vertices
  // relationships are directed from outer vertices to the inner vertex
  // every vertex hash label "label" and property "prop" with value "prop"
  // every relationship has type "type" and property "prop" with value "prop"
  auto dba = dbms.active();
  auto va_middle = dba->insert_vertex();
  va_middle.add_label(dba->label("label"));
  va_middle.PropsSet(dba->property("prop"), "prop");
  for (int i = 1; i < 1000; ++i) {
    auto va = dba->insert_vertex();
    va.add_label(dba->label("label"));
    va.PropsSet(dba->property("prop"), "prop");
    auto ea = dba->insert_edge(va, va_middle, dba->edge_type("type"));
    ea.PropsSet(dba->property("prop"), "prop");
  }
  dba->commit();
}

void TakeSnapshot(Dbms &dbms, int max_retained_snapshots_) {
  auto dba = dbms.active();
  Snapshooter snapshooter;
  snapshooter.MakeSnapshot(*dba.get(), SNAPSHOTS_RECOVERY_DEFAULT_DB_DIR,
                           max_retained_snapshots_);
}

std::string GetLatestSnapshot() {
  std::vector<fs::path> files =
      GetFilesFromDir(SNAPSHOTS_RECOVERY_DEFAULT_DB_DIR);
  permanent_assert(static_cast<int>(files.size()) == 1,
                   "No snapshot files in folder.");
  std::sort(files.rbegin(), files.rend());
  return files[0];
}

TEST_F(RecoveryTest, TestEncoding) {
  // Creates snapshot of the small graph. Uses file_reader_buffer and bolt
  // decoder to read data from the snapshot and reads graph from it. After
  // reading graph is tested.
  Dbms dbms;
  CreateSmallGraph(dbms);
  TakeSnapshot(dbms, max_retained_snapshots_);
  std::string snapshot = GetLatestSnapshot();

  FileReaderBuffer buffer;
  communication::bolt::Decoder<FileReaderBuffer> decoder(buffer);

  snapshot::Summary summary;
  buffer.Open(snapshot, summary);

  std::vector<int64_t> ids;
  std::vector<std::string> edge_types;

  for (int i = 0; i < summary.vertex_num_; ++i) {
    communication::bolt::DecodedVertex vertex;
    decoder.ReadVertex(&vertex);
    ids.push_back(vertex.id);
  }
  std::vector<int> from, to;
  for (int i = 0; i < summary.edge_num_; ++i) {
    communication::bolt::DecodedEdge edge;
    decoder.ReadEdge(&edge);
    from.push_back(edge.from);
    to.push_back(edge.to);
    edge_types.push_back(edge.type);
  }
  buffer.Close();

  permanent_assert(static_cast<int>(to.size()) == 2,
                    "There should be two edges.");
  permanent_assert(static_cast<int>(from.size()) == 2,
                    "There should be two edges.");

  EXPECT_EQ(buffer.hash(), summary.hash_);
  EXPECT_NE(edge_types.end(),
            std::find(edge_types.begin(), edge_types.end(), "hates"));
  EXPECT_NE(edge_types.end(),
            std::find(edge_types.begin(), edge_types.end(), "likes"));
  EXPECT_EQ(to[0], to[1]);
  EXPECT_NE(from[0], from[1]);
  EXPECT_NE(ids.end(), std::find(ids.begin(), ids.end(), to[0]));
  EXPECT_NE(ids.end(), std::find(ids.begin(), ids.end(), from[0]));
  EXPECT_NE(ids.end(), std::find(ids.begin(), ids.end(), from[1]));
}

TEST_F(RecoveryTest, TestEncodingAndDecoding) {
  // Creates snapshot of the small graph. Uses Recovery to recover graph from
  // the snapshot file. After creation graph is tested.
  Dbms dbms;
  CreateSmallGraph(dbms);
  TakeSnapshot(dbms, max_retained_snapshots_);
  std::string snapshot = GetLatestSnapshot();

  // New dbms is needed - old dbms has database "default"
  Dbms dbms_recover;
  auto dba_recover = dbms_recover.active();

  Recovery recovery;
  EXPECT_TRUE(recovery.Recover(snapshot, *dba_recover));

  std::vector<VertexAccessor> vertices;
  std::vector<EdgeAccessor> edges;

  auto dba = dbms_recover.active();
  int64_t vertex_count = 0;
  for (const auto &vertex : dba->vertices()) {
    vertices.push_back(vertex);
    vertex_count++;
  }
  EXPECT_EQ(vertex_count, 3);

  int64_t edge_count = 0;
  for (const auto &edge : dba->edges()) {
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.to()));
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.from()));
    edges.push_back(edge);
    edge_count++;
  }
  permanent_assert(static_cast<int>(edges.size()) == 2,
                    "There should be two edges.");

  EXPECT_EQ(edge_count, 2);
  EXPECT_TRUE(edges[0].to() == edges[1].to());
  EXPECT_FALSE(edges[0].from() == edges[1].from());
}

TEST_F(RecoveryTest, TestEncodingAndRecovering) {
  // Creates snapshot of the big graph. Uses Recovery to recover graph from
  // the snapshot file. After creation graph is tested.
  Dbms dbms;
  CreateBigGraph(dbms);
  TakeSnapshot(dbms, max_retained_snapshots_);
  std::string snapshot = GetLatestSnapshot();

  // New dbms is needed - old dbms has database "default"
  Dbms dbms_recover;
  auto dba_recover = dbms_recover.active();

  Recovery recovery;
  EXPECT_TRUE(recovery.Recover(snapshot, *dba_recover));

  auto dba_get = dbms_recover.active();
  int64_t vertex_count = 0;
  for (const auto &vertex : dba_get->vertices()) {
    EXPECT_EQ(vertex.labels().size(), 1);
    EXPECT_TRUE(vertex.has_label(dba_get->label("label")));
    query::TypedValue prop =
        query::TypedValue(vertex.PropsAt(dba_get->property("prop")));
    query::TypedValue expected_prop = query::TypedValue(PropertyValue("prop"));
    EXPECT_TRUE((prop == expected_prop).Value<bool>());
    vertex_count++;
  }
  EXPECT_EQ(vertex_count, 1000);

  int64_t edge_count = 0;
  for (const auto &edge : dba_get->edges()) {
    EXPECT_EQ(edge.edge_type(), dba_get->edge_type("type"));
    query::TypedValue prop =
        query::TypedValue(edge.PropsAt(dba_get->property("prop")));
    query::TypedValue expected_prop = query::TypedValue(PropertyValue("prop"));
    EXPECT_TRUE((prop == expected_prop).Value<bool>());
    edge_count++;
  }
  EXPECT_EQ(edge_count, 999);
  dba_get->commit();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
