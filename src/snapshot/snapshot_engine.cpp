#include "snapshot/snapshot_engine.hpp"

#include "config/config.hpp"
#include "database/db_accessor.hpp"
#include "logging/default.hpp"
#include "snapshot/snapshot_decoder.hpp"
#include "snapshot/snapshot_encoder.hpp"
#include "storage/indexes/indexes.hpp"
#include "threading/thread.hpp"
#include "utils/sys.hpp"

SnapshotEngine::SnapshotEngine(Db &db)
    : snapshot_folder(CONFIG(config::SNAPSHOTS_PATH)),
      db(db),
      max_retained_snapshots(CONFIG_INTEGER(config::MAX_RETAINED_SNAPSHOTS)),
      logger(logging::log->logger("SnapshotEngine db[" + db.name() + "]")) {}

bool SnapshotEngine::make_snapshot() {
  std::lock_guard<std::mutex> lock(guard);
  std::time_t now = std::time(nullptr);
  if (make_snapshot(now, "full")) {
    // Sanpsthot was created so whe should check if some older snapshots
    // should be deleted.
    clean_snapshots();
    return true;

  } else {
    return false;
  }
}

void SnapshotEngine::clean_snapshots() {
  logger.info("Started cleaning commit_file");
  // Whe first count the number of snapshots that whe know about in commit
  // file.
  std::vector<std::string> lines;
  {
    std::ifstream commit_file(snapshot_commit_file());

    std::string line;
    while (std::getline(commit_file, line)) {
      lines.push_back(line);
    }
  }

  int n = lines.size() - max_retained_snapshots;
  if (n > 0) {
    // Whe have to much snapshots so whe should delete some.
    std::ofstream commit_file(snapshot_commit_file(), std::fstream::trunc);

    // First whw will rewrite commit file to contain only
    // max_retained_snapshots newest snapshots.
    for (auto i = n; i < lines.size(); i++) {
      commit_file << lines[i] << std::endl;
    }

    auto res = sys::flush_file_to_disk(commit_file);
    if (res == 0) {
      // Commit file was succesfully changed so whe can now delete
      // snapshots which whe evicted from commit file.
      commit_file.close();
      logger.info("Removed {} snapshot from commit_file", n);

      for (auto i = 0; i < n; i++) {
        auto res = std::remove(lines[i].c_str());
        if (res == 0) {
          logger.info("Succesfully deleted snapshot file \"{}\"", lines[i]);
        } else {
          logger.error("Error {} occured while deleting snapshot file \"{}\"",
                       res, lines[i]);
        }
      }

    } else {
      logger.error("Error {} occured while flushing commit file", res);
    }
  }

  logger.info("Finished cleaning commit_file");
}

bool SnapshotEngine::make_snapshot(std::time_t now, const char *type) {
  bool success = false;

  auto snapshot_file_name = snapshot_file(now, type);

  logger.info("Writing {} snapshot to file \"{}\"", type, snapshot_file_name);

  DbTransaction t(db);

  try {
    std::ofstream snapshot_file(snapshot_file_name,
                                std::fstream::binary | std::fstream::trunc);

    SnapshotEncoder snap(snapshot_file);

    auto old_trans =
        tx::TransactionRead(db.tx_engine);  // Overenginered for incremental
                                            // snapshot. Can be removed.

    // Everything is ready for creation of snapshot.
    snapshot(t, snap, old_trans);

    auto res = sys::flush_file_to_disk(snapshot_file);
    if (res == 0) {
      // Snapshot was succesfully written to disk.
      t.trans.commit();
      success = true;

    } else {
      logger.error("Error {} occured while flushing snapshot file", res);
      t.trans.abort();
    }

  } catch (const std::exception &e) {
    logger.error("Exception occured while creating {} snapshot", type);
    logger.error("{}", e.what());

    t.trans.abort();
  }

  if (success) {
    // Snapshot was succesfully created but for it to be reachable for
    // import whe must add it to the end of commit file.
    std::ofstream commit_file(snapshot_commit_file(), std::fstream::app);

    commit_file << snapshot_file_name << std::endl;

    auto res = sys::flush_file_to_disk(commit_file);
    if (res == 0) {
      commit_file.close();
      snapshoted_no_v.fetch_add(1);
      // Snapshot was succesfully commited.

    } else {
      logger.error("Error {} occured while flushing commit file", res);
    }
  }

  return success;
}

bool SnapshotEngine::import() {
  std::lock_guard<std::mutex> lock(guard);

  logger.info("Started import");
  bool success = false;

  try {
    std::ifstream commit_file(snapshot_commit_file());

    // Whe first load all known snpashot file names from commit file.
    std::vector<std::string> snapshots;
    std::string line;
    while (std::getline(commit_file, line)) {
      snapshots.push_back(line);
    }

    while (snapshots.size() > 0) {
      logger.info("Importing data from snapshot \"{}\"", snapshots.back());

      DbAccessor t(db);

      try {
        std::ifstream snapshot_file(snapshots.back(), std::fstream::binary);
        SnapshotDecoder decoder(snapshot_file);

        auto indexes = snapshot_load(t, decoder);
        if (t.commit()) {
          logger.info("Succesfully imported snapshot \"{}\"", snapshots.back());
          add_indexes(indexes);
          success = true;
          break;

        } else {
          logger.info(
              "Unuccesfully tryed to import snapshot "
              "\"{}\"",
              snapshots.back());
        }

      } catch (const std::exception &e) {
        logger.error("Error occured while importing snapshot \"{}\"",
                     snapshots.back());
        logger.error("{}", e.what());
        t.abort();
      }

      snapshots.pop_back();
      // Whe will try to import older snapashot if such one exist.
    }

  } catch (const std::exception &e) {
    logger.error("Error occured while importing snapshot");
    logger.error("{}", e.what());
  }

  logger.info("Finished import");

  return success;
}

void SnapshotEngine::snapshot(DbTransaction const &dt, SnapshotEncoder &snap,
                              tx::TransactionRead const &old_trans) {
  Db &db = dt.db;
  DbAccessor t(db, dt.trans);

  // Anounce property names
  for (auto &family : db.graph.vertices.property_family_access()) {
    snap.property_name_init(family.first);
  }
  for (auto &family : db.graph.edges.property_family_access()) {
    snap.property_name_init(family.first);
  }

  // Anounce label names
  for (auto &labels : db.graph.label_store.access()) {
    snap.label_name_init(labels.first.to_string());
  }

  // Anounce edge_type names
  for (auto &et : db.graph.edge_type_store.access()) {
    snap.edge_type_name_init(et.first.to_string());
  }

  // Store vertices
  snap.start_vertices();
  t.vertex_access()
      .fill()
      .filter([&](auto va) { return !va.is_visble_to(old_trans); })
      .for_all([&](auto va) { serialization::serialize_vertex(va, snap); });

  // Store edges
  snap.start_edges();
  t.edge_access()
      .fill()
      .filter([&](auto va) { return !va.is_visble_to(old_trans); })
      .for_all([&](auto ea) { serialization::serialize_edge(ea, snap); });

  // Store info on existing indexes.
  snap.start_indexes();
  db.indexes().vertex_indexes([&](auto &i) { snap.index(i.definition()); });
  db.indexes().edge_indexes([&](auto &i) { snap.index(i.definition()); });

  snap.end();
}

std::vector<IndexDefinition> SnapshotEngine::snapshot_load(
    DbAccessor &t, SnapshotDecoder &snap) {
  std::unordered_map<uint64_t, VertexAccessor> vertices;

  // Load names
  snap.load_init();

  // Load vertices
  snap.begin_vertices();
  size_t v_count = 0;
  while (!snap.end_vertices()) {
    vertices.insert(serialization::deserialize_vertex(t, snap));
    v_count++;
  }
  logger.info("Loaded {} vertices", v_count);

  // Load edges
  snap.begin_edges();
  size_t e_count = 0;
  while (!snap.end_edges()) {
    serialization::deserialize_edge(t, snap, vertices);
    e_count++;
  }
  logger.info("Loaded {} edges", e_count);

  // Load indexes
  snap.start_indexes();
  std::vector<IndexDefinition> indexes;
  while (!snap.end()) {
    indexes.push_back(snap.load_index());
  }

  return indexes;
}

void SnapshotEngine::add_indexes(std::vector<IndexDefinition> &v) {
  logger.info("Adding: {} indexes", v.size());
  for (auto id : v) {
    // TODO: It is alright for now to ignore if add_index return false. I am
    // not even sure if false should stop snapshot loading.
    if (!db.indexes().add_index(id)) {
      logger.warn(
          "Failed to add index, but still continuing with "
          "loading snapshot");
    }
  }
}

std::string SnapshotEngine::snapshot_file(std::time_t const &now,
                                          const char *type) {
  // Current nano time less than second.
  auto now_nano = std::chrono::time_point_cast<std::chrono::nanoseconds>(
                      std::chrono::high_resolution_clock::now())
                      .time_since_epoch()
                      .count() %
                  (1000 * 1000 * 1000);

  return snapshot_db_dir() + "/" + std::to_string(now) + "_" +
         std::to_string(now_nano) + "_" + type;
}

std::string SnapshotEngine::snapshot_commit_file() {
  return snapshot_db_dir() + "/snapshot_commit.txt";
}

std::string SnapshotEngine::snapshot_db_dir() {
  if (!sys::ensure_directory_exists(snapshot_folder)) {
    logger.error("Error while creating directory \"{}\"", snapshot_folder);
  }

  auto db_path = snapshot_folder + "/" + db.name();
  if (!sys::ensure_directory_exists(db_path)) {
    logger.error("Error while creating directory \"{}\"", db_path);
  }

  return db_path;
}
