#pragma once

#include <cstring>
#include <experimental/filesystem>
#include <vector>

namespace fs = std::experimental::filesystem;

class GraphDbAccessor;

/**
 * Class responsible for making snapshots. Snapshots are stored in folder
 * memgraph/build/$snapshot_folder/$db_name using bolt protocol.
 */
class Snapshooter {
 public:
  Snapshooter(){};
  /**
   * Make snapshot and save it in snapshots folder. Returns true if successful.
   * @param db_accessor:
   *     GraphDbAccessor used to access elements of GraphDb.
   * @param snapshot_folder:
   *     folder where snapshots are stored.
   * @param max_retained_snapshots:
   *     maximum number of snapshots stored in snapshot folder.
   */
  bool MakeSnapshot(GraphDbAccessor &db_accessor,
                    const fs::path &snapshot_folder,
                    int max_retained_snapshots);

 private:
  /**
   * Method returns path to new snapshot file in format
   * memgraph/build/$snapshot_folder/$db_name/$timestamp
   */
  fs::path GetSnapshotFileName(const fs::path &snapshot_folder);
  /**
   * Method used to keep given number of snapshots in snapshot folder. Newest
   * max_retained_files snapshots are kept, other snapshots are deleted. If
   * max_retained_files is -1, all snapshots are kept.
   */
  void MaintainMaxRetainedFiles(const fs::path &snapshot_folder,
                                const int max_retained_files);
  /**
   * Function returns list of snapshot files in snapshot folder.
   */
  std::vector<fs::path> GetSnapshotFiles(const fs::path &snapshot_folder);

  /**
   * Encodes graph and stores it in file given as parameter. Graph elements are
   * accessed using parameter db_accessor. If function is successfully executed,
   * true is returned.
   */
  bool Encode(const fs::path &snapshot_file, GraphDbAccessor &db_accessor);
};
